# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de troca de mensagens entre usuários, atendendo somente aos seguintes requisitos:
#
# 1. Permitir a execução de operações simultâneas.
# 2. Validar os dados recebidos antes de realizar operações.
# 3. Fazer uso de macros.
# 4. Permitir limitar a quantidade de operações simultâneas.
# 5. Permitir cadastrar, consultar, atualizar e remover registros.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - Agent
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# Sistema de troca de mensagens entre usuários com Agent, operações concorrentes,
# limite de concorrência, validação de dados e macros.

defmodule MessageSystem.Validator do
  # Validação pura de dados. Retorna {:ok, mapa_limpo} ou {:error, [{campo, motivo}]}.
  # Modos: :create (campos obrigatórios devem existir) e :update (apenas campos enviados são validados).

  @type mode :: :create | :update

  def validate(attrs, schema, mode \\ :create)

  def validate(attrs, schema, mode)
      when is_map(attrs) and is_list(schema) and mode in [:create, :update] do
    fields = Keyword.keys(schema)

    # Campos desconhecidos são rejeitados
    unknown = for key <- Map.keys(attrs), key not in fields, do: {key, :unknown_field}

    field_errors = Enum.flat_map(schema, &field_errors(attrs, &1, mode))

    case unknown ++ field_errors do
      [] -> {:ok, Map.take(attrs, fields)}
      errors -> {:error, errors}
    end
  end

  def validate(_attrs, _schema, _mode), do: {:error, [{:attrs, :must_be_map}]}

  defp field_errors(attrs, {field, rules}, mode) do
    case Map.fetch(attrs, field) do
      {:ok, value} -> check(field, value, rules)
      :error when mode == :create -> check(field, nil, rules)
      :error -> []
    end
  end

  defp check(field, nil, rules) do
    if :required in rules, do: [{field, :required}], else: []
  end

  defp check(field, value, rules) do
    for rule <- rules, rule != :required, not valid?(value, rule), do: {field, rule_name(rule)}
  end

  defp valid?(v, :string), do: is_binary(v)
  defp valid?(v, :non_empty), do: is_binary(v) and String.trim(v) != ""
  defp valid?(v, :positive_integer), do: is_integer(v) and v > 0
  defp valid?(v, :email), do: is_binary(v) and email?(v)
  defp valid?(v, {:max_len, n}), do: is_binary(v) and String.length(v) <= n
  defp valid?(v, {:min_len, n}), do: is_binary(v) and String.length(v) >= n
  defp valid?(_v, _rule), do: false

  defp email?(v) do
    case String.split(v, "@") do
      [local, domain] ->
        local != "" and String.contains?(domain, ".") and
          not String.contains?(v, [" ", "\t", "\n"])

      _ ->
        false
    end
  end

  defp rule_name({name, _arg}), do: name
  defp rule_name(name), do: name
end

defmodule MessageSystem.Limiter do
  # Semáforo baseado em Agent que limita o número de operações simultâneas.
  # Quando o limite é atingido, a chamada aguarda até um timeout.

  use Agent

  @default_timeout 5_000

  def start_link(max) when is_integer(max) and max > 0 do
    Agent.start_link(fn -> %{max: max, active: 0} end, name: __MODULE__)
  end

  def acquire(timeout \\ @default_timeout) when is_integer(timeout) and timeout >= 0 do
    do_acquire(now_ms() + timeout)
  end

  def release do
    Agent.update(__MODULE__, fn state -> %{state | active: max(state.active - 1, 0)} end)
  end

  def status, do: Agent.get(__MODULE__, & &1)

  defp do_acquire(deadline) do
    case Agent.get_and_update(__MODULE__, &try_take/1) do
      :ok ->
        :ok

      :busy ->
        if now_ms() >= deadline do
          {:error, :limit_timeout}
        else
          Process.sleep(1)
          do_acquire(deadline)
        end
    end
  end

  defp try_take(%{active: active, max: max} = state) when active < max,
    do: {:ok, %{state | active: active + 1}}

  defp try_take(state), do: {:busy, state}

  defp now_ms, do: System.monotonic_time(:millisecond)
end

defmodule MessageSystem.Macros do
  # Macros utilitárias do sistema.

  # Executa o bloco somente após adquirir uma permissão do Limiter,
  # garantindo a liberação da permissão mesmo em caso de exceção.
  defmacro with_limit(do: block) do
    quote do
      case MessageSystem.Limiter.acquire() do
        :ok ->
          try do
            unquote(block)
          after
            MessageSystem.Limiter.release()
          end

        {:error, _reason} = error ->
          error
      end
    end
  end
end

defmodule MessageSystem.Entity do
  # Macro `use` que gera um repositório CRUD baseado em Agent para cada entidade.
  # Cada operação valida os dados antes de acessar o Agent e respeita o limite de concorrência.

  defmacro __using__(opts) do
    schema = Keyword.fetch!(opts, :schema)

    quote do
      use Agent
      import MessageSystem.Macros

      @schema unquote(schema)

      def start_link(_opts \\ []) do
        Agent.start_link(fn -> %{next_id: 1, records: %{}} end, name: __MODULE__)
      end

      def create(attrs) do
        with {:ok, clean} <- MessageSystem.Validator.validate(attrs, @schema, :create) do
          with_limit do
            Agent.get_and_update(__MODULE__, fn %{next_id: id, records: records} = state ->
              record = Map.put(clean, :id, id)
              {{:ok, record}, %{state | next_id: id + 1, records: Map.put(records, id, record)}}
            end)
          end
        end
      end

      def get(id) do
        with :ok <- check_id(id) do
          with_limit do
            Agent.get(__MODULE__, fn %{records: records} ->
              case Map.fetch(records, id) do
                {:ok, record} -> {:ok, record}
                :error -> {:error, :not_found}
              end
            end)
          end
        end
      end

      def update(id, attrs) do
        with :ok <- check_id(id),
             {:ok, clean} <- MessageSystem.Validator.validate(attrs, @schema, :update) do
          with_limit do
            Agent.get_and_update(__MODULE__, fn %{records: records} = state ->
              case Map.fetch(records, id) do
                {:ok, record} ->
                  updated = Map.merge(record, clean)
                  {{:ok, updated}, %{state | records: Map.put(records, id, updated)}}

                :error ->
                  {{:error, :not_found}, state}
              end
            end)
          end
        end
      end

      def delete(id) do
        with :ok <- check_id(id) do
          with_limit do
            Agent.get_and_update(__MODULE__, fn %{records: records} = state ->
              case Map.pop(records, id) do
                {nil, _rest} -> {{:error, :not_found}, state}
                {record, rest} -> {{:ok, record}, %{state | records: rest}}
              end
            end)
          end
        end
      end

      def list do
        with_limit do
          Agent.get(__MODULE__, fn %{records: records} ->
            {:ok, records |> Map.values() |> Enum.sort_by(& &1.id)}
          end)
        end
      end

      defp check_id(id) when is_integer(id) and id > 0, do: :ok
      defp check_id(_id), do: {:error, [{:id, :positive_integer}]}
    end
  end
end

defmodule MessageSystem.Users do
  # Repositório de usuários.
  use MessageSystem.Entity,
    schema: [
      name: [:required, :non_empty, {:max_len, 50}],
      email: [:required, :email]
    ]
end

defmodule MessageSystem.Messages do
  # Repositório de mensagens.
  use MessageSystem.Entity,
    schema: [
      from_id: [:required, :positive_integer],
      to_id: [:required, :positive_integer],
      body: [:required, :non_empty, {:max_len, 500}]
    ]
end

defmodule MessageSystem.Chat do
  # Regras de negócio para troca de mensagens entre usuários.

  alias MessageSystem.{Users, Messages}

  # Envia uma mensagem, verificando se remetente e destinatário existem.
  def send_message(from_id, to_id, body) do
    with {:ok, _sender} <- Users.get(from_id),
         {:ok, _recipient} <- Users.get(to_id) do
      Messages.create(%{from_id: from_id, to_id: to_id, body: body})
    end
  end

  # Envia a mesma mensagem para vários destinatários de forma concorrente.
  # O limite global é imposto pelo Limiter; max_concurrency limita as tarefas deste lote.
  def broadcast(from_id, to_ids, body, opts \\ []) when is_list(to_ids) do
    max = Keyword.get(opts, :max_concurrency, System.schedulers_online())

    to_ids
    |> Task.async_stream(&send_message(from_id, &1, body),
      max_concurrency: max,
      timeout: :infinity,
      ordered: true
    )
    |> Enum.map(fn {:ok, result} -> result end)
  end

  # Retorna as mensagens recebidas por um usuário.
  def inbox(user_id) do
    with {:ok, _user} <- Users.get(user_id),
         {:ok, messages} <- Messages.list() do
      {:ok, Enum.filter(messages, &(&1.to_id == user_id))}
    end
  end
end

defmodule MessageSystem do
  # Ponto de entrada do sistema: inicia o Limiter e os repositórios sob um Supervisor.

  def start_link(opts \\ []) do
    max_concurrency = Keyword.get(opts, :max_concurrency, 10)

    children = [
      {MessageSystem.Limiter, max_concurrency},
      MessageSystem.Users,
      MessageSystem.Messages
    ]

    Supervisor.start_link(children, strategy: :one_for_one, name: __MODULE__)
  end
end
