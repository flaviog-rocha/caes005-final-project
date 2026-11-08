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

# Sistema de troca de mensagens entre usuários
# Requisitos atendidos:
#   1. Operações simultâneas (cada chamada roda no processo do chamador;
#      o estado é protegido por Agent, com atomicidade garantida pelo get_and_update).
#   2. Validação dos dados antes de qualquer operação (regras declarativas via macro `rule`).
#   3. Uso de macros (`with_slot/2` e `rule/2`).
#   4. Limite de operações simultâneas (semáforo implementado com Agent).
#   5. CRUD de usuários e mensagens (create, read, update, delete/list).
# Executar com: elixir sistema_mensagens.exs

defmodule Macros do
  @moduledoc """
  Macros utilitárias para controle de concorrência e validação declarativa.
  """

  @doc """
  Executa o bloco somente após obter um slot do `Limiter`.
  O slot é sempre liberado ao final, mesmo em caso de exceção.
  Retorna `{:error, reason}` se o limite não puder ser respeitado a tempo.
  """
  defmacro with_slot(limiter, do: block) do
    quote do
      limiter_pid = unquote(limiter)

      case Limiter.acquire(limiter_pid) do
        :ok ->
          try do
            unquote(block)
          after
            Limiter.release(limiter_pid)
          end

        {:error, _reason} = error ->
          error
      end
    end
  end

  @doc """
  Retorna `nil` quando a condição é verdadeira, ou a mensagem de erro caso contrário.
  Usada para compor listas de regras de validação.
  """
  defmacro rule(condition, message) do
    quote do
      unless unquote(condition), do: unquote(message)
    end
  end
end

defmodule Limiter do
  @moduledoc """
  Semáforo baseado em Agent que limita o número de operações simultâneas.
  """

  @doc "Inicia o limitador com no máximo `max` operações simultâneas."
  def start_link(max) when is_integer(max) and max > 0 do
    Agent.start_link(fn -> %{max: max, active: 0} end)
  end

  @doc """
  Tenta obter um slot, aguardando até `timeout` milissegundos.
  Retorna `:ok` ou `{:error, :limit_timeout}`.
  """
  def acquire(pid, timeout \\ 5_000) do
    deadline = System.monotonic_time(:millisecond) + timeout
    do_acquire(pid, deadline)
  end

  @doc "Libera um slot previamente obtido."
  def release(pid) do
    Agent.update(pid, fn s -> %{s | active: max(s.active - 1, 0)} end)
  end

  @doc "Retorna o estado atual do limitador."
  def stats(pid), do: Agent.get(pid, &Map.take(&1, [:max, :active]))

  defp do_acquire(pid, deadline) do
    case Agent.get_and_update(pid, &take_slot/1) do
      :ok ->
        :ok

      :full ->
        if System.monotonic_time(:millisecond) >= deadline do
          {:error, :limit_timeout}
        else
          Process.sleep(1)
          do_acquire(pid, deadline)
        end
    end
  end

  defp take_slot(%{max: max, active: active} = s) when active < max,
    do: {:ok, %{s | active: active + 1}}

  defp take_slot(s), do: {:full, s}
end

defmodule Store do
  @moduledoc """
  Armazenamento em memória baseado em Agent.
  Funções puras operam sobre o estado e retornam `{resposta, novo_estado}`,
  sendo executadas atomicamente via `transact/2`.
  """

  @doc "Inicia o armazenamento com coleções vazias."
  def start_link do
    Agent.start_link(fn -> %{next_id: 1, users: %{}, messages: %{}} end)
  end

  @doc "Executa uma função pura de forma atômica dentro do Agent."
  def transact(pid, fun), do: Agent.get_and_update(pid, fun)

  @doc "Insere um registro gerando um novo id."
  def insert(st, coll, attrs) do
    id = st.next_id
    rec = Map.put(attrs, :id, id)

    {{:ok, rec}, st |> Map.put(:next_id, id + 1) |> Map.update!(coll, &Map.put(&1, id, rec))}
  end

  @doc "Consulta um registro pelo id."
  def fetch(st, coll, id) do
    case Map.fetch(Map.fetch!(st, coll), id) do
      {:ok, rec} -> {{:ok, rec}, st}
      :error -> {{:error, :not_found}, st}
    end
  end

  @doc "Atualiza um registro, validando o resultado final antes de persistir."
  def update(st, coll, id, attrs, validator) do
    case Map.fetch(Map.fetch!(st, coll), id) do
      :error ->
        {{:error, :not_found}, st}

      {:ok, rec} ->
        merged = Map.merge(rec, attrs)

        case validator.(merged) do
          [] -> {{:ok, merged}, Map.update!(st, coll, &Map.put(&1, id, merged))}
          errors -> {{:error, {:validation, errors}}, st}
        end
    end
  end

  @doc "Remove um registro pelo id."
  def delete(st, coll, id) do
    if Map.has_key?(Map.fetch!(st, coll), id) do
      {{:ok, id}, Map.update!(st, coll, &Map.delete(&1, id))}
    else
      {{:error, :not_found}, st}
    end
  end

  @doc "Lista registros, opcionalmente filtrados, ordenados por id."
  def list(st, coll, filter \\ fn _ -> true end) do
    records =
      st
      |> Map.fetch!(coll)
      |> Map.values()
      |> Enum.filter(filter)
      |> Enum.sort_by(& &1.id)

    {{:ok, records}, st}
  end
end

defmodule MessageSystem do
  @moduledoc """
  API pública do sistema de troca de mensagens.
  Todas as operações são validadas antes de execução e limitadas em concorrência.
  """

  import Macros

  @user_fields [:name, :email]
  @message_fields [:body]

  @doc """
  Inicializa o sistema com `max_concurrency` operações simultâneas.
  Retorna um mapa com os pids do armazenamento e do limitador.
  """
  def start(max_concurrency \\ 10) do
    {:ok, store} = Store.start_link()
    {:ok, limiter} = Limiter.start_link(max_concurrency)
    %{store: store, limiter: limiter}
  end

  # ---------------------------------------------------------------------------
  # Usuários
  # ---------------------------------------------------------------------------

  @doc "Cadastra um usuário."
  def create_user(sys, attrs) do
    perform(
      sys,
      field_errors(attrs, @user_fields) ++ validate_user(attrs),
      fn -> Store.transact(sys.store, fn st -> Store.insert(st, :users, attrs) end) end
    )
  end

  @doc "Consulta um usuário pelo id."
  def get_user(sys, id) do
    perform(sys, validate_id(id), fn ->
      Store.transact(sys.store, fn st -> Store.fetch(st, :users, id) end)
    end)
  end

  @doc "Lista todos os usuários."
  def list_users(sys) do
    perform(sys, [], fn ->
      Store.transact(sys.store, fn st -> Store.list(st, :users) end)
    end)
  end

  @doc "Atualiza campos de um usuário."
  def update_user(sys, id, attrs) do
    perform(sys, validate_id(id) ++ field_errors(attrs, @user_fields), fn ->
      Store.transact(sys.store, fn st ->
        Store.update(st, :users, id, attrs, fn merged -> validate_user(merged) end)
      end)
    end)
  end

  @doc "Remove um usuário pelo id."
  def delete_user(sys, id) do
    perform(sys, validate_id(id), fn ->
      Store.transact(sys.store, fn st -> Store.delete(st, :users, id) end)
    end)
  end

  # ---------------------------------------------------------------------------
  # Mensagens
  # ---------------------------------------------------------------------------

  @doc "Envia uma mensagem de um usuário para outro, verificando a existência de ambos."
  def send_message(sys, from_id, to_id, body) do
    errors = validate_id(from_id) ++ validate_id(to_id) ++ validate_body(body)

    perform(sys, errors, fn ->
      Store.transact(sys.store, fn st ->
        cond do
          not Map.has_key?(st.users, from_id) ->
            {{:error, {:not_found, :sender}}, st}

          not Map.has_key?(st.users, to_id) ->
            {{:error, {:not_found, :recipient}}, st}

          true ->
            Store.insert(st, :messages, %{
              from: from_id,
              to: to_id,
              body: body,
              sent_at: DateTime.utc_now()
            })
        end
      end)
    end)
  end

  @doc "Consulta uma mensagem pelo id."
  def get_message(sys, id) do
    perform(sys, validate_id(id), fn ->
      Store.transact(sys.store, fn st -> Store.fetch(st, :messages, id) end)
    end)
  end

  @doc "Lista todas as mensagens."
  def list_messages(sys) do
    perform(sys, [], fn ->
      Store.transact(sys.store, fn st -> Store.list(st, :messages) end)
    end)
  end

  @doc "Lista as mensagens recebidas por um usuário."
  def inbox(sys, user_id) do
    perform(sys, validate_id(user_id), fn ->
      Store.transact(sys.store, fn st ->
        Store.list(st, :messages, fn msg -> msg.to == user_id end)
      end)
    end)
  end

  @doc "Atualiza o conteúdo de uma mensagem."
  def update_message(sys, id, attrs) do
    perform(sys, validate_id(id) ++ field_errors(attrs, @message_fields), fn ->
      Store.transact(sys.store, fn st ->
        Store.update(st, :messages, id, attrs, fn merged -> validate_message(merged) end)
      end)
    end)
  end

  @doc "Remove uma mensagem pelo id."
  def delete_message(sys, id) do
    perform(sys, validate_id(id), fn ->
      Store.transact(sys.store, fn st -> Store.delete(st, :messages, id) end)
    end)
  end

  # ---------------------------------------------------------------------------
  # Execução controlada
  # ---------------------------------------------------------------------------

  # Só executa a operação se não houver erros de validação e
  # se houver um slot disponível no limitador.
  defp perform(sys, errors, fun) do
    with_slot sys.limiter do
      if errors == [], do: fun.(), else: {:error, {:validation, errors}}
    end
  end

  # ---------------------------------------------------------------------------
  # Validações
  # ---------------------------------------------------------------------------

  defp errors(list), do: Enum.reject(list, &is_nil/1)

  defp field_errors(attrs, allowed) when is_map(attrs) do
    unknown = Map.keys(attrs) -- allowed
    errors([rule(unknown == [], "campos desconhecidos: #{inspect(unknown)}")])
  end

  defp field_errors(_attrs, _allowed), do: ["attrs deve ser um mapa"]

  defp validate_id(id) do
    errors([rule(is_integer(id) and id > 0, "id deve ser inteiro positivo")])
  end

  defp validate_user(attrs) when is_map(attrs) do
    name = Map.get(attrs, :name)
    email = Map.get(attrs, :email)

    errors([
      rule(is_binary(name) and String.trim(name) != "", "name é obrigatório"),
      rule(is_binary(name) and String.length(name) <= 50, "name deve ter até 50 caracteres"),
      rule(
        is_binary(email) and Regex.match?(~r/^[^@\s]+@[^@\s]+\.[^@\s]+$/, email),
        "email inválido"
      )
    ])
  end

  defp validate_user(_attrs), do: []

  defp validate_message(attrs) when is_map(attrs), do: validate_body(Map.get(attrs, :body))
  defp validate_message(_attrs), do: []

  defp validate_body(body) do
    errors([
      rule(is_binary(body) and String.trim(body) != "", "body é obrigatório"),
      rule(is_binary(body) and String.length(body) <= 500, "body deve ter até 500 caracteres")
    ])
  end
end

# ---------------------------------------------------------------------------
# Demonstração de uso (execução concorrente com limite de 3 operações simultâneas)
# ---------------------------------------------------------------------------

sys = MessageSystem.start(3)

{:ok, ana} = MessageSystem.create_user(sys, %{name: "Ana", email: "ana@exemplo.com"})
{:ok, bruno} = MessageSystem.create_user(sys, %{name: "Bruno", email: "bruno@exemplo.com"})

1..20
|> Task.async_stream(
  fn i -> MessageSystem.send_message(sys, ana.id, bruno.id, "Mensagem #{i}") end,
  max_concurrency: 50,
  timeout: 10_000
)
|> Enum.to_list()

{:ok, recebidas} = MessageSystem.inbox(sys, bruno.id)
IO.puts("Mensagens recebidas por Bruno: #{length(recebidas)}")

IO.inspect(MessageSystem.create_user(sys, %{name: "", email: "invalido"}), label: "Validação")
IO.inspect(MessageSystem.send_message(sys, ana.id, 999, "oi"), label: "Destinatário inexistente")
IO.inspect(Limiter.stats(sys.limiter), label: "Limitador")
