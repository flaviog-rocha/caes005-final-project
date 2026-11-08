# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de troca de mensagens entre usuários, atendendo somente aos seguintes requisitos:
#
# 1. Impedir atualizações parcialmente concluídas.
# 2. Permitir consultar o estado atual das operações em andamento.
# 3. Fazer uso de macros.
# 4. Impedir que uma mesma operação seja executada mais de uma vez.
# 5. Permitir cadastrar, consultar, atualizar e remover registros.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - Supervisor
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# Arquivo único: sistema de troca de mensagens entre usuários.
#
# Execução sugerida:
#   {:ok, _sup} = MessageSystem.Supervisor.start_link([])
#   {:ok, ana} = MessageSystem.create_user(MessageSystem.new_op_id(), %{name: "Ana"}) |> elem(0) |> then(fn _ -> {:ok, nil} end)

defmodule MessageSystem.Repo do
  @moduledoc false
  # Funções puras sobre o snapshot de dados. Cada operação de escrita retorna
  # {:ok, novo_estado, resultado} ou {:error, motivo}; nada é aplicado aqui.

  def empty, do: %{seq: 0, users: %{}, messages: %{}}

  def fetch(data, resource, id) do
    case data |> Map.fetch!(resource) |> Map.fetch(id) do
      {:ok, record} -> {:ok, record}
      :error -> {:error, :not_found}
    end
  end

  def insert(data, resource, attrs) when is_map(attrs) do
    id = data.seq + 1
    record = Map.put(attrs, :id, id)

    new_data =
      data
      |> Map.put(:seq, id)
      |> Map.update!(resource, &Map.put(&1, id, record))

    {:ok, new_data, record}
  end

  def update(data, resource, id, changes) when is_map(changes) do
    with {:ok, record} <- fetch(data, resource, id) do
      updated = Map.merge(record, Map.delete(changes, :id))
      {:ok, Map.update!(data, resource, &Map.put(&1, id, updated)), updated}
    end
  end

  def delete(data, resource, id) do
    with {:ok, record} <- fetch(data, resource, id) do
      {:ok, Map.update!(data, resource, &Map.delete(&1, id)), record}
    end
  end
end

defmodule MessageSystem.Store do
  @moduledoc false
  # GenServer que mantém o estado versionado e o registro de operações.
  #
  # - Atomicidade: uma operação só é confirmada se o worker produzir {:ok, novo, _}.
  #   O novo estado substitui o antigo de uma só vez; caso contrário nada muda.
  # - Controle de concorrência otimista: se outra operação confirmou no meio
  #   do caminho, o worker recebe {:retry, ...} e recalcula sobre o snapshot atual.
  # - Idempotência: cada op_id é executado no máximo uma vez.
  # - Visibilidade: operações rodam em Task supervisionada; enquanto isso o
  #   Store continua respondendo consultas de status.
  use GenServer

  @task_sup MessageSystem.TaskSupervisor

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, :ok, Keyword.put_new(opts, :name, __MODULE__))
  end

  # ---------------------------------------------------------------------------
  # API pública
  # ---------------------------------------------------------------------------

  @doc "Executa a função `fun` (pura, recebe o snapshot) como operação `op_id`."
  def run(op_id, fun) when is_function(fun, 1) do
    GenServer.call(__MODULE__, {:run, op_id, fun}, :infinity)
  end

  @doc "Executa leitura pura sobre o snapshot atual."
  def query(fun) when is_function(fun, 1) do
    GenServer.call(__MODULE__, {:query, fun})
  end

  @doc "Retorna :unknown | :running | {:done, resultado} | {:failed, motivo}."
  def status(op_id), do: GenServer.call(__MODULE__, {:status, op_id})

  @doc "Lista todas as operações e seus estados."
  def operations, do: GenServer.call(__MODULE__, :operations)

  # ---------------------------------------------------------------------------
  # Callbacks
  # ---------------------------------------------------------------------------

  @impl true
  def init(:ok) do
    {:ok, %{version: 0, data: MessageSystem.Repo.empty(), ops: %{}}}
  end

  @impl true
  def handle_call({:run, op_id, fun}, from, state) do
    case Map.fetch(state.ops, op_id) do
      :error ->
        # Registra como em andamento e dispara o worker com o snapshot atual.
        state = %{state | ops: Map.put(state.ops, op_id, {:running, from})}
        start_worker(op_id, fun, state.data, state.version)
        {:noreply, state}

      {:ok, {:running, _}} ->
        {:reply, {:error, :in_progress}, state}

      {:ok, {_finished, _}} ->
        {:reply, {:error, :already_executed}, state}
    end
  end

  def handle_call({:finish, op_id, version, outcome}, _from, state) do
    {:running, from} = Map.fetch!(state.ops, op_id)

    if version != state.version do
      # Snapshot obsoleto: nada é aplicado, o worker deve recalcular.
      {:reply, {:retry, state.data, state.version}, state}
    else
      case outcome do
        {:ok, new_data, result} ->
          GenServer.reply(from, {:ok, result})

          new_state = %{
            state
            | version: state.version + 1,
              data: new_data,
              ops: Map.put(state.ops, op_id, {:done, result})
          }

          {:reply, :ok, new_state}

        {:error, reason} ->
          GenServer.reply(from, {:error, reason})
          {:reply, :ok, %{state | ops: Map.put(state.ops, op_id, {:failed, reason})}}
      end
    end
  end

  def handle_call({:query, fun}, _from, state) do
    {:reply, fun.(state.data), state}
  end

  def handle_call({:status, op_id}, _from, state) do
    reply =
      case Map.get(state.ops, op_id) do
        nil -> :unknown
        {:running, _} -> :running
        {:done, result} -> {:done, result}
        {:failed, reason} -> {:failed, reason}
      end

    {:reply, reply, state}
  end

  def handle_call(:operations, _from, state) do
    list =
      Enum.map(state.ops, fn
        {id, {:running, _}} -> {id, :running}
        {id, status} -> {id, status}
      end)

    {:reply, list, state}
  end

  # ---------------------------------------------------------------------------
  # Worker (executa fora do processo do Store)
  # ---------------------------------------------------------------------------

  defp start_worker(op_id, fun, data, version) do
    {:ok, _pid} =
      Task.Supervisor.start_child(@task_sup, fn ->
        worker_loop(op_id, fun, data, version)
      end)
  end

  defp worker_loop(op_id, fun, data, version) do
    outcome =
      try do
        case fun.(data) do
          {:ok, _, _} = ok -> ok
          {:error, _} = err -> err
          other -> {:error, {:invalid_return, other}}
        end
      rescue
        e -> {:error, {:exception, Exception.message(e)}}
      catch
        kind, reason -> {:error, {kind, reason}}
      end

    case GenServer.call(__MODULE__, {:finish, op_id, version, outcome}, :infinity) do
      {:retry, new_data, new_version} -> worker_loop(op_id, fun, new_data, new_version)
      :ok -> :ok
    end
  end
end

defmodule MessageSystem.Macros do
  @moduledoc false
  # Macros de domínio.

  @doc """
  Executa um bloco como operação atômica e idempotente.
  O bloco recebe o snapshot na variável `data` e deve retornar
  `{:ok, novo_estado, resultado}` ou `{:error, motivo}`.
  """
  defmacro atomic(op_id, data_var, do: block) do
    quote do
      MessageSystem.Store.run(unquote(op_id), fn unquote(data_var) -> unquote(block) end)
    end
  end

  @doc """
  Gera as funções CRUD de um recurso.
  Para `:users` gera create_user/2, get_user/1, update_user/3 e delete_user/2.
  """
  defmacro defresource(resource) when is_atom(resource) do
    singular = resource |> Atom.to_string() |> String.trim_trailing("s")
    create_fn = String.to_atom("create_" <> singular)
    get_fn = String.to_atom("get_" <> singular)
    update_fn = String.to_atom("update_" <> singular)
    delete_fn = String.to_atom("delete_" <> singular)

    quote do
      def unquote(create_fn)(op_id, attrs) when is_map(attrs) do
        MessageSystem.Store.run(op_id, fn data ->
          MessageSystem.Repo.insert(data, unquote(resource), attrs)
        end)
      end

      def unquote(get_fn)(id) do
        MessageSystem.Store.query(fn data ->
          MessageSystem.Repo.fetch(data, unquote(resource), id)
        end)
      end

      def unquote(update_fn)(op_id, id, changes) when is_map(changes) do
        MessageSystem.Store.run(op_id, fn data ->
          MessageSystem.Repo.update(data, unquote(resource), id, changes)
        end)
      end

      def unquote(delete_fn)(op_id, id) do
        MessageSystem.Store.run(op_id, fn data ->
          MessageSystem.Repo.delete(data, unquote(resource), id)
        end)
      end
    end
  end
end

defmodule MessageSystem do
  @moduledoc """
  API pública do sistema de troca de mensagens.

  Cada operação de escrita recebe um `op_id` único. Repetir o mesmo `op_id`
  retorna `{:error, :already_executed}` e não altera o estado. Consulte
  `MessageSystem.Store.status/1` para ver o resultado de uma operação.
  """
  import MessageSystem.Macros

  defresource :users
  defresource :messages

  @doc "Gera um identificador único para uma operação."
  def new_op_id, do: make_ref() |> :erlang.ref_to_list() |> to_string()

  @doc "Envia uma mensagem de `from_id` para `to_id` de forma atômica."
  def send_message(op_id, from_id, to_id, body) when is_binary(body) do
    atomic op_id, data do
      with {:ok, _} <- MessageSystem.Repo.fetch(data, :users, from_id),
           {:ok, _} <- MessageSystem.Repo.fetch(data, :users, to_id) do
        MessageSystem.Repo.insert(data, :messages, %{
          from: from_id,
          to: to_id,
          body: body,
          read: false
        })
      end
    end
  end

  @doc "Lista as mensagens recebidas por um usuário, da mais antiga para a mais nova."
  def inbox(user_id) do
    MessageSystem.Store.query(fn data ->
      data.messages
      |> Map.values()
      |> Enum.filter(&(&1.to == user_id))
      |> Enum.sort_by(& &1.id)
    end)
  end
end

defmodule MessageSystem.Supervisor do
  @moduledoc false
  # Árvore de supervisão: Task.Supervisor para workers e o Store.
  use Supervisor

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, :ok, Keyword.put_new(opts, :name, __MODULE__))
  end

  @impl true
  def init(:ok) do
    children = [
      {Task.Supervisor, name: MessageSystem.TaskSupervisor},
      MessageSystem.Store
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end
