# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de troca de mensagens entre usuários, atendendo somente aos seguintes requisitos:
#
# 1. Fazer uso de macros.
# 2. Permitir comunicação entre diferentes processos.
# 3. Garantir a consistência dos dados durante operações concorrentes.
# 4. Persistir os dados para recuperação após reinicializações.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - Task
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule MessageSystem do
  use GenServer

  @table :message_system_store

  defmacro __using__(opts) do
    server = Keyword.get(opts, :server, MessageSystem)

    quote do
      @message_system_server unquote(server)

      def deliver(from, to, body) do
        MessageSystem.deliver(@message_system_server, from, to, body)
      end

      def deliver_async(from, to, body) do
        MessageSystem.deliver_async(@message_system_server, from, to, body)
      end

      def inbox(user) do
        MessageSystem.inbox(@message_system_server, user)
      end
    end
  end

  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)

    case name do
      nil -> GenServer.start_link(__MODULE__, opts)
      _ -> GenServer.start_link(__MODULE__, opts, name: name)
    end
  end

  def deliver(server \\ __MODULE__, from, to, body) do
    GenServer.call(server, {:deliver, from, to, body}, :infinity)
  end

  def deliver_async(server \\ __MODULE__, from, to, body) do
    Task.async(fn -> deliver(server, from, to, body) end)
  end

  def inbox(server \\ __MODULE__, user) do
    GenServer.call(server, {:inbox, user})
  end

  @impl true
  def init(opts) do
    path =
      opts
      |> Keyword.get(:path, "message_system.dets")
      |> Path.expand()
      |> String.to_charlist()

    case :dets.open_file(@table, file: path, type: :set, repair: true) do
      {:ok, @table} -> load_state()
      {:error, reason} -> {:stop, {:cannot_open_store, reason}}
    end
  end

  @impl true
  def handle_call({:deliver, from, to, body}, _caller, state) do
    message = %{
      id: state.next_id,
      from: from,
      to: to,
      body: body,
      timestamp: System.system_time(:millisecond)
    }

    new_state = %{
      next_id: state.next_id + 1,
      inboxes: Map.update(state.inboxes, to, [message], &[message | &1])
    }

    case persist(new_state) do
      :ok -> {:reply, {:ok, message}, new_state}
      {:error, reason} -> {:reply, {:error, {:persistence_failed, reason}}, state}
    end
  end

  def handle_call({:inbox, user}, _caller, state) do
    messages =
      state.inboxes
      |> Map.get(user, [])
      |> Enum.reverse()

    {:reply, messages, state}
  end

  @impl true
  def terminate(_reason, _state) do
    :dets.close(@table)
  end

  defp load_state do
    case :dets.lookup(@table, :state) do
      [] ->
        state = %{next_id: 1, inboxes: %{}}

        case persist(state) do
          :ok -> {:ok, state}
          {:error, reason} -> {:stop, {:cannot_initialize_store, reason}}
        end

      [{:state, %{next_id: next_id, inboxes: inboxes} = state}]
      when is_integer(next_id) and is_map(inboxes) ->
        {:ok, state}

      _ ->
        {:stop, :invalid_persisted_state}
    end
  end

  defp persist(state) do
    case :dets.insert(@table, {:state, state}) do
      :ok -> :dets.sync(@table)
      {:error, reason} -> {:error, reason}
    end
  end
end

defmodule MessageSystem.Client do
  use MessageSystem
end
