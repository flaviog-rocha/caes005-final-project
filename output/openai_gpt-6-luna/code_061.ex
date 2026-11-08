# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de troca de mensagens entre usuários, atendendo somente aos seguintes requisitos:
#
# 1. Permitir comunicação entre diferentes processos.
# 2. Permitir a execução de operações simultâneas.
# 3. Recuperar processos após falhas inesperadas.
# 4. Manter um histórico das operações realizadas.
# 5. Executar determinadas operações periodicamente.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - GenServer
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule MessagingSystem do
  @moduledoc """
  Sistema de mensagens entre processos, com supervisão, histórico e manutenção periódica.
  """

  use Supervisor

  alias MessagingSystem.{History, Scheduler, User}

  @registry MessagingSystem.Registry
  @user_supervisor MessagingSystem.UserSupervisor

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    interval_ms = Keyword.get(opts, :interval_ms, 60_000)

    children = [
      {Registry, keys: :unique, name: @registry},
      History,
      {DynamicSupervisor, strategy: :one_for_one, name: @user_supervisor},
      {Scheduler, interval_ms}
    ]

    Supervisor.init(children, strategy: :rest_for_one)
  end

  @doc "Envia uma mensagem do processo de um usuário para o processo de outro."
  def send_message(from, to, body) do
    with :ok <- ensure_user(from),
         :ok <- ensure_user(to) do
      GenServer.call(via_user(from), {:send_message, to, body})
    end
  end

  @doc "Retorna as mensagens recebidas por um usuário."
  def inbox(username) do
    with :ok <- ensure_user(username) do
      GenServer.call(via_user(username), :inbox)
    end
  end

  @doc "Retorna o histórico de operações, da mais antiga para a mais recente."
  def history do
    History.operations()
  end

  @doc "Retorna o PID atualmente associado a um usuário, se existir."
  def whereis_user(username) do
    case Registry.lookup(@registry, username) do
      [{pid, _value} | _] -> pid
      [] -> nil
    end
  end

  defp ensure_user(username) do
    case Registry.lookup(@registry, username) do
      [{_pid, _value} | _] ->
        :ok

      [] ->
        case DynamicSupervisor.start_child(
               @user_supervisor,
               {User, username}
             ) do
          {:ok, _pid} ->
            :ok

          {:error, {:already_started, _pid}} ->
            :ok

          {:error, reason} ->
            case Registry.lookup(@registry, username) do
              [{_pid, _value} | _] -> :ok
              [] -> {:error, reason}
            end
        end
    end
  end

  defp via_user(username) do
    {:via, Registry, {@registry, username}}
  end
end

defmodule MessagingSystem.User do
  @moduledoc false

  use GenServer

  alias MessagingSystem.History

  def child_spec(username) do
    %{
      id: {__MODULE__, username},
      start: {__MODULE__, :start_link, [username]},
      restart: :permanent,
      shutdown: 5_000,
      type: :worker
    }
  end

  def start_link(username) do
    GenServer.start_link(__MODULE__, username,
      name: {:via, Registry, {MessagingSystem.Registry, username}}
    )
  end

  @impl true
  def init(username) do
    {:ok, %{username: username, inbox: History.inbox(username)}}
  end

  @impl true
  def handle_call({:send_message, to, body}, _from, state) do
    message = %{
      id: System.unique_integer([:positive, :monotonic]),
      from: state.username,
      to: to,
      body: body,
      at: DateTime.utc_now()
    }

    message = History.record_message(message)

    GenServer.cast(
      {:via, Registry, {MessagingSystem.Registry, to}},
      {:new_message, message}
    )

    {:reply, {:ok, message}, state}
  end

  def handle_call(:inbox, _from, state) do
    {:reply, state.inbox, state}
  end

  @impl true
  def handle_cast({:new_message, message}, state) do
    {:noreply, add_message(state, message)}
  end

  defp add_message(state, message) do
    messages =
      [message | state.inbox]
      |> Enum.uniq_by(& &1.id)
      |> Enum.sort_by(& &1.sequence)

    %{state | inbox: messages}
  end
end

defmodule MessagingSystem.History do
  @moduledoc false

  use GenServer

  def start_link(_opts \\ []) do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  def record_message(message) do
    GenServer.call(__MODULE__, {:record_message, message})
  end

  def record_event(type, data) do
    GenServer.call(__MODULE__, {:record_event, type, data})
  end

  def operations do
    GenServer.call(__MODULE__, :operations)
  end

  def inbox(username) do
    GenServer.call(__MODULE__, {:inbox, username})
  end

  @impl true
  def init(_opts) do
    {:ok, %{sequence: 0, operations: []}}
  end

  @impl true
  def handle_call({:record_message, message}, _from, state) do
    sequence = state.sequence + 1
    message = Map.put(message, :sequence, sequence)

    operation = %{
      sequence: sequence,
      type: :message,
      at: message.at,
      data: message
    }

    {:reply, message,
     %{state | sequence: sequence, operations: [operation | state.operations]}}
  end

  def handle_call({:record_event, type, data}, _from, state) do
    sequence = state.sequence + 1

    operation = %{
      sequence: sequence,
      type: type,
      at: DateTime.utc_now(),
      data: data
    }

    {:reply, operation,
     %{state | sequence: sequence, operations: [operation | state.operations]}}
  end

  def handle_call(:operations, _from, state) do
    {:reply, Enum.reverse(state.operations), state}
  end

  def handle_call({:inbox, username}, _from, state) do
    messages =
      state.operations
      |> Enum.filter(fn operation ->
        operation.type == :message and operation.data.to == username
      end)
      |> Enum.map(& &1.data)
      |> Enum.sort_by(& &1.sequence)

    {:reply, messages, state}
  end
end

defmodule MessagingSystem.Scheduler do
  @moduledoc false

  use GenServer

  alias MessagingSystem.History

  def start_link(interval_ms) do
    GenServer.start_link(__MODULE__, interval_ms, name: __MODULE__)
  end

  @impl true
  def init(interval_ms) when is_integer(interval_ms) and interval_ms > 0 do
    schedule_tick(interval_ms)
    {:ok, interval_ms}
  end

  @impl true
  def handle_info(:maintenance, interval_ms) do
    History.record_event(:scheduled_maintenance, %{
      active_users: Registry.count(MessagingSystem.Registry)
    })

    schedule_tick(interval_ms)
    {:noreply, interval_ms}
  end

  defp schedule_tick(interval_ms) do
    Process.send_after(self(), :maintenance, interval_ms)
  end
end
