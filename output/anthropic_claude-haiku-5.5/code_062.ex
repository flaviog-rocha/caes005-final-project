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

# sistema_mensagens.exs
#
# Sistema de troca de mensagens entre usuários com OTP/GenServer.
# Execute com: elixir sistema_mensagens.exs
# Ou carregue no iex com: iex sistema_mensagens.exs, depois ChatSystem.demo()

defmodule ChatSystem do
  @moduledoc """
  Supervisor raiz e API pública do sistema de mensagens.

  Árvore de supervisão:

    ChatSystem (Supervisor :one_for_one)
    ├── Registry (localiza processos de usuário pelo nome)
    ├── ChatSystem.History (GenServer: histórico de operações)
    ├── DynamicSupervisor (supervisiona um GenServer por usuário)
    └── ChatSystem.Maintenance (GenServer: tarefas periódicas)
  """

  use Supervisor

  @registry ChatSystem.Registry
  @user_sup ChatSystem.UserSupervisor

  # ---------------------------------------------------------------------------
  # Inicialização
  # ---------------------------------------------------------------------------

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    children = [
      # Registry com chaves únicas: cada nome de usuário aponta para um único pid.
      {Registry, keys: :unique, name: @registry},

      # Histórico das operações. Iniciado antes dos usuários, pois eles
      # reconstruem a caixa de entrada a partir dele ao (re)iniciar.
      ChatSystem.History,

      # Supervisor dinâmico: cria um processo por usuário sob demanda.
      # Se um usuário falhar, é reiniciado automaticamente (restart: :permanent).
      {DynamicSupervisor, strategy: :one_for_one, name: @user_sup},

      # Processo periódico de manutenção (poda de histórico e relatórios).
      {ChatSystem.Maintenance, Keyword.get(opts, :maintenance, [])}
    ]

    # Se algum filho falhar repetidamente, o supervisor raiz também reinicia.
    Supervisor.init(children, strategy: :one_for_one, max_restarts: 10, max_seconds: 5)
  end

  # ---------------------------------------------------------------------------
  # API pública
  # ---------------------------------------------------------------------------

  @doc "Cadastra um novo usuário, criando seu processo GenServer."
  def register_user(name) when is_binary(name) do
    case DynamicSupervisor.start_child(@user_sup, {ChatSystem.User, name}) do
      {:ok, _pid} -> :ok
      {:error, {:already_started, _pid}} -> {:error, :already_registered}
      other -> other
    end
  end

  @doc "Envia uma mensagem de `from` para `to` (comunicação entre processos)."
  def send_message(from, to, body) do
    ChatSystem.User.send_message(from, to, body)
  end

  @doc """
  Envia a mesma mensagem para vários destinatários em paralelo.
  Cada envio é executado em uma Task separada (operações simultâneas).
  """
  def broadcast(from, recipients, body) when is_list(recipients) do
    recipients
    |> Task.async_stream(
      fn to -> {to, send_message(from, to, body)} end,
      max_concurrency: System.schedulers_online() * 2,
      timeout: 5_000,
      ordered: true
    )
    |> Enum.map(fn {:ok, result} -> result end)
  end

  @doc "Retorna a caixa de entrada de um usuário."
  def inbox(name), do: ChatSystem.User.inbox(name)

  @doc """
  Consulta o histórico de operações.
  Opções: `:limit` (quantidade de registros mais recentes) e `:type` (ex.: `:message`).
  """
  def history(opts \\ []), do: ChatSystem.History.list(opts)

  @doc "Lista os usuários com processo ativo no momento."
  def online_users do
    Registry.select(@registry, [{{:"$1", :_, :_}, [], [:"$1"]}])
  end

  @doc """
  Simula uma falha inesperada, encerrando o processo do usuário.
  O supervisor o reiniciará e a caixa de entrada será reconstruída a partir do histórico.
  """
  def kill_user(name) do
    case Registry.lookup(@registry, name) do
      [{pid, _}] ->
        Process.exit(pid, :kill)
        :ok

      [] ->
        {:error, :not_found}
    end
  end

  @doc "Demonstração completa do sistema."
  def demo do
    {:ok, _} = start_link(maintenance: [interval: 2_000, max_history: 500])

    Enum.each(~w(ana bruno carla), &register_user/1)

    IO.inspect(send_message("ana", "bruno", "Oi, Bruno!"), label: "envio ana->bruno")

    IO.inspect(
      broadcast("carla", ["ana", "bruno", "usuario_inexistente"], "Reunião às 10h"),
      label: "broadcast carla"
    )

    IO.inspect(online_users(), label: "usuários online")

    IO.inspect(kill_user("bruno"), label: "falha simulada em bruno")
    wait_until_online("bruno")

    IO.inspect(inbox("bruno"), label: "caixa de entrada de bruno (recuperada)")
    IO.inspect(history(limit: 5), label: "últimas 5 operações")

    Process.sleep(2_100)
    IO.inspect(ChatSystem.Maintenance.last_report(), label: "relatório periódico")
    :ok
  end

  defp wait_until_online(name, attempts \\ 50)
  defp wait_until_online(_name, 0), do: {:error, :timeout}

  defp wait_until_online(name, attempts) do
    if name in online_users() do
      :ok
    else
      Process.sleep(10)
      wait_until_online(name, attempts - 1)
    end
  end
end

defmodule ChatSystem.User do
  @moduledoc """
  Processo GenServer que representa um usuário.

  Ao iniciar (inclusive após uma falha), reconstrói sua caixa de entrada
  a partir do histórico, garantindo recuperação de estado.
  """

  use GenServer

  # ---------------------------------------------------------------------------
  # API
  # ---------------------------------------------------------------------------

  def start_link(name) do
    GenServer.start_link(__MODULE__, name, name: via(name))
  end

  @doc "Envia uma mensagem a partir deste usuário."
  def send_message(from, to, body) do
    GenServer.call(via(from), {:send, to, body})
  end

  @doc "Retorna a caixa de entrada deste usuário."
  def inbox(name), do: GenServer.call(via(name), :inbox)

  defp via(name), do: {:via, Registry, {ChatSystem.Registry, name}}

  # ---------------------------------------------------------------------------
  # Callbacks
  # ---------------------------------------------------------------------------

  @impl true
  def init(name) do
    # Recuperação: o estado é reconstruído a partir do histórico persistido.
    inbox = ChatSystem.History.inbox_for(name)
    {:ok, %{name: name, inbox: inbox}}
  end

  @impl true
  def handle_call({:send, to, body}, _from, state) do
    case Registry.lookup(ChatSystem.Registry, to) do
      [] ->
        {:reply, {:error, :unknown_recipient}, state}

      [{recipient_pid, _}] ->
        # Primeiro registra no histórico (fonte da verdade), depois entrega.
        # Assim, se o destinatário falhar, a mensagem será recuperada.
        msg = ChatSystem.History.record(:message, state.name, to, body)

        # Comunicação assíncrona entre processos.
        GenServer.cast(recipient_pid, {:deliver, msg})

        {:reply, {:ok, msg.id}, state}
    end
  end

  def handle_call(:inbox, _from, state) do
    {:reply, state.inbox, state}
  end

  @impl true
  def handle_cast({:deliver, msg}, state) do
    {:noreply, %{state | inbox: state.inbox ++ [msg]}}
  end
end

defmodule ChatSystem.History do
  @moduledoc """
  GenServer que mantém o histórico de todas as operações realizadas.
  As entradas são armazenadas da mais recente para a mais antiga.
  """

  use GenServer

  # ---------------------------------------------------------------------------
  # API
  # ---------------------------------------------------------------------------

  def start_link(_opts \\ []) do
    GenServer.start_link(__MODULE__, nil, name: __MODULE__)
  end

  @doc "Registra uma operação no histórico e retorna a entrada criada."
  def record(type, from, to, body) do
    GenServer.call(__MODULE__, {:record, type, from, to, body})
  end

  @doc "Lista entradas do histórico (cronológicas)."
  def list(opts \\ []), do: GenServer.call(__MODULE__, {:list, opts})

  @doc "Retorna as mensagens recebidas por `name`, em ordem cronológica."
  def inbox_for(name), do: GenServer.call(__MODULE__, {:inbox_for, name})

  @doc "Retorna estatísticas agregadas do histórico."
  def stats, do: GenServer.call(__MODULE__, :stats)

  @doc "Remove entradas antigas, mantendo apenas as `max` mais recentes."
  def prune(max) when is_integer(max) and max >= 0 do
    GenServer.cast(__MODULE__, {:prune, max})
  end

  # ---------------------------------------------------------------------------
  # Callbacks
  # ---------------------------------------------------------------------------

  @impl true
  def init(_) do
    {:ok, %{entries: [], next_id: 1}}
  end

  @impl true
  def handle_call({:record, type, from, to, body}, _from, state) do
    entry = %{
      id: state.next_id,
      at: DateTime.utc_now(),
      type: type,
      from: from,
      to: to,
      body: body
    }

    {:reply, entry, %{state | entries: [entry | state.entries], next_id: state.next_id + 1}}
  end

  def handle_call({:list, opts}, _from, state) do
    limit = Keyword.get(opts, :limit, 100)
    type = Keyword.get(opts, :type)

    result =
      state.entries
      |> filter_type(type)
      |> Enum.take(limit)
      |> Enum.reverse()

    {:reply, result, state}
  end

  def handle_call({:inbox_for, name}, _from, state) do
    result =
      state.entries
      |> Enum.filter(&(&1.type == :message and &1.to == name))
      |> Enum.reverse()

    {:reply, result, state}
  end

  def handle_call(:stats, _from, state) do
    stats = %{
      total: length(state.entries),
      by_type: Enum.frequencies_by(state.entries, & &1.type)
    }

    {:reply, stats, state}
  end

  @impl true
  def handle_cast({:prune, max}, state) do
    {:noreply, %{state | entries: Enum.take(state.entries, max)}}
  end

  defp filter_type(entries, nil), do: entries
  defp filter_type(entries, type), do: Enum.filter(entries, &(&1.type == type))
end

defmodule ChatSystem.Maintenance do
  @moduledoc """
  GenServer com tarefas executadas periodicamente:
  - poda do histórico para limitar o uso de memória;
  - geração de relatório com usuários online e estatísticas.
  """

  use GenServer

  @default_interval 5_000
  @default_max_history 1_000

  # ---------------------------------------------------------------------------
  # API
  # ---------------------------------------------------------------------------

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc "Retorna o último relatório gerado pela tarefa periódica."
  def last_report, do: GenServer.call(__MODULE__, :last_report)

  # ---------------------------------------------------------------------------
  # Callbacks
  # ---------------------------------------------------------------------------

  @impl true
  def init(opts) do
    state = %{
      interval: Keyword.get(opts, :interval, @default_interval),
      max_history: Keyword.get(opts, :max_history, @default_max_history),
      last_report: nil
    }

    schedule_tick(state.interval)
    {:ok, state}
  end

  @impl true
  def handle_call(:last_report, _from, state) do
    {:reply, state.last_report, state}
  end

  @impl true
  def handle_info(:tick, state) do
    ChatSystem.History.prune(state.max_history)

    report = %{
      at: DateTime.utc_now(),
      online: ChatSystem.online_users(),
      history: ChatSystem.History.stats()
    }

    schedule_tick(state.interval)
    {:noreply, %{state | last_report: report}}
  end

  defp schedule_tick(interval) do
    Process.send_after(self(), :tick, interval)
  end
end
