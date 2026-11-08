# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o monitoramento de sensores, atendendo somente aos seguintes requisitos:
#
# 1. Detectar encerramentos inesperados de processos.
# 2. Permitir limitar a quantidade de operações simultâneas.
# 3. Permitir comunicação entre diferentes processos.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - Task
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# Monitor de sensores com Task, controle de concorrência e comunicação por mensagens.
#
# Requisitos atendidos:
# 1. Detecção de encerramentos inesperados: cada leitura é uma Task não linkada,
#    monitorada pelo coordenador via Process.monitor (DOWN). Crashes e timeouts
#    são reportados aos assinantes.
# 2. Limite de operações simultâneas: fila de leituras com no máximo
#    `max_concurrency` tasks rodando ao mesmo tempo.
# 3. Comunicação entre processos: assinantes recebem mensagens
#    {:sensor_reading, id, valor} e {:sensor_failure, id, motivo}.
#
# Uso:
#   {:ok, _} = SensorMonitor.start_link(max_concurrency: 3, interval: 500, timeout: 1_000)
#   SensorMonitor.register_sensor(:temp, fn -> :rand.uniform(100) end)
#   SensorMonitor.register_sensor(:falho, fn -> raise "falha de hardware" end)
#   SensorMonitor.subscribe()
#   flush/receive para ver {:sensor_reading, ...} e {:sensor_failure, ...}

defmodule SensorMonitor do
  use GenServer

  @default_max_concurrency 4
  @default_interval 1_000
  @default_timeout 2_000

  # ---------------------------------------------------------------------------
  # API pública
  # ---------------------------------------------------------------------------

  @doc "Inicia o coordenador registrado com o nome do módulo."
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc "Registra um sensor identificado por `id` com a função de leitura `read_fun` (aridade 0)."
  def register_sensor(id, read_fun) when is_function(read_fun, 0) do
    GenServer.call(__MODULE__, {:register, id, read_fun})
  end

  @doc "Solicita uma leitura imediata de um sensor, respeitando o limite de concorrência."
  def read_now(id) do
    GenServer.cast(__MODULE__, {:read, id})
  end

  @doc "Assina as mensagens de leitura/falha. Por padrão, assina o processo chamador."
  def subscribe(pid \\ self()) do
    GenServer.call(__MODULE__, {:subscribe, pid})
  end

  @doc "Retorna um snapshot do estado atual (tarefas em execução, fila, sensores)."
  def status do
    GenServer.call(__MODULE__, :status)
  end

  # ---------------------------------------------------------------------------
  # Callbacks do GenServer
  # ---------------------------------------------------------------------------

  @impl true
  def init(opts) do
    # Supervisor de tasks, linkado ao coordenador: se o coordenador cair, as tasks caem junto.
    {:ok, sup} = Task.Supervisor.start_link()

    state = %{
      sup: sup,
      sensors: %{},                 # id => função de leitura
      queue: :queue.new(),          # ids aguardando execução
      queued: MapSet.new(),         # conjunto de ids na fila (evita duplicatas)
      running: %{},                 # task.ref => {id, task, timer_ref}
      subscribers: %{},             # monitor_ref => pid
      max_concurrency: Keyword.get(opts, :max_concurrency, @default_max_concurrency),
      interval: Keyword.get(opts, :interval, @default_interval),
      timeout: Keyword.get(opts, :timeout, @default_timeout)
    }

    schedule_poll(state.interval)
    {:ok, state}
  end

  @impl true
  def handle_call({:register, id, read_fun}, _from, state) do
    {:reply, :ok, %{state | sensors: Map.put(state.sensors, id, read_fun)}}
  end

  def handle_call({:subscribe, pid}, _from, state) do
    # Monitora o assinante para remover automaticamente quando ele encerrar.
    ref = Process.monitor(pid)
    {:reply, :ok, %{state | subscribers: Map.put(state.subscribers, ref, pid)}}
  end

  def handle_call(:status, _from, state) do
    snapshot = %{
      running: Map.values(state.running) |> Enum.map(fn {id, _task, _timer} -> id end),
      queued: MapSet.to_list(state.queued),
      sensors: Map.keys(state.sensors),
      max_concurrency: state.max_concurrency
    }

    {:reply, snapshot, state}
  end

  @impl true
  def handle_cast({:read, id}, state) do
    state =
      state
      |> enqueue(id)
      |> dispatch()

    {:noreply, state}
  end

  @impl true
  # Coleta periódica: enfileira todos os sensores e despacha dentro do limite.
  def handle_info(:poll, state) do
    state =
      state.sensors
      |> Map.keys()
      |> Enum.reduce(state, &enqueue(&2, &1))
      |> dispatch()

    schedule_poll(state.interval)
    {:noreply, state}
  end

  # Resultado normal de uma Task.
  def handle_info({ref, result}, state) when is_reference(ref) do
    case Map.pop(state.running, ref) do
      {{id, _task, timer}, running} ->
        Process.cancel_timer(timer)
        # Remove o monitor e descarta eventual DOWN já enfileirado.
        Process.demonitor(ref, [:flush])
        notify(state, {:sensor_reading, id, result})

        state =
          %{state | running: running}
          |> dispatch()

        {:noreply, state}

      {nil, _} ->
        {:noreply, state}
    end
  end

  # Encerramento inesperado de uma Task (crash) ou de um assinante.
  def handle_info({:DOWN, ref, :process, _pid, reason}, state) do
    case Map.pop(state.running, ref) do
      {{id, _task, timer}, running} ->
        Process.cancel_timer(timer)
        notify(state, {:sensor_failure, id, reason})

        state =
          %{state | running: running}
          |> dispatch()

        {:noreply, state}

      {nil, _} ->
        # Não era uma task: é um assinante que encerrou.
        {:noreply, %{state | subscribers: Map.delete(state.subscribers, ref)}}
    end
  end

  # Timeout de uma leitura: encerra a task à força e reporta falha.
  def handle_info({:timeout, ref}, state) do
    case Map.pop(state.running, ref) do
      {{id, task, _timer}, running} ->
        Task.shutdown(task, :brutal_kill)
        notify(state, {:sensor_failure, id, :timeout})

        state =
          %{state | running: running}
          |> dispatch()

        {:noreply, state}

      {nil, _} ->
        {:noreply, state}
    end
  end

  def handle_info(_msg, state), do: {:noreply, state}

  # ---------------------------------------------------------------------------
  # Funções privadas
  # ---------------------------------------------------------------------------

  defp schedule_poll(interval) do
    Process.send_after(self(), :poll, interval)
  end

  # Adiciona um sensor à fila, evitando duplicatas e leituras já em execução.
  defp enqueue(state, id) do
    cond do
      not Map.has_key?(state.sensors, id) -> state
      MapSet.member?(state.queued, id) -> state
      running?(state, id) -> state

      true ->
        %{
          state
          | queue: :queue.in(id, state.queue),
            queued: MapSet.put(state.queued, id)
        }
    end
  end

  defp running?(state, id) do
    Enum.any?(state.running, fn {_ref, {rid, _task, _timer}} -> rid == id end)
  end

  # Inicia tarefas da fila enquanto houver capacidade disponível.
  defp dispatch(state) do
    if map_size(state.running) < state.max_concurrency do
      case :queue.out(state.queue) do
        {{:value, id}, queue} ->
          state = %{state | queue: queue, queued: MapSet.delete(state.queued, id)}

          case Map.fetch(state.sensors, id) do
            {:ok, read_fun} -> state |> start_task(id, read_fun) |> dispatch()
            :error -> dispatch(state)
          end

        {:empty, _} ->
          state
      end
    else
      state
    end
  end

  # Inicia a leitura como Task não linkada e agenda o timeout.
  defp start_task(state, id, read_fun) do
    task = Task.Supervisor.async_nolink(state.sup, fn -> read_fun.() end)
    timer = Process.send_after(self(), {:timeout, task.ref}, state.timeout)

    %{state | running: Map.put(state.running, task.ref, {id, task, timer})}
  end

  # Comunicação: envia a mensagem a todos os assinantes.
  defp notify(state, message) do
    Enum.each(state.subscribers, fn {_ref, pid} -> send(pid, message) end)
  end
end
