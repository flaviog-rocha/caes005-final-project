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

# Monitor de sensores em Elixir (arquivo único, apenas biblioteca padrão)
#
# Requisitos atendidos:
#   1. Detecção de encerramentos inesperados: cada leitura roda em uma Task
#      monitorada. Qualquer término (exceção, exit, kill, timeout) gera evento.
#   2. Limite de operações simultâneas: fila de leituras com concorrência
#      máxima configurável (`max_concurrency`).
#   3. Comunicação entre processos: assinantes recebem mensagens de eventos
#      e qualquer processo pode enviar broadcast.
#
# Uso: SensorMonitor.Demo.run()

defmodule SensorMonitor do
  @moduledoc """
  Monitora sensores periódicos executando leituras em Tasks supervisionadas.
  """

  use GenServer

  @default_max_concurrency 4
  @default_interval_ms 1_000
  @default_timeout_ms 2_000

  # Número de falhas consecutivas a partir do qual o sensor é marcado como :suspended
  @max_failures 3

  ## ---------------------------------------------------------------------------
  ## API pública
  ## ---------------------------------------------------------------------------

  @doc "Inicia o monitor. Opções: :name, :max_concurrency, :timeout."
  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc "Registra um sensor. `read_fun` é uma função de aridade 0. Opções: :interval."
  def register_sensor(server, id, read_fun, opts \\ []) when is_function(read_fun, 0) do
    GenServer.call(server, {:register, id, read_fun, opts})
  end

  @doc "Remove um sensor do monitoramento."
  def unregister_sensor(server, id), do: GenServer.call(server, {:unregister, id})

  @doc "Assina eventos. Assinantes recebem `{:sensor_monitor, evento}`."
  def subscribe(server, pid \\ self()), do: GenServer.call(server, {:subscribe, pid})

  @doc "Cancela a assinatura de eventos."
  def unsubscribe(server, pid \\ self()), do: GenServer.call(server, {:unsubscribe, pid})

  @doc "Envia uma mensagem arbitrária para todos os assinantes."
  def broadcast(server, msg), do: GenServer.cast(server, {:broadcast, msg})

  @doc "Retorna um resumo do estado atual do monitor."
  def status(server), do: GenServer.call(server, :status)

  ## ---------------------------------------------------------------------------
  ## Callbacks do GenServer
  ## ---------------------------------------------------------------------------

  @impl true
  def init(opts) do
    # Supervisor de Tasks ligado ao monitor: garante que as leituras
    # sejam encerradas junto com ele.
    {:ok, sup} = Task.Supervisor.start_link()

    state = %{
      sup: sup,
      max_concurrency: Keyword.get(opts, :max_concurrency, @default_max_concurrency),
      timeout: Keyword.get(opts, :timeout, @default_timeout_ms),
      # id => %{fun, interval, gen, failures, status}
      sensors: %{},
      # fila de ids de sensores aguardando execução
      queue: :queue.new(),
      # ref da Task => {id, %Task{}, timer_ref}
      running: %{},
      # pid assinante => ref de monitoramento
      subscribers: %{}
    }

    {:ok, state}
  end

  @impl true
  def handle_call({:register, id, read_fun, opts}, _from, state) do
    if Map.has_key?(state.sensors, id) do
      {:reply, {:error, :already_registered}, state}
    else
      # `gen` identifica esta "geração" de registro; ticks antigos são descartados
      gen = make_ref()

      sensor = %{
        fun: read_fun,
        interval: Keyword.get(opts, :interval, @default_interval_ms),
        gen: gen,
        failures: 0,
        status: :ok
      }

      schedule_tick(id, gen, 0)
      {:reply, :ok, %{state | sensors: Map.put(state.sensors, id, sensor)}}
    end
  end

  def handle_call({:unregister, id}, _from, state) do
    {:reply, :ok, %{state | sensors: Map.delete(state.sensors, id)}}
  end

  def handle_call({:subscribe, pid}, _from, state) do
    if Map.has_key?(state.subscribers, pid) do
      {:reply, :ok, state}
    else
      # Monitora o assinante para removê-lo automaticamente ao encerrar
      ref = Process.monitor(pid)
      {:reply, :ok, %{state | subscribers: Map.put(state.subscribers, pid, ref)}}
    end
  end

  def handle_call({:unsubscribe, pid}, _from, state) do
    case Map.pop(state.subscribers, pid) do
      {nil, _} ->
        {:reply, :ok, state}

      {ref, subscribers} ->
        Process.demonitor(ref, [:flush])
        {:reply, :ok, %{state | subscribers: subscribers}}
    end
  end

  def handle_call(:status, _from, state) do
    info = %{
      running: map_size(state.running),
      queued: :queue.len(state.queue),
      max_concurrency: state.max_concurrency,
      subscribers: map_size(state.subscribers),
      sensors: Map.new(state.sensors, fn {id, s} -> {id, s.status} end)
    }

    {:reply, info, state}
  end

  @impl true
  def handle_cast({:broadcast, msg}, state) do
    {:noreply, publish(state, {:broadcast, msg})}
  end

  # Tick periódico: enfileira a leitura do sensor se ele ainda estiver registrado
  @impl true
  def handle_info({:tick, id, gen}, state) do
    case Map.get(state.sensors, id) do
      %{gen: ^gen} ->
        state =
          state
          |> enqueue(id)
          |> dispatch()

        {:noreply, state}

      _ ->
        {:noreply, state}
    end
  end

  # Resultado normal de uma Task: {ref, valor}
  def handle_info({ref, result}, state) when is_reference(ref) do
    case Map.pop(state.running, ref) do
      {nil, _} ->
        {:noreply, state}

      {{id, _task, timer}, running} ->
        Process.cancel_timer(timer)
        # Remove a monitoração: a Task já terminou com sucesso
        Process.demonitor(ref, [:flush])

        state =
          %{state | running: running}
          |> finish(id, {:ok, result})
          |> dispatch()

        {:noreply, state}
    end
  end

  # Término da Task (exceção, exit, kill) ou de um assinante
  def handle_info({:DOWN, ref, :process, pid, reason}, state) do
    case Map.pop(state.running, ref) do
      {{id, _task, timer}, running} ->
        Process.cancel_timer(timer)

        state =
          %{state | running: running}
          |> finish(id, {:error, reason})
          |> dispatch()

        {:noreply, state}

      {nil, _} ->
        # Não era uma Task: trata como assinante encerrado
        {:noreply, %{state | subscribers: Map.delete(state.subscribers, pid)}}
    end
  end

  # Timeout de uma leitura: encerra a Task; o DOWN posterior registra o evento
  def handle_info({:timeout, ref}, state) do
    case Map.get(state.running, ref) do
      {_id, task, _timer} ->
        Process.exit(task.pid, :timeout)
        {:noreply, state}

      nil ->
        {:noreply, state}
    end
  end

  def handle_info(_unknown, state), do: {:noreply, state}

  ## ---------------------------------------------------------------------------
  ## Funções privadas
  ## ---------------------------------------------------------------------------

  defp schedule_tick(id, gen, delay) do
    Process.send_after(self(), {:tick, id, gen}, delay)
  end

  defp enqueue(state, id) do
    %{state | queue: :queue.in(id, state.queue)}
  end

  # Inicia leituras enquanto houver vaga respeitando max_concurrency
  defp dispatch(state) do
    if map_size(state.running) < state.max_concurrency and not :queue.is_empty(state.queue) do
      {{:value, id}, queue} = :queue.out(state.queue)
      state = %{state | queue: queue}

      case Map.get(state.sensors, id) do
        nil -> dispatch(state)
        sensor -> state |> start_read(id, sensor) |> dispatch()
      end
    else
      state
    end
  end

  defp start_read(state, id, %{fun: fun}) do
    # async_nolink: o crash da Task não derruba o monitor; chega como mensagem :DOWN
    task = Task.Supervisor.async_nolink(state.sup, fun)
    timer = Process.send_after(self(), {:timeout, task.ref}, state.timeout)
    %{state | running: Map.put(state.running, task.ref, {id, task, timer})}
  end

  # Processa o resultado de uma leitura, atualiza o sensor e agenda a próxima
  defp finish(state, id, outcome) do
    case Map.get(state.sensors, id) do
      nil ->
        # Sensor removido durante a execução
        state

      sensor ->
        {sensor, event} =
          case outcome do
            {:ok, value} ->
              {%{sensor | failures: 0, status: :ok}, {:reading, id, value}}

            {:error, reason} ->
              failures = sensor.failures + 1
              status = if failures >= @max_failures, do: :suspended, else: :degraded
              {%{sensor | failures: failures, status: status}, {:unexpected_exit, id, reason}}
          end

        schedule_tick(id, sensor.gen, sensor.interval)

        state = %{state | sensors: Map.put(state.sensors, id, sensor)}
        publish(state, event)
    end
  end

  # Envia uma mensagem para todos os assinantes (comunicação entre processos)
  defp publish(state, event) do
    Enum.each(Map.keys(state.subscribers), &send(&1, {:sensor_monitor, event}))
    state
  end
end

defmodule SensorMonitor.Demo do
  @moduledoc """
  Demonstração: um sensor estável, um que sempre falha e um que excede o timeout.
  """

  def run do
    {:ok, _pid} = SensorMonitor.start_link(max_concurrency: 2, timeout: 500)
    server = SensorMonitor

    SensorMonitor.subscribe(server, self())

    SensorMonitor.register_sensor(server, :temperatura, fn ->
      Process.sleep(100)
      21.5
    end, interval: 300)

    SensorMonitor.register_sensor(server, :defeituoso, fn ->
      raise "falha simulada no sensor"
    end, interval: 300)

    SensorMonitor.register_sensor(server, :lento, fn ->
      Process.sleep(2_000)
      0
    end, interval: 300)

    listen(2_000)

    IO.inspect(SensorMonitor.status(server), label: "status final")
  end

  # Consome os eventos recebidos durante `ms` milissegundos
  defp listen(ms) do
    receive do
      {:sensor_monitor, event} ->
        IO.inspect(event, label: "evento")
        listen(ms)
    after
      ms -> :ok
    end
  end
end
