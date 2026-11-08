# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o monitoramento de sensores, atendendo somente aos seguintes requisitos:
#
# 1. Permitir a execução de operações simultâneas.
# 2. Manter um histórico das operações realizadas.
# 3. Diferenciar erros de validação de falhas operacionais.
# 4. Implementar expiração automática de registros.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - Registry
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# Monitor de sensores concorrente com Registry, histórico com expiração
# automática e separação entre erros de validação e falhas operacionais.
#
# Formato dos resultados:
#   {:ok, valor}
#   {:error, {:validation, motivo}}    -> entrada inválida (culpa do chamador)
#   {:error, {:operational, motivo}}   -> falha do sistema (timeout, processo caído etc.)

defmodule SensorMonitor do
  @moduledoc """
  API pública do monitor de sensores.

  Cada sensor é um processo GenServer registrado no `Registry` pelo seu id.
  Leituras em lote são executadas concorrentemente com `Task.async_stream/3`.
  Todas as operações (sucesso ou falha) são registradas no histórico, que
  expira automaticamente após um TTL configurável.
  """

  alias SensorMonitor.{History, Sensor, Validator}

  @registry SensorMonitor.Registry
  @sensor_sup SensorMonitor.SensorSupervisor
  @call_timeout 5_000

  @doc "Inicia a árvore de supervisão. Opções: `:ttl_ms` e `:sweep_interval_ms`."
  def start_link(opts \\ []) do
    children = [
      {Registry, keys: :unique, name: @registry},
      {History, Keyword.take(opts, [:ttl_ms, :sweep_interval_ms])},
      {DynamicSupervisor, strategy: :one_for_one, name: @sensor_sup}
    ]

    Supervisor.start_link(children, strategy: :one_for_one, name: __MODULE__)
  end

  @doc "Registra um sensor. Opções: `:min` e `:max` (opcionais)."
  def register_sensor(sensor_id, opts \\ []) do
    track(sensor_id, :register, fn ->
      with :ok <- Validator.sensor_id(sensor_id),
           {:ok, limits} <- Validator.limits(opts) do
        case DynamicSupervisor.start_child(@sensor_sup, {Sensor, {sensor_id, limits}}) do
          {:ok, _pid} -> :ok
          {:error, {:already_started, _pid}} -> {:error, {:validation, :already_registered}}
          {:error, reason} -> {:error, {:operational, reason}}
        end
      end
    end)
  end

  @doc "Remove um sensor e encerra seu processo."
  def unregister_sensor(sensor_id) do
    track(sensor_id, :unregister, fn ->
      with :ok <- Validator.sensor_id(sensor_id),
           {:ok, pid} <- lookup(sensor_id) do
        case DynamicSupervisor.terminate_child(@sensor_sup, pid) do
          :ok -> :ok
          {:error, reason} -> {:error, {:operational, reason}}
        end
      end
    end)
  end

  @doc "Registra uma leitura para um sensor."
  def record_reading(sensor_id, value) do
    track(sensor_id, :reading, fn ->
      with :ok <- Validator.sensor_id(sensor_id),
           :ok <- Validator.reading(value),
           {:ok, pid} <- lookup(sensor_id) do
        call_sensor(pid, {:reading, value})
      end
    end)
  end

  @doc """
  Processa uma lista de `{sensor_id, valor}` concorrentemente.
  Retorna uma lista de `{sensor_id, resultado}` na mesma ordem da entrada.
  """
  def record_readings(readings) when is_list(readings) do
    if Enum.all?(readings, &match?({_, _}, &1)) do
      readings
      |> Task.async_stream(
        fn {id, value} -> {id, record_reading(id, value)} end,
        max_concurrency: System.schedulers_online() * 2,
        timeout: @call_timeout + 1_000,
        on_timeout: :kill_task,
        ordered: true
      )
      |> Enum.zip(readings)
      |> Enum.map(fn
        {{:ok, pair}, _input} ->
          pair

        # Tarefa encerrada por timeout ou crash: registra a falha operacional.
        {{:exit, reason}, {id, _value}} ->
          result = {:error, {:operational, {:task_failed, reason}}}
          History.record(id, :reading, result)
          {id, result}
      end)
    else
      {:error, {:validation, :invalid_batch}}
    end
  end

  @doc "Retorna as entradas não expiradas do histórico. Opções: `:sensor_id` e `:limit`."
  def history(opts \\ []), do: History.list(opts)

  # --- Auxiliares privados ---

  # Executa a operação, registra o resultado no histórico e o devolve.
  defp track(sensor_id, op, fun) do
    result = fun.()
    History.record(sensor_id, op, result)
    result
  end

  defp lookup(sensor_id) do
    case Registry.lookup(@registry, sensor_id) do
      [{pid, _}] -> {:ok, pid}
      [] -> {:error, {:validation, {:unknown_sensor, sensor_id}}}
    end
  end

  # Converte falhas de processo (timeout, morte do sensor) em erro operacional.
  defp call_sensor(pid, msg) do
    GenServer.call(pid, msg, @call_timeout)
  catch
    :exit, {:timeout, _} -> {:error, {:operational, :timeout}}
    :exit, reason -> {:error, {:operational, {:sensor_down, reason}}}
  end
end

defmodule SensorMonitor.Validator do
  @moduledoc "Validações puras de entrada. Todos os erros aqui são de validação."

  @doc "Valida o id do sensor: string não vazia."
  def sensor_id(id) when is_binary(id) and byte_size(id) > 0, do: :ok
  def sensor_id(_), do: {:error, {:validation, :invalid_sensor_id}}

  @doc "Valida uma leitura: deve ser número."
  def reading(value) when is_number(value), do: :ok
  def reading(_), do: {:error, {:validation, :invalid_reading}}

  @doc "Extrai e valida os limites `:min` e `:max` (ambos opcionais)."
  def limits(opts) when is_list(opts) do
    min = Keyword.get(opts, :min)
    max = Keyword.get(opts, :max)

    cond do
      not (optional_number?(min) and optional_number?(max)) ->
        {:error, {:validation, :invalid_limits}}

      is_number(min) and is_number(max) and min > max ->
        {:error, {:validation, :invalid_limits}}

      true ->
        {:ok, %{min: min, max: max}}
    end
  end

  def limits(_), do: {:error, {:validation, :invalid_options}}

  @doc "Verifica se um valor está dentro dos limites do sensor."
  def check_range(value, %{min: min, max: max}) do
    if (is_nil(min) or value >= min) and (is_nil(max) or value <= max) do
      :ok
    else
      {:error, {:validation, {:out_of_range, value}}}
    end
  end

  defp optional_number?(nil), do: true
  defp optional_number?(value), do: is_number(value)
end

defmodule SensorMonitor.Sensor do
  @moduledoc "Processo de um sensor individual, registrado no `Registry`."

  use GenServer

  alias SensorMonitor.Validator

  # Reinicia somente em caso de falha; encerramento normal não é reiniciado.
  @impl true
  def child_spec({sensor_id, limits}) do
    %{
      id: {__MODULE__, sensor_id},
      start: {__MODULE__, :start_link, [{sensor_id, limits}]},
      restart: :transient
    }
  end

  def start_link({sensor_id, limits}) do
    GenServer.start_link(__MODULE__, {sensor_id, limits}, name: via(sensor_id))
  end

  defp via(sensor_id), do: {:via, Registry, {SensorMonitor.Registry, sensor_id}}

  @impl true
  def init({sensor_id, limits}) do
    {:ok, %{id: sensor_id, limits: limits, count: 0, last: nil}}
  end

  @impl true
  def handle_call({:reading, value}, _from, state) do
    case Validator.check_range(value, state.limits) do
      :ok ->
        new_state = %{state | count: state.count + 1, last: value}
        reply = {:ok, %{sensor_id: state.id, value: value, count: new_state.count}}
        {:reply, reply, new_state}

      {:error, _reason} = error ->
        {:reply, error, state}
    end
  end
end

defmodule SensorMonitor.History do
  @moduledoc """
  Histórico de operações em ETS com expiração automática.

  Cada entrada expira `ttl_ms` após ser gravada. Uma varredura periódica
  remove as expiradas, e `list/1` também ignora entradas vencidas entre
  varreduras.
  """

  use GenServer

  @table __MODULE__
  @default_ttl_ms 60_000
  @default_sweep_ms 5_000

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc "Grava uma operação no histórico."
  def record(sensor_id, op, outcome) do
    now = now_ms()
    ttl = :persistent_term.get({__MODULE__, :ttl_ms})

    entry =
      {System.unique_integer([:monotonic, :positive]), now + ttl,
       System.system_time(:millisecond), sensor_id, op, outcome}

    :ets.insert(@table, entry)
    :ok
  end

  @doc """
  Lista entradas não expiradas, em ordem cronológica.
  Opções: `:sensor_id` (filtro) e `:limit` (padrão 100, retorna as mais recentes).
  """
  def list(opts \\ []) do
    now = now_ms()
    sensor_id = Keyword.get(opts, :sensor_id)
    limit = Keyword.get(opts, :limit, 100)

    @table
    |> :ets.select([{{:_, :"$1", :_, :_, :_, :_}, [{:>, :"$1", now}], [:"$_"]}])
    |> Enum.filter(fn {_key, _exp, _at, id, _op, _outcome} ->
      is_nil(sensor_id) or id == sensor_id
    end)
    |> Enum.take(-limit)
    |> Enum.map(fn {_key, _exp, at, id, op, outcome} ->
      %{at: at, sensor_id: id, op: op, outcome: outcome}
    end)
  end

  @impl true
  def init(opts) do
    ttl = Keyword.get(opts, :ttl_ms, @default_ttl_ms)
    sweep = Keyword.get(opts, :sweep_interval_ms, @default_sweep_ms)

    :persistent_term.put({__MODULE__, :ttl_ms}, ttl)

    :ets.new(@table, [
      :ordered_set,
      :public,
      :named_table,
      read_concurrency: true,
      write_concurrency: true
    ])

    schedule_sweep(sweep)
    {:ok, %{sweep_ms: sweep}}
  end

  @impl true
  def handle_info(:sweep, state) do
    now = now_ms()

    # Remove entradas cujo expires_at já passou.
    :ets.select_delete(@table, [{{:_, :"$1", :_, :_, :_, :_}, [{:"=<", :"$1", now}], [true]}])

    schedule_sweep(state.sweep_ms)
    {:noreply, state}
  end

  defp schedule_sweep(interval), do: Process.send_after(self(), :sweep, interval)

  defp now_ms, do: System.monotonic_time(:millisecond)
end
