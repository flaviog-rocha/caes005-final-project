# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o monitoramento de sensores, atendendo somente aos seguintes requisitos:
#
# 1. Gerar notificações quando determinadas condições forem atingidas.
# 2. Permitir consultar o estado atual das operações em andamento.
# 3. Quando fizer sentido para o domínio, considerar a comunicação com um banco de dados fictício e, opcionalmente, o uso de Ecto.Migration, sem tornar isso obrigatório.
# 4. Diferenciar erros de validação de falhas operacionais.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# sensor_monitor.ex
#
# Monitoramento de sensores com:
#   * notificações quando regras de limiar são atingidas;
#   * consulta do estado das operações em andamento;
#   * persistência via behaviour (store em memória como "banco fictício");
#   * separação entre erros de validação (:validation) e falhas operacionais (:operational).
#
# Usa apenas a biblioteca padrão do Elixir/OTP (GenServer, Task.Supervisor, Agent, DateTime).

defmodule SensorMonitor.Error do
  @moduledoc """
  Erro tipado. O campo `kind` diferencia:

    * `:validation` - entrada inválida. O chamador deve corrigir os dados; reenviar sem
      alteração não faz sentido.
    * `:operational` - falha de execução ou infraestrutura (banco indisponível, task que
      caiu etc.). Pode ser reprocessada.
  """

  @enforce_keys [:kind, :code, :message]
  defstruct [:kind, :code, :message, details: %{}]

  @type kind :: :validation | :operational
  @type t :: %__MODULE__{kind: kind(), code: atom(), message: String.t(), details: map()}

  @spec validation(atom(), String.t(), map()) :: t()
  def validation(code, message, details \\ %{}),
    do: %__MODULE__{kind: :validation, code: code, message: message, details: details}

  @spec operational(atom(), String.t(), map()) :: t()
  def operational(code, message, details \\ %{}),
    do: %__MODULE__{kind: :operational, code: code, message: message, details: details}
end

defmodule SensorMonitor.Reading do
  @moduledoc "Leitura de sensor já validada."

  alias SensorMonitor.Error

  @enforce_keys [:sensor_id, :value, :timestamp]
  defstruct [:sensor_id, :value, :timestamp]

  @type t :: %__MODULE__{sensor_id: String.t(), value: number(), timestamp: DateTime.t()}

  @spec new(term()) :: {:ok, t()} | {:error, Error.t()}
  def new(attrs) when is_map(attrs) do
    # `with` encadeia validações: no primeiro erro, retorna-o sem prosseguir.
    with {:ok, sensor_id} <- fetch_string(attrs, :sensor_id),
         {:ok, value} <- fetch_number(attrs, :value),
         {:ok, ts} <- fetch_timestamp(attrs) do
      {:ok, %__MODULE__{sensor_id: sensor_id, value: value, timestamp: ts}}
    end
  end

  def new(_other),
    do: {:error, Error.validation(:invalid_shape, "a leitura deve ser um mapa")}

  defp fetch_string(attrs, field) do
    case Map.get(attrs, field) do
      v when is_binary(v) and byte_size(v) > 0 -> {:ok, v}
      _ -> {:error, invalid(field, "deve ser string não vazia")}
    end
  end

  defp fetch_number(attrs, field) do
    case Map.get(attrs, field) do
      v when is_number(v) -> {:ok, v}
      _ -> {:error, invalid(field, "deve ser numérico")}
    end
  end

  defp fetch_timestamp(attrs) do
    # Timestamp é opcional; se ausente, usa o instante atual.
    case Map.get(attrs, :timestamp, DateTime.utc_now()) do
      %DateTime{} = ts -> {:ok, ts}
      _ -> {:error, invalid(:timestamp, "deve ser um DateTime")}
    end
  end

  defp invalid(field, msg),
    do: Error.validation(:invalid_field, "#{field} #{msg}", %{field: field})
end

defmodule SensorMonitor.Rule do
  @moduledoc """
  Regra de limiar por sensor. Exemplo:

      %{sensor_id: "temp-1", comparator: :gt, threshold: 80.0, level: :critical}

  Dispara notificação quando `valor > threshold` (`:gt`) ou `valor < threshold` (`:lt`).
  """

  alias SensorMonitor.{Error, Reading}

  @enforce_keys [:sensor_id, :comparator, :threshold, :level]
  defstruct [:sensor_id, :comparator, :threshold, :level]

  @type t :: %__MODULE__{
          sensor_id: String.t(),
          comparator: :gt | :lt,
          threshold: number(),
          level: :info | :warning | :critical
        }

  @comparators [:gt, :lt]
  @levels [:info, :warning, :critical]

  @spec new(map()) :: {:ok, t()} | {:error, Error.t()}
  def new(attrs) when is_map(attrs) do
    rule = %__MODULE__{
      sensor_id: Map.get(attrs, :sensor_id),
      comparator: Map.get(attrs, :comparator),
      threshold: Map.get(attrs, :threshold),
      level: Map.get(attrs, :level, :warning)
    }

    cond do
      not (is_binary(rule.sensor_id) and byte_size(rule.sensor_id) > 0) ->
        invalid(:sensor_id, "deve ser string não vazia")

      rule.comparator not in @comparators ->
        invalid(:comparator, "deve ser :gt ou :lt")

      not is_number(rule.threshold) ->
        invalid(:threshold, "deve ser numérico")

      rule.level not in @levels ->
        invalid(:level, "deve ser :info, :warning ou :critical")

      true ->
        {:ok, rule}
    end
  end

  def new(_other),
    do: {:error, Error.validation(:invalid_shape, "a regra deve ser um mapa")}

  @doc "Avalia uma leitura contra as regras e devolve as notificações geradas."
  @spec evaluate([t()], Reading.t(), term()) :: [map()]
  def evaluate(rules, %Reading{} = reading, op_id) do
    for %__MODULE__{} = rule <- rules,
        rule.sensor_id == reading.sensor_id,
        breached?(rule, reading.value) do
      %{
        op_id: op_id,
        sensor_id: reading.sensor_id,
        level: rule.level,
        comparator: rule.comparator,
        threshold: rule.threshold,
        value: reading.value,
        at: DateTime.utc_now()
      }
    end
  end

  defp breached?(%__MODULE__{comparator: :gt, threshold: t}, v), do: v > t
  defp breached?(%__MODULE__{comparator: :lt, threshold: t}, v), do: v < t

  defp invalid(field, msg),
    do: {:error, Error.validation(:invalid_field, "#{field} #{msg}", %{field: field})}
end

defmodule SensorMonitor.Store do
  @moduledoc """
  Behaviour de persistência. Permite trocar a implementação (ex.: um adaptador
  baseado em Ecto.Repo) sem alterar o servidor.
  """

  @doc "Persiste uma leitura. Retorna `:ok` ou `{:error, motivo}` (falha operacional)."
  @callback save_reading(handle :: term(), reading :: SensorMonitor.Reading.t()) ::
              :ok | {:error, term()}

  @doc """
  Referência de esquema (opcional, apenas documentação) para uma futura migration Ecto:

      # create table(:sensor_readings) do
      #   add :sensor_id, :string, null: false
      #   add :value, :float, null: false
      #   add :recorded_at, :utc_datetime_usec, null: false
      # end
      # create index(:sensor_readings, [:sensor_id, :recorded_at])
  """
end

defmodule SensorMonitor.Store.Memory do
  @moduledoc """
  "Banco de dados" fictício em memória, implementado com Agent.
  Suporta injeção de falha para simular erros operacionais.
  """

  @behaviour SensorMonitor.Store

  @doc "Inicia o armazenamento e retorna o pid usado como handle."
  def start_link(_opts \\ []),
    do: Agent.start_link(fn -> %{readings: [], failure: nil} end)

  @doc "Ativa (ou desativa, com `nil`) uma falha simulada em todas as escritas."
  def simulate_failure(handle, reason),
    do: Agent.update(handle, &Map.put(&1, :failure, reason))

  @doc "Lista leituras persistidas de um sensor."
  def readings(handle, sensor_id) do
    Agent.get(handle, fn %{readings: rs} ->
      Enum.filter(rs, &(&1.sensor_id == sensor_id))
    end)
  end

  @impl true
  def save_reading(handle, reading) do
    Agent.get_and_update(handle, fn
      %{failure: nil} = s -> {:ok, %{s | readings: [reading | s.readings]}}
      %{failure: reason} = s -> {{:error, reason}, s}
    end)
  end
end

defmodule SensorMonitor.Server do
  @moduledoc """
  Servidor que:

    * valida leituras de forma síncrona (erros de validação retornam imediatamente);
    * processa cada leitura válida em uma Task supervisionada (operação assíncrona);
    * mantém o estado das operações e as notificações geradas;
    * entrega notificações ao `:notify` (pid ou função de aridade 1), se informado.
  """

  use GenServer

  alias SensorMonitor.{Error, Reading, Rule}

  # ---------------------------------------------------------------------------
  # API pública
  # ---------------------------------------------------------------------------

  @doc """
  Opções:
    * `:store` - obrigatório, tupla `{modulo_store, handle}`;
    * `:rules` - lista de mapas de regra (opcional);
    * `:notify` - pid ou função `(notificacao -> any)` (opcional);
    * `:name` - nome do processo (opcional).
  """
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, Keyword.take(opts, [:name]))
  end

  @spec add_rule(GenServer.server(), map()) :: :ok | {:error, Error.t()}
  def add_rule(server, attrs), do: GenServer.call(server, {:add_rule, attrs})

  @doc "Submete uma leitura. Retorna `{:ok, op_id}` ou erro de validação."
  @spec submit(GenServer.server(), map()) :: {:ok, integer()} | {:error, Error.t()}
  def submit(server, attrs), do: GenServer.call(server, {:submit, attrs})

  @doc "Consulta o estado de uma operação específica."
  def operation(server, op_id), do: GenServer.call(server, {:operation, op_id})

  @doc "Lista operações ainda em andamento."
  def running_operations(server), do: GenServer.call(server, :running_operations)

  @doc "Lista todas as notificações geradas até o momento."
  def notifications(server), do: GenServer.call(server, :notifications)

  # ---------------------------------------------------------------------------
  # Callbacks
  # ---------------------------------------------------------------------------

  @impl true
  def init(opts) do
    with {:ok, store} <- fetch_store(opts),
         {:ok, rules} <- build_rules(Keyword.get(opts, :rules, [])) do
      {:ok, sup} = Task.Supervisor.start_link()

      {:ok,
       %{
         store: store,
         rules: rules,
         notify: Keyword.get(opts, :notify),
         sup: sup,
         ops: %{},
         # mapa ref_da_task => op_id, para correlacionar resultados
         tasks: %{},
         notifications: []
       }}
    else
      {:error, %Error{} = e} -> {:stop, e}
    end
  end

  @impl true
  def handle_call({:add_rule, attrs}, _from, state) do
    case Rule.new(attrs) do
      {:ok, rule} -> {:reply, :ok, %{state | rules: state.rules ++ [rule]}}
      {:error, _} = err -> {:reply, err, state}
    end
  end

  def handle_call({:submit, attrs}, _from, state) do
    # Erro de validação: falha síncrona, nenhuma operação é criada.
    case Reading.new(attrs) do
      {:error, _} = err ->
        {:reply, err, state}

      {:ok, reading} ->
        op_id = System.unique_integer([:positive, :monotonic])
        {store_mod, handle} = state.store
        rules = state.rules

        task =
          Task.Supervisor.async_nolink(state.sup, fn ->
            run_operation(store_mod, handle, rules, op_id, reading)
          end)

        op = %{
          id: op_id,
          sensor_id: reading.sensor_id,
          status: :running,
          started_at: DateTime.utc_now(),
          finished_at: nil,
          notifications: 0,
          error: nil
        }

        state = %{
          state
          | ops: Map.put(state.ops, op_id, op),
            tasks: Map.put(state.tasks, task.ref, op_id)
        }

        {:reply, {:ok, op_id}, state}
    end
  end

  def handle_call({:operation, op_id}, _from, state) do
    reply =
      case Map.fetch(state.ops, op_id) do
        {:ok, op} -> {:ok, op}
        :error -> {:error, Error.validation(:unknown_operation, "operação não encontrada", %{op_id: op_id})}
      end

    {:reply, reply, state}
  end

  def handle_call(:running_operations, _from, state) do
    running =
      state.ops
      |> Map.values()
      |> Enum.filter(&(&1.status == :running))
      |> Enum.sort_by(& &1.id)

    {:reply, running, state}
  end

  def handle_call(:notifications, _from, state),
    do: {:reply, state.notifications, state}

  # Resultado de uma task concluída normalmente.
  @impl true
  def handle_info({ref, result}, %{tasks: tasks} = state)
      when is_reference(ref) and is_map_key(tasks, ref) do
    # Remove o monitor e descarta o :DOWN que chegaria depois.
    Process.demonitor(ref, [:flush])

    {op_id, tasks} = Map.pop(tasks, ref)
    state = %{state | tasks: tasks}
    now = DateTime.utc_now()

    case result do
      {:ok, notifications} ->
        state = dispatch(notifications, state)

        {:noreply,
         finish_op(state, op_id, %{
           status: :completed,
           finished_at: now,
           notifications: length(notifications)
         })}

      {:error, %Error{} = error} ->
        {:noreply, finish_op(state, op_id, %{status: :failed, finished_at: now, error: error})}
    end
  end

  # Task que terminou com crash (falha operacional não prevista).
  def handle_info({:DOWN, ref, :process, _pid, reason}, %{tasks: tasks} = state)
      when is_map_key(tasks, ref) do
    {op_id, tasks} = Map.pop(tasks, ref)

    error =
      Error.operational(:task_crashed, "a operação foi interrompida", %{
        reason: inspect(reason)
      })

    {:noreply,
     finish_op(%{state | tasks: tasks}, op_id, %{
       status: :failed,
       finished_at: DateTime.utc_now(),
       error: error
     })}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  # ---------------------------------------------------------------------------
  # Funções privadas
  # ---------------------------------------------------------------------------

  # Executada dentro da Task: persiste e avalia regras.
  defp run_operation(store_mod, handle, rules, op_id, reading) do
    case store_mod.save_reading(handle, reading) do
      :ok ->
        {:ok, Rule.evaluate(rules, reading, op_id)}

      {:error, reason} ->
        {:error,
         Error.operational(:store_failed, "falha ao persistir leitura", %{reason: reason})}
    end
  end

  defp dispatch(notifications, state) do
    Enum.each(notifications, fn notification ->
      case state.notify do
        pid when is_pid(pid) -> send(pid, {:sensor_notification, notification})
        fun when is_function(fun, 1) -> fun.(notification)
        nil -> :ok
      end
    end)

    %{state | notifications: state.notifications ++ notifications}
  end

  defp finish_op(state, op_id, changes) do
    %{state | ops: Map.update!(state.ops, op_id, &Map.merge(&1, changes))}
  end

  defp fetch_store(opts) do
    case Keyword.fetch(opts, :store) do
      {:ok, {mod, _handle} = store} when is_atom(mod) ->
        {:ok, store}

      _ ->
        {:error, Error.validation(:invalid_store, "informe :store como {modulo, handle}")}
    end
  end

  defp build_rules(attrs_list) do
    result =
      Enum.reduce_while(attrs_list, {:ok, []}, fn attrs, {:ok, acc} ->
        case Rule.new(attrs) do
          {:ok, rule} -> {:cont, {:ok, [rule | acc]}}
          {:error, _} = err -> {:halt, err}
        end
      end)

    case result do
      {:ok, rules} -> {:ok, Enum.reverse(rules)}
      {:error, _} = err -> err
    end
  end
end

# Exemplo de uso (no iex):
#
#   {:ok, store} = SensorMonitor.Store.Memory.start_link()
#   {:ok, srv} =
#     SensorMonitor.Server.start_link(
#       store: {SensorMonitor.Store.Memory, store},
#       rules: [%{sensor_id: "temp-1", comparator: :gt, threshold: 80, level: :critical}],
#       notify: self()
#     )
#
#   # Erro de validação (síncrono):
#   {:error, %SensorMonitor.Error{kind: :validation}} =
#     SensorMonitor.Server.submit(srv, %{sensor_id: "", value: "x"})
#
#   # Operação assíncrona:
#   {:ok, op_id} = SensorMonitor.Server.submit(srv, %{sensor_id: "temp-1", value: 95})
#   SensorMonitor.Server.operation(srv, op_id)
#   SensorMonitor.Server.running_operations(srv)
#
#   # Notificação recebida no processo chamador:
#   receive do
#     {:sensor_notification, n} -> n
#   end
#
#   # Falha operacional simulada:
#   SensorMonitor.Store.Memory.simulate_failure(store, :db_timeout)
#   {:ok, op2} = SensorMonitor.Server.submit(srv, %{sensor_id: "temp-1", value: 10})
#   {:ok, %{status: :failed, error: %{kind: :operational}}} =
#     SensorMonitor.Server.operation(srv, op2)
