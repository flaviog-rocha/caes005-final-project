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
#   - notificações quando regras de condição são atingidas;
#   - consulta do estado das operações (em andamento e concluídas);
#   - persistência via behaviour, com banco fictício em memória (FakeDB);
#   - separação entre erros de validação e falhas operacionais.
#
# Convenções de retorno:
#   {:ok, valor}                       -> sucesso
#   {:error, {:validation, [msg]}}     -> entrada inválida (responsabilidade do chamador)
#   {:error, {:operational, motivo}}   -> falha do sistema (infra, banco, processo)
#   {:error, :not_found}               -> consulta sem resultado

defmodule SensorMonitor.Reading do
  @moduledoc "Leitura de sensor já validada."

  @enforce_keys [:sensor_id, :value, :timestamp]
  defstruct [:sensor_id, :value, :timestamp]

  @type t :: %__MODULE__{
          sensor_id: String.t(),
          value: number(),
          timestamp: integer()
        }
end

defmodule SensorMonitor.Rule do
  @moduledoc "Regra de condição aplicada às leituras de um sensor."

  @enforce_keys [:id, :sensor_id, :operator, :threshold, :message]
  defstruct [:id, :sensor_id, :operator, :threshold, :message]

  @type t :: %__MODULE__{
          id: String.t(),
          sensor_id: String.t(),
          operator: :gt | :gte | :lt | :lte | :eq,
          threshold: number(),
          message: String.t()
        }

  @doc "Retorna true quando o valor satisfaz a condição da regra."
  def triggered?(%__MODULE__{operator: op, threshold: t}, value) do
    case op do
      :gt -> value > t
      :gte -> value >= t
      :lt -> value < t
      :lte -> value <= t
      :eq -> value == t
    end
  end
end

defmodule SensorMonitor.Validation do
  @moduledoc """
  Validação pura de entradas.
  Retorna apenas `{:ok, struct}` ou `{:error, {:validation, msgs}}`.
  Nunca produz erros operacionais.
  """

  alias SensorMonitor.{Reading, Rule}

  @operators [:gt, :gte, :lt, :lte, :eq]

  @doc "Valida atributos de uma leitura de sensor."
  def reading(attrs) when is_map(attrs) do
    []
    |> check(non_empty_string?(attrs[:sensor_id]), "sensor_id deve ser string não vazia")
    |> check(is_number(attrs[:value]), "value deve ser numérico")
    |> check(
      is_nil(attrs[:timestamp]) or is_integer(attrs[:timestamp]),
      "timestamp deve ser inteiro (ms) quando informado"
    )
    |> finish(fn ->
      %Reading{
        sensor_id: attrs.sensor_id,
        value: attrs.value,
        timestamp: attrs[:timestamp] || System.system_time(:millisecond)
      }
    end)
  end

  def reading(_), do: {:error, {:validation, ["a leitura deve ser um mapa"]}}

  @doc "Valida atributos de uma regra de condição."
  def rule(attrs) when is_map(attrs) do
    []
    |> check(non_empty_string?(attrs[:id]), "id deve ser string não vazia")
    |> check(non_empty_string?(attrs[:sensor_id]), "sensor_id deve ser string não vazia")
    |> check(attrs[:operator] in @operators, "operator deve ser um de #{inspect(@operators)}")
    |> check(is_number(attrs[:threshold]), "threshold deve ser numérico")
    |> check(
      is_nil(attrs[:message]) or is_binary(attrs[:message]),
      "message deve ser string quando informada"
    )
    |> finish(fn ->
      %Rule{
        id: attrs.id,
        sensor_id: attrs.sensor_id,
        operator: attrs.operator,
        threshold: attrs.threshold,
        message: attrs[:message] || "Regra #{attrs.id} disparada"
      }
    end)
  end

  def rule(_), do: {:error, {:validation, ["a regra deve ser um mapa"]}}

  defp check(errors, true, _msg), do: errors
  defp check(errors, false, msg), do: [msg | errors]

  defp finish(errors, build) do
    case Enum.reverse(errors) do
      [] -> {:ok, build.()}
      msgs -> {:error, {:validation, msgs}}
    end
  end

  defp non_empty_string?(value), do: is_binary(value) and value != ""
end

defmodule SensorMonitor.Repository do
  @moduledoc """
  Contrato de persistência de leituras.
  Qualquer adaptador (FakeDB, Ecto, outro) pode implementá-lo.
  """

  alias SensorMonitor.Reading

  @callback connect(opts :: keyword()) :: {:ok, term()} | {:error, term()}
  @callback save_reading(conn :: term(), Reading.t()) :: :ok | {:error, term()}
end

defmodule SensorMonitor.FakeDB do
  @moduledoc """
  Banco fictício em memória, baseado em Agent.
  Permite simular latência e falhas operacionais para testes.
  """

  @behaviour SensorMonitor.Repository

  alias SensorMonitor.Reading

  @impl true
  def connect(opts) do
    Agent.start_link(fn ->
      %{rows: [], fail_next: nil, latency_ms: Keyword.get(opts, :latency_ms, 0)}
    end)
  end

  @impl true
  def save_reading(conn, %Reading{} = reading) do
    latency = Agent.get(conn, & &1.latency_ms)
    Process.sleep(latency)

    Agent.get_and_update(conn, fn state ->
      case state.fail_next do
        nil -> {:ok, %{state | rows: [reading | state.rows]}}
        reason -> {{:error, reason}, %{state | fail_next: nil}}
      end
    end)
  end

  @doc "Faz a próxima gravação falhar com o motivo informado (simulação)."
  def simulate_failure(conn, reason) do
    Agent.update(conn, &%{&1 | fail_next: reason})
  end

  @doc "Lista as leituras gravadas, na ordem de chegada."
  def rows(conn) do
    Agent.get(conn, fn state -> Enum.reverse(state.rows) end)
  end
end

defmodule SensorMonitor do
  @moduledoc """
  Servidor de monitoramento de sensores.

  Exemplo de uso:

      {:ok, pid} = SensorMonitor.start_link(name: :monitor, repo_opts: [latency_ms: 50])
      {:ok, "alta"} = SensorMonitor.add_rule(:monitor, %{id: "alta", sensor_id: "s1", operator: :gt, threshold: 30})
      SensorMonitor.subscribe(:monitor)
      {:ok, op_id} = SensorMonitor.ingest(:monitor, %{sensor_id: "s1", value: 42})
      SensorMonitor.list_operations(:monitor, :in_progress)
      SensorMonitor.operation(:monitor, op_id)
  """

  use GenServer

  alias SensorMonitor.{Reading, Rule, Validation}

  @max_notifications 100
  @default_timeout 5_000

  ## API pública

  def start_link(opts \\ []) do
    gen_opts =
      case Keyword.fetch(opts, :name) do
        {:ok, name} -> [name: name]
        :error -> []
      end

    GenServer.start_link(__MODULE__, opts, gen_opts)
  end

  @doc "Cadastra uma regra de condição."
  def add_rule(server \\ __MODULE__, attrs), do: call(server, {:add_rule, attrs})

  @doc "Registra uma leitura. Retorna o id da operação de persistência."
  def ingest(server \\ __MODULE__, attrs), do: call(server, {:ingest, attrs})

  @doc "Lista operações: `:all` ou `:in_progress`."
  def list_operations(server \\ __MODULE__, filter \\ :all)
      when filter in [:all, :in_progress],
      do: call(server, {:list_operations, filter})

  @doc "Consulta o estado de uma operação pelo id."
  def operation(server \\ __MODULE__, op_id), do: call(server, {:operation, op_id})

  @doc "Inscreve um processo para receber `{:sensor_notification, notification}`."
  def subscribe(server \\ __MODULE__, pid \\ self()), do: call(server, {:subscribe, pid})

  @doc "Lista as notificações mais recentes geradas."
  def notifications(server \\ __MODULE__), do: call(server, :notifications)

  # Converte falhas de comunicação com o servidor em erro operacional.
  defp call(server, msg) do
    GenServer.call(server, msg, @default_timeout)
  catch
    :exit, reason -> {:error, {:operational, {:server_unavailable, reason}}}
  end

  ## Callbacks do GenServer

  @impl true
  def init(opts) do
    repo = Keyword.get(opts, :repo, SensorMonitor.FakeDB)
    repo_opts = Keyword.get(opts, :repo_opts, [])

    case repo.connect(repo_opts) do
      {:ok, conn} ->
        {:ok,
         %{
           repo: repo,
           conn: conn,
           rules: %{},
           # op_id => %{id, sensor_id, status, ref, started_at, finished_at, error}
           ops: %{},
           # monitor_ref => op_id (tarefas de persistência)
           monitors: %{},
           # pid => monitor_ref (assinantes de notificações)
           subscribers: %{},
           notifications: []
         }}

      {:error, reason} ->
        {:stop, {:operational, {:repo_connect_failed, reason}}}
    end
  end

  @impl true
  def handle_call({:add_rule, attrs}, _from, state) do
    case Validation.rule(attrs) do
      {:error, _} = error ->
        {:reply, error, state}

      {:ok, %Rule{id: id} = rule} ->
        if Map.has_key?(state.rules, id) do
          {:reply, {:error, {:validation, ["id #{id} já cadastrado"]}}, state}
        else
          {:reply, {:ok, id}, %{state | rules: Map.put(state.rules, id, rule)}}
        end
    end
  end

  def handle_call({:ingest, attrs}, _from, state) do
    case Validation.reading(attrs) do
      {:error, _} = error ->
        {:reply, error, state}

      {:ok, %Reading{} = reading} ->
        op_id = System.unique_integer([:positive, :monotonic])

        state =
          state
          |> start_persistence(op_id, reading)
          |> evaluate_rules(reading)

        {:reply, {:ok, op_id}, state}
    end
  end

  def handle_call({:list_operations, filter}, _from, state) do
    ops =
      state.ops
      |> Map.values()
      |> Enum.filter(&matches_filter?(&1, filter))
      |> Enum.sort_by(& &1.id)
      |> Enum.map(&public_op/1)

    {:reply, {:ok, ops}, state}
  end

  def handle_call({:operation, op_id}, _from, state) do
    case Map.fetch(state.ops, op_id) do
      {:ok, op} -> {:reply, {:ok, public_op(op)}, state}
      :error -> {:reply, {:error, :not_found}, state}
    end
  end

  def handle_call({:subscribe, pid}, _from, state) do
    if Map.has_key?(state.subscribers, pid) do
      {:reply, :ok, state}
    else
      ref = Process.monitor(pid)
      {:reply, :ok, %{state | subscribers: Map.put(state.subscribers, pid, ref)}}
    end
  end

  def handle_call(:notifications, _from, state) do
    {:reply, {:ok, state.notifications}, state}
  end

  @impl true
  def handle_info({:persisted, op_id, result}, state) do
    {:noreply, finish_op(state, op_id, result)}
  end

  def handle_info({:DOWN, ref, :process, pid, reason}, state) do
    case Map.pop(state.monitors, ref) do
      {nil, _} ->
        # Não é uma tarefa de operação: é um assinante que saiu.
        {:noreply, %{state | subscribers: Map.delete(state.subscribers, pid)}}

      {op_id, monitors} ->
        # A tarefa de persistência terminou sem reportar resultado (crash).
        crash = {:error, {:operational, {:persistence_task_crashed, reason}}}
        {:noreply, finish_op(%{state | monitors: monitors}, op_id, crash)}
    end
  end

  def handle_info(_msg, state), do: {:noreply, state}

  ## Operações

  defp start_persistence(state, op_id, %Reading{} = reading) do
    server = self()
    repo = state.repo
    conn = state.conn

    {:ok, pid} =
      Task.start(fn ->
        result = safe_save(repo, conn, reading)
        send(server, {:persisted, op_id, result})
      end)

    ref = Process.monitor(pid)

    op = %{
      id: op_id,
      sensor_id: reading.sensor_id,
      status: :running,
      ref: ref,
      started_at: now(),
      finished_at: nil,
      error: nil
    }

    %{
      state
      | ops: Map.put(state.ops, op_id, op),
        monitors: Map.put(state.monitors, ref, op_id)
    }
  end

  defp finish_op(state, op_id, result) do
    case Map.fetch(state.ops, op_id) do
      {:ok, %{status: :running, ref: ref} = op} ->
        Process.demonitor(ref, [:flush])

        {status, error} =
          case result do
            :ok -> {:completed, nil}
            {:error, reason} -> {:failed, reason}
          end

        op = %{op | status: status, error: error, ref: nil, finished_at: now()}

        %{
          state
          | ops: Map.put(state.ops, op_id, op),
            monitors: Map.delete(state.monitors, ref)
        }

      _ ->
        state
    end
  end

  defp safe_save(repo, conn, reading) do
    case repo.save_reading(conn, reading) do
      :ok -> :ok
      {:error, reason} -> {:error, {:operational, {:persistence_failed, reason}}}
    end
  rescue
    e -> {:error, {:operational, {:persistence_exception, Exception.message(e)}}}
  catch
    :exit, reason -> {:error, {:operational, {:repo_unavailable, reason}}}
  end

  defp matches_filter?(_op, :all), do: true
  defp matches_filter?(op, :in_progress), do: op.status == :running

  defp public_op(op), do: Map.drop(op, [:ref])

  ## Notificações

  defp evaluate_rules(state, %Reading{} = reading) do
    state.rules
    |> Map.values()
    |> Enum.filter(&(&1.sensor_id == reading.sensor_id))
    |> Enum.filter(&Rule.triggered?(&1, reading.value))
    |> Enum.reduce(state, fn rule, acc ->
      notify(acc, build_notification(rule, reading))
    end)
  end

  defp build_notification(%Rule{} = rule, %Reading{} = reading) do
    %{
      rule_id: rule.id,
      sensor_id: reading.sensor_id,
      value: reading.value,
      operator: rule.operator,
      threshold: rule.threshold,
      message: rule.message,
      at: reading.timestamp
    }
  end

  defp notify(state, notification) do
    Enum.each(state.subscribers, fn {pid, _ref} ->
      send(pid, {:sensor_notification, notification})
    end)

    %{state | notifications: Enum.take([notification | state.notifications], @max_notifications)}
  end

  defp now, do: System.system_time(:millisecond)
end

# ---------------------------------------------------------------------------
# Opcional: adaptador Ecto (não compilado por padrão; requer :ecto_sql).
# Pode substituir o FakeDB passando `repo: SensorMonitor.EctoDB` no start_link.
#
# defmodule SensorMonitor.Repo.Migrations.CreateSensorReadings do
#   use Ecto.Migration
#
#   def change do
#     create table(:sensor_readings) do
#       add :sensor_id, :string, null: false
#       add :value, :float, null: false
#       add :timestamp, :bigint, null: false
#     end
#
#     create index(:sensor_readings, [:sensor_id, :timestamp])
#   end
# end
#
# defmodule SensorMonitor.EctoDB do
#   @behaviour SensorMonitor.Repository
#
#   @impl true
#   def connect(opts), do: {:ok, Keyword.fetch!(opts, :repo)}
#
#   @impl true
#   def save_reading(repo, %SensorMonitor.Reading{} = r) do
#     case repo.insert_all("sensor_readings", [%{sensor_id: r.sensor_id, value: r.value, timestamp: r.timestamp}]) do
#       {1, _} -> :ok
#       other -> {:error, other}
#     end
#   end
# end
