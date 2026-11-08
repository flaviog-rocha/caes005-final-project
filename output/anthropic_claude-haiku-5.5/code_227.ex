# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de gerenciamento de notificações, atendendo somente aos seguintes requisitos:
#
# 1. Gerar notificações quando determinadas condições forem atingidas.
# 2. Quando fizer sentido para o domínio, considerar a comunicação com um banco de dados fictício e, opcionalmente, o uso de Ecto.Migration, sem tornar isso obrigatório.
# 3. Diferenciar erros de validação de falhas operacionais.
# 4. Detectar encerramentos inesperados de processos.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# notification_system.exs
#
# Sistema de gerenciamento de notificações (autocontido, somente biblioteca padrão).
#
# Execução: elixir notification_system.exs
#
# Estratégia de erros:
#   {:error, {:validation, [mensagens]}}  -> erro do chamador (dado inválido, não reexecutar)
#   {:error, {:operational, motivo}}      -> falha de infraestrutura/execução (pode reexecutar)
#
# Detecção de encerramentos inesperados:
#   - Workers são iniciados com spawn_monitor e monitorados via {:DOWN, ...}.
#   - O Dispatcher faz trap_exit e linka o banco fictício para receber {:EXIT, ...}.

defmodule Notifications.Rule do
  @moduledoc """
  Regras de condição. Funções puras: avaliam leituras e produzem
  atributos de notificação, sem efeitos colaterais.
  """

  @enforce_keys [:name, :metric, :op, :threshold, :severity]
  defstruct [:name, :metric, :op, :threshold, :severity]

  @type t :: %__MODULE__{
          name: String.t(),
          metric: atom(),
          op: :gt | :lt | :gte | :lte,
          threshold: number(),
          severity: :info | :warning | :critical
        }

  # Verifica se o valor atinge a condição da regra.
  @doc false
  def matches?(%__MODULE__{op: :gt, threshold: t}, v), do: v > t
  def matches?(%__MODULE__{op: :lt, threshold: t}, v), do: v < t
  def matches?(%__MODULE__{op: :gte, threshold: t}, v), do: v >= t
  def matches?(%__MODULE__{op: :lte, threshold: t}, v), do: v <= t

  @doc """
  Avalia uma leitura validada contra a lista de regras e retorna
  a lista de atributos de notificação geradas (função pura).
  """
  def evaluate(rules, %{metric: metric, value: value}) do
    for rule <- rules,
        rule.metric == metric,
        matches?(rule, value) do
      %{
        type: :threshold,
        severity: rule.severity,
        message: "#{rule.name}: #{metric}=#{value}"
      }
    end
  end
end

defmodule Notifications.Validator do
  @moduledoc """
  Validação de entrada. Sempre retorna erros do tipo `:validation`,
  que indicam problema nos dados e não em infraestrutura.
  """

  @severities [:info, :warning, :critical]
  @types [:threshold, :process_crash]

  @doc "Valida uma leitura de métrica."
  def reading(attrs) when is_map(attrs) do
    []
    |> check(is_atom(Map.get(attrs, :metric)), "metric deve ser um átomo")
    |> check(is_number(Map.get(attrs, :value)), "value deve ser numérico")
    |> to_result(attrs)
  end

  def reading(_), do: {:error, {:validation, ["a leitura deve ser um mapa"]}}

  @doc "Valida atributos de uma notificação antes de persistir."
  def notification(attrs) when is_map(attrs) do
    message = Map.get(attrs, :message)

    []
    |> check(Map.get(attrs, :type) in @types, "type inválido")
    |> check(Map.get(attrs, :severity) in @severities, "severity inválida")
    |> check(is_binary(message) and message != "", "message é obrigatória")
    |> to_result(attrs)
  end

  defp check(errors, true, _msg), do: errors
  defp check(errors, false, msg), do: errors ++ [msg]

  defp to_result(_attrs, [_ | _] = errors), do: {:error, {:validation, errors}}
  defp to_result(attrs, []), do: {:ok, attrs}
end

defmodule Notifications.FakeDB do
  @moduledoc """
  Banco de dados fictício em memória, implementado com Agent.

  Permite simular falhas operacionais com `fail_next/1`.
  Chamadas ao Agent que falham (ex.: processo morto) são convertidas
  em `{:error, {:operational, _}}` em vez de derrubar o chamador.
  """
  use Agent

  def start_link(_opts) do
    Agent.start_link(fn -> %{seq: 0, rows: [], fail_next: 0} end, name: __MODULE__)
  end

  @doc "Insere uma notificação. Retorna {:ok, registro} ou erro operacional."
  def insert(attrs) do
    safe(fn -> Agent.get_and_update(__MODULE__, &do_insert(&1, attrs)) end)
  end

  @doc "Lista todas as notificações em ordem de inserção."
  def all do
    safe(fn -> Agent.get(__MODULE__, fn s -> Enum.reverse(s.rows) end) end)
  end

  @doc "Simula as próximas `n` operações de inserção falhando."
  def fail_next(n) do
    Agent.update(__MODULE__, &Map.put(&1, :fail_next, n))
  end

  defp do_insert(%{fail_next: n} = state, _attrs) when n > 0 do
    {{:error, {:operational, :db_unavailable}}, %{state | fail_next: n - 1}}
  end

  defp do_insert(state, attrs) do
    id = state.seq + 1
    record = Map.merge(attrs, %{id: id, inserted_at: DateTime.utc_now()})
    {{:ok, record}, %{state | seq: id, rows: [record | state.rows]}}
  end

  defp safe(fun) do
    fun.()
  catch
    :exit, reason -> {:error, {:operational, {:db_exit, reason}}}
  end
end

# Migração opcional: só é compilada se Ecto estiver disponível.
# O sistema funciona sem ela, usando o banco fictício acima.
if Code.ensure_loaded?(Ecto.Migration) do
  defmodule Notifications.Migrations.CreateNotifications do
    use Ecto.Migration

    def change do
      create table(:notifications) do
        add :type, :string, null: false
        add :severity, :string, null: false
        add :message, :text, null: false
        timestamps(updated_at: false)
      end
    end
  end
end

defmodule Notifications.Worker do
  @moduledoc """
  Executa o pipeline de uma leitura: validar -> avaliar regras -> persistir.
  Roda em um processo separado, supervisionado pelo Dispatcher.
  """
  alias Notifications.{Rule, Validator, FakeDB}

  def run(reading, rules) do
    with {:ok, reading} <- Validator.reading(reading),
         :ok <- maybe_crash(reading),
         notifications = Rule.evaluate(rules, reading),
         :ok <- persist_all(notifications) do
      {:ok, length(notifications)}
    end
  end

  # Simula uma falha inesperada (exceção não tratada) para fins de teste.
  defp maybe_crash(%{crash: true}), do: raise("falha inesperada no worker")
  defp maybe_crash(_), do: :ok

  defp persist_all(attrs_list) do
    Enum.reduce_while(attrs_list, :ok, fn attrs, :ok ->
      with {:ok, valid} <- Validator.notification(attrs),
           {:ok, _record} <- FakeDB.insert(valid) do
        {:cont, :ok}
      else
        {:error, _} = error -> {:halt, error}
      end
    end)
  end
end

defmodule Notifications.Dispatcher do
  @moduledoc """
  Coordena os workers e detecta encerramentos inesperados:

    * `{:DOWN, ...}` de workers monitorados com saída anormal;
    * `{:EXIT, ...}` de processos linkados (ex.: banco fictício), graças a `trap_exit`.

  Em ambos os casos, o chamador recebe erro operacional e uma notificação
  do tipo `:process_crash` é registrada.
  """
  use GenServer

  alias Notifications.FakeDB

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc "Processa uma leitura de forma síncrona."
  def process(reading, rules, timeout \\ 5_000) do
    GenServer.call(__MODULE__, {:process, reading, rules}, timeout)
  catch
    :exit, {:timeout, _} -> {:error, {:operational, :timeout}}
    :exit, reason -> {:error, {:operational, {:dispatcher_unavailable, reason}}}
  end

  # --- GenServer callbacks ---

  @impl true
  def init(_opts) do
    # Transforma sinais de saída de processos linkados em mensagens {:EXIT, pid, reason}.
    Process.flag(:trap_exit, true)

    # Linka o banco fictício para ser notificado se ele cair.
    case Process.whereis(FakeDB) do
      nil -> :ok
      pid -> Process.link(pid)
    end

    {:ok, %{workers: %{}}}
  end

  @impl true
  def handle_call({:process, reading, rules}, from, state) do
    parent = self()

    {pid, ref} =
      spawn_monitor(fn ->
        send(parent, {:worker_result, self(), Notifications.Worker.run(reading, rules)})
      end)

    workers = Map.put(state.workers, pid, {ref, from})
    {:noreply, %{state | workers: workers}}
  end

  @impl true
  def handle_info({:worker_result, pid, result}, state) do
    case Map.pop(state.workers, pid) do
      {{ref, from}, workers} ->
        # Resultado recebido: o DOWN normal que virá depois não precisa mais ser tratado.
        Process.demonitor(ref, [:flush])
        GenServer.reply(from, result)
        {:noreply, %{state | workers: workers}}

      {nil, _} ->
        {:noreply, state}
    end
  end

  def handle_info({:DOWN, ref, :process, pid, :normal}, state) do
    # Encerramento normal sem resultado pendente: nada a fazer.
    {_entry, workers} = Map.pop(state.workers, pid)
    _ = ref
    {:noreply, %{state | workers: workers}}
  end

  def handle_info({:DOWN, ref, :process, pid, reason}, state) do
    case Map.pop(state.workers, pid) do
      {{^ref, from}, workers} ->
        GenServer.reply(from, {:error, {:operational, {:worker_crashed, reason}}})
        record_crash("worker #{inspect(pid)} encerrou: #{inspect(reason)}")
        {:noreply, %{state | workers: workers}}

      _ ->
        {:noreply, state}
    end
  end

  def handle_info({:EXIT, _pid, :normal}, state), do: {:noreply, state}

  def handle_info({:EXIT, pid, reason}, state) do
    # Processo linkado (ex.: banco) terminou inesperadamente.
    record_crash("processo linkado #{inspect(pid)} encerrou: #{inspect(reason)}")
    {:noreply, state}
  end

  defp record_crash(message) do
    attrs = %{type: :process_crash, severity: :critical, message: message}

    case FakeDB.insert(attrs) do
      {:ok, _} -> :ok
      {:error, reason} -> IO.warn("Falha ao registrar crash: #{inspect(reason)}")
    end
  end
end

defmodule Notifications do
  @moduledoc """
  Ponto de entrada: inicia o banco fictício e o dispatcher sob um supervisor.
  """

  def start_link(_opts \\ []) do
    Supervisor.start_link([Notifications.FakeDB, Notifications.Dispatcher],
      strategy: :one_for_one,
      name: Notifications.Supervisor
    )
  end
end

defmodule Notifications.Demo do
  @moduledoc "Demonstração dos cenários: sucesso, validação, operacional e crash."
  alias Notifications.{Rule, Dispatcher, FakeDB}

  def run do
    {:ok, _sup} = Notifications.start_link()

    rules = [
      %Rule{name: "CPU alta", metric: :cpu, op: :gt, threshold: 90, severity: :critical},
      %Rule{name: "Memória alta", metric: :mem, op: :gte, threshold: 80, severity: :warning}
    ]

    # Sucesso: gera uma notificação.
    IO.inspect(Dispatcher.process(%{metric: :cpu, value: 95}, rules), label: "sucesso")

    # Erro de validação: dado inválido.
    IO.inspect(Dispatcher.process(%{metric: :cpu, value: "x"}, rules), label: "validação")

    # Falha operacional: banco indisponível na próxima escrita.
    FakeDB.fail_next(1)
    IO.inspect(Dispatcher.process(%{metric: :mem, value: 85}, rules), label: "operacional")

    # Encerramento inesperado de worker.
    IO.inspect(Dispatcher.process(%{metric: :mem, value: 85, crash: true}, rules),
      label: "crash"
    )

    IO.inspect(FakeDB.all(), label: "notificações persistidas")
  end
end

Notifications.Demo.run()
