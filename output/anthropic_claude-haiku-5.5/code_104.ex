# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de usuários e sessões, atendendo somente aos seguintes requisitos:
#
# 1. Permitir a execução de operações simultâneas.
# 2. Manter um histórico das operações realizadas.
# 3. Permitir limitar a quantidade de operações simultâneas.
# 4. Detectar encerramentos inesperados de processos.
# 5. Impedir que uma mesma operação seja executada mais de uma vez.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - DynamicSupervisor
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# Arquivo: ops_manager.exs
# Gerenciador de operações concorrentes de usuários e sessões.
#
# Execução: `elixir ops_manager.exs`
#
# Requisitos atendidos:
#   1. Execução simultânea: cada operação roda em seu próprio processo (Ops.Worker),
#      iniciado sob um DynamicSupervisor.
#   2. Histórico: o Ops.Manager registra todas as operações concluídas, com falha
#      ou encerradas inesperadamente.
#   3. Limite de concorrência: o DynamicSupervisor é configurado com `max_children`.
#   4. Encerramentos inesperados: o Ops.Manager monitora cada worker e registra
#      o motivo quando ele termina sem reportar resultado.
#   5. Duplicidade: o Ops.Manager mantém o conjunto de IDs já submetidos e rejeita
#      qualquer nova submissão com o mesmo ID.

defmodule Ops do
  @moduledoc """
  Ponto de entrada da aplicação.

  Inicia um Supervisor contendo:
    * um DynamicSupervisor (limitado por `max_concurrency`) para os workers;
    * o Ops.Manager, responsável por histórico, monitoramento e deduplicação.
  """

  use Supervisor

  @worker_sup Ops.WorkerSupervisor

  @doc """
  Inicia o sistema.

  Opções:
    * `:max_concurrency` - número máximo de operações simultâneas (padrão: 2 * schedulers).
  """
  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    max = Keyword.get(opts, :max_concurrency, System.schedulers_online() * 2)

    children = [
      {DynamicSupervisor, name: @worker_sup, strategy: :one_for_one, max_children: max},
      Ops.Manager
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  @doc """
  Submete uma operação identificada por `op_id` para execução assíncrona.

  Retorna:
    * `{:ok, pid}` - operação iniciada;
    * `{:error, :duplicate}` - o `op_id` já foi submetido anteriormente;
    * `{:error, :limit_reached}` - limite de operações simultâneas atingido.
  """
  def submit(op_id, fun) when is_function(fun, 0) do
    Ops.Manager.submit(op_id, fun)
  end

  @doc "Lista as operações em execução (`[{op_id, started_at}]`)."
  def running, do: Ops.Manager.running()

  @doc "Histórico de operações, da mais recente para a mais antiga."
  def history, do: Ops.Manager.history()

  @doc "Nome do DynamicSupervisor de workers (uso interno/diagnóstico)."
  def worker_supervisor, do: @worker_sup
end

defmodule Ops.Manager do
  @moduledoc false
  # Responsável por:
  #   * serializar submissões (atomicidade da checagem de duplicidade e do limite);
  #   * monitorar os workers e detectar encerramentos inesperados;
  #   * manter o histórico de operações.

  use GenServer

  @worker_sup Ops.WorkerSupervisor

  def start_link(_opts \\ []) do
    GenServer.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  def submit(op_id, fun), do: GenServer.call(__MODULE__, {:submit, op_id, fun})
  def running, do: GenServer.call(__MODULE__, :running)
  def history, do: GenServer.call(__MODULE__, :history)

  @impl true
  def init(:ok) do
    # ids: IDs já submetidos (em execução ou finalizados) -> garante execução única
    # running: op_id => %{pid, ref, started_at}
    # history: registros finalizados (mais recente primeiro)
    {:ok, %{ids: MapSet.new(), running: %{}, history: []}}
  end

  @impl true
  def handle_call({:submit, op_id, fun}, _from, state) do
    if MapSet.member?(state.ids, op_id) do
      {:reply, {:error, :duplicate}, state}
    else
      # O DynamicSupervisor aplica o limite de `max_children`.
      case DynamicSupervisor.start_child(@worker_sup, {Ops.Worker, {op_id, fun, self()}}) do
        {:ok, pid} ->
          # Monitora antes de liberar o worker para executar, evitando condições de corrida.
          entry = %{pid: pid, ref: Process.monitor(pid), started_at: DateTime.utc_now()}
          GenServer.cast(pid, :run)

          state = %{
            state
            | ids: MapSet.put(state.ids, op_id),
              running: Map.put(state.running, op_id, entry)
          }

          {:reply, {:ok, pid}, state}

        {:error, :max_children} ->
          {:reply, {:error, :limit_reached}, state}

        {:error, reason} ->
          {:reply, {:error, reason}, state}
      end
    end
  end

  def handle_call(:running, _from, state) do
    list = Enum.map(state.running, fn {id, e} -> {id, e.started_at} end)
    {:reply, list, state}
  end

  def handle_call(:history, _from, state) do
    {:reply, state.history, state}
  end

  @impl true
  # Resultado reportado pelo worker antes de encerrar normalmente.
  def handle_cast({:result, op_id, outcome}, state) do
    case Map.pop(state.running, op_id) do
      {nil, _} ->
        {:noreply, state}

      {entry, running} ->
        Process.demonitor(entry.ref, [:flush])
        status = if match?({:ok, _}, outcome), do: :completed, else: :failed
        record = build_record(op_id, entry, status, outcome)

        {:noreply, %{state | running: running, history: [record | state.history]}}
    end
  end

  @impl true
  # Worker terminou sem reportar resultado: encerramento inesperado.
  def handle_info({:DOWN, ref, :process, _pid, reason}, state) do
    case Enum.find(state.running, fn {_id, e} -> e.ref == ref end) do
      nil ->
        {:noreply, state}

      {op_id, entry} ->
        running = Map.delete(state.running, op_id)
        record = build_record(op_id, entry, :crashed, {:error, {:unexpected_exit, reason}})

        {:noreply, %{state | running: running, history: [record | state.history]}}
    end
  end

  def handle_info(_msg, state), do: {:noreply, state}

  defp build_record(op_id, entry, status, outcome) do
    %{
      id: op_id,
      status: status,
      outcome: outcome,
      started_at: entry.started_at,
      finished_at: DateTime.utc_now()
    }
  end
end

defmodule Ops.Worker do
  @moduledoc false
  # Processo que executa uma única operação. Não é reiniciado automaticamente
  # (`restart: :temporary`): falhas são detectadas e registradas pelo Ops.Manager.

  use GenServer, restart: :temporary

  def start_link({_op_id, _fun, _manager} = args) do
    GenServer.start_link(__MODULE__, args)
  end

  @impl true
  def init(args), do: {:ok, args}

  @impl true
  # Só executa após o Ops.Manager ter registrado e monitorado este processo.
  def handle_cast(:run, {op_id, fun, manager} = state) do
    outcome =
      try do
        {:ok, fun.()}
      rescue
        e -> {:error, {:exception, Exception.message(e)}}
      catch
        kind, reason -> {:error, {kind, reason}}
      end

    GenServer.cast(manager, {:result, op_id, outcome})
    {:stop, :normal, state}
  end
end

defmodule Ops.Demo do
  @moduledoc false
  # Demonstração de operações de usuários e sessões.

  def run do
    {:ok, _} = Ops.start_link(max_concurrency: 3)

    # 1. Execução simultânea de logins de usuários distintos.
    IO.inspect(
      Enum.map(1..2, fn i ->
        Ops.submit({:login, "user#{i}", "req-#{i}"}, fn ->
          Process.sleep(200)
          {:session, "sess-#{i}"}
        end)
      end),
      label: "logins"
    )

    # 2. Limite de concorrência (máximo 3 simultâneas).
    IO.inspect(
      Enum.map(1..5, fn i ->
        Ops.submit({:logout, "user#{i}", "req-#{i}"}, fn -> Process.sleep(300) end)
      end),
      label: "logouts (limite 3)"
    )

    # 3. Duplicidade: mesmo op_id não pode ser executado novamente.
    IO.inspect(Ops.submit({:login, "user1", "req-1"}, fn -> :again end), label: "duplicada")

    # 4. Encerramento inesperado de processo.
    {:ok, pid} = Ops.submit({:session_refresh, "user2", "req-x"}, fn -> Process.sleep(1_000) end)
    Process.exit(pid, :kill)

    # 5. Exceção dentro da operação.
    Ops.submit({:session_close, "user3", "req-y"}, fn -> raise "sessão inválida" end)

    Process.sleep(1_000)
    IO.inspect(Ops.history(), label: "histórico")
  end
end

Ops.Demo.run()
