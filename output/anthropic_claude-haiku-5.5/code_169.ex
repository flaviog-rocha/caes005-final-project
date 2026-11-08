# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de transações financeiras, atendendo somente aos seguintes requisitos:
#
# 1. Fazer uso de macros.
# 2. Permitir consultar o estado atual das operações em andamento.
# 3. Executar determinadas operações periodicamente.
# 4. Manter um histórico das operações realizadas.
# 5. Recuperar processos após falhas inesperadas.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# arquivo: fin_tx.ex
#
# Gerenciador de transações financeiras (arquivo único, apenas stdlib do Elixir/OTP).
#
# Recursos:
#   1. Macros (FinTx.DSL): `defoperation` e `defjob` geram código em tempo de compilação.
#   2. Consulta de estado das operações em andamento (FinTx.Engine.status/1, in_progress/0).
#   3. Execução periódica de jobs definidos com `defjob` (Process.send_after/3).
#   4. Histórico persistente de operações concluídas ou falhas (:dets).
#   5. Recuperação após falhas: checkpoints por passo em :dets, reinício via Supervisor
#      e retomada do passo pendente a partir do último checkpoint.
#
# Uso:
#   {:ok, _pid} = FinTx.start(modules: [FinTx.Operations], file: "fin_tx.dets")
#   {:ok, id}   = FinTx.Engine.submit(FinTx.Operations, :transfer, %{amount: 100})
#   FinTx.Engine.status(id)
#   FinTx.Engine.in_progress()
#   FinTx.Engine.history()

defmodule FinTx.DSL do
  @moduledoc """
  DSL baseada em macros para declarar operações transacionais (sequências de passos)
  e jobs periódicos.
  """

  # Ao usar o módulo, registra atributos acumulados e agenda a geração
  # das funções de introspecção no fim da compilação.
  defmacro __using__(_opts) do
    quote do
      import FinTx.DSL
      Module.register_attribute(__MODULE__, :fin_ops, accumulate: true)
      Module.register_attribute(__MODULE__, :fin_jobs, accumulate: true)
      @before_compile FinTx.DSL
    end
  end

  # Gera __fin_operations__/0 e __fin_jobs__/0 com base nos atributos acumulados.
  defmacro __before_compile__(env) do
    ops = env.module |> Module.get_attribute(:fin_ops) |> Enum.reverse()
    jobs = env.module |> Module.get_attribute(:fin_jobs) |> Enum.reverse()

    quote do
      @doc false
      def __fin_operations__, do: unquote(ops)

      @doc false
      def __fin_jobs__, do: unquote(jobs)
    end
  end

  @doc """
  Declara uma operação transacional composta por passos `step "rótulo", fn ctx -> ... end`.
  Cada passo deve retornar `{:ok, novo_ctx}` ou `{:error, motivo}`.
  Gera `name/0`, que retorna a lista `[{rótulo, fun}]`.
  """
  defmacro defoperation(name, do: block) when is_atom(name) do
    steps =
      block
      |> flatten_block()
      |> Enum.map(fn
        {:step, _meta, [label, fun]} when is_binary(label) ->
          quote do: {unquote(label), unquote(fun)}

        other ->
          raise ArgumentError,
                "dentro de defoperation só é permitido `step \"rótulo\", fn ctx -> ... end`, " <>
                  "recebido: #{Macro.to_string(other)}"
      end)

    quote do
      @fin_ops unquote(name)
      def unquote(name)(), do: unquote(steps)
    end
  end

  @doc """
  Declara um job periódico. `every:` é o intervalo em milissegundos.
  O corpo é executado a cada intervalo pelo FinTx.Engine.
  """
  defmacro defjob(name, opts, do: body) when is_atom(name) do
    every = Keyword.fetch!(opts, :every)

    quote do
      @fin_jobs {unquote(name), unquote(every)}
      def unquote(name)(), do: unquote(body)
    end
  end

  defp flatten_block({:__block__, _meta, exprs}), do: exprs
  defp flatten_block(expr), do: [expr]
end

defmodule FinTx.Operations do
  @moduledoc """
  Exemplo de operações e jobs declarados com a DSL.
  """
  use FinTx.DSL

  # Operação de transferência: cada passo é um checkpoint persistido.
  # Os passos devem ser idempotentes, pois, após uma falha, o passo pendente pode ser reexecutado.
  defoperation :transfer do
    step "validar", fn ctx ->
      if is_number(ctx[:amount]) and ctx.amount > 0,
        do: {:ok, Map.put(ctx, :validated, true)},
        else: {:error, :invalid_amount}
    end

    step "debitar", fn ctx ->
      # Aqui entraria a chamada ao sistema de contas (idempotente via ctx.id ou similar).
      {:ok, Map.put(ctx, :debited, true)}
    end

    step "creditar", fn ctx ->
      {:ok, Map.put(ctx, :credited, true)}
    end
  end

  # Job periódico de conciliação.
  defjob :reconcile, every: 5_000 do
    # Aqui entraria a lógica de conciliação de saldos.
    :ok
  end
end

defmodule FinTx.Engine do
  @moduledoc false
  # Núcleo do gerenciador: mantém operações em andamento, agenda jobs,
  # persiste checkpoints e retoma operações após reinício.
  use GenServer

  @max_attempts 3
  @backoff_ms 200
  @task_sup FinTx.TaskSup
  @terminal [:done, :failed]

  # ---------------------------------------------------------------------------
  # API pública
  # ---------------------------------------------------------------------------

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  @doc "Submete uma operação declarada com defoperation/2 em um módulo."
  def submit(mod, op, ctx \\ %{}) when is_atom(mod) and is_atom(op) and is_map(ctx),
    do: GenServer.call(__MODULE__, {:submit, mod, op, ctx})

  @doc "Consulta o estado de uma operação (em andamento ou já finalizada)."
  def status(id), do: GenServer.call(__MODULE__, {:status, id})

  @doc "Lista as operações em andamento."
  def in_progress, do: GenServer.call(__MODULE__, :in_progress)

  @doc "Lista o histórico de operações finalizadas (mais recentes primeiro)."
  def history, do: GenServer.call(__MODULE__, :history)

  # ---------------------------------------------------------------------------
  # Inicialização
  # ---------------------------------------------------------------------------

  @impl true
  def init(opts) do
    file = Keyword.get(opts, :file, "fin_tx.dets")
    modules = Keyword.get(opts, :modules, [])

    # O :dets é fechado automaticamente quando o dono morre, então reabrir após um
    # reinício é seguro.
    {:ok, table} = :dets.open_file(:fin_tx_store, file: String.to_charlist(file), type: :set)

    # Agenda jobs periódicos declarados com defjob/3.
    Enum.each(modules, fn mod ->
      Code.ensure_loaded(mod)

      for {name, ms} <- mod.__fin_jobs__() do
        Process.send_after(self(), {:job, mod, name, ms}, ms)
      end
    end)

    # Recuperação é feita fora do init para não bloquear o supervisor.
    send(self(), :recover)

    {:ok, %{table: table, ops: %{}, tasks: %{}}}
  end

  # ---------------------------------------------------------------------------
  # Chamadas síncronas
  # ---------------------------------------------------------------------------

  @impl true
  def handle_call({:submit, mod, name, ctx}, _from, state) do
    if Code.ensure_loaded?(mod) and function_exported?(mod, name, 0) do
      id = new_id()

      op = %{
        id: id,
        mod: mod,
        name: name,
        ctx: ctx,
        idx: 0,
        step: nil,
        status: :pending,
        attempts: 0,
        error: nil,
        submitted_at: now(),
        finished_at: nil
      }

      state =
        state
        |> put_op(op)
        |> run_step(id)

      {:reply, {:ok, id}, state}
    else
      {:reply, {:error, :unknown_operation}, state}
    end
  end

  def handle_call({:status, id}, _from, state) do
    {:reply, load_op(state, id), state}
  end

  def handle_call(:in_progress, _from, state) do
    {:reply, Map.values(state.ops), state}
  end

  def handle_call(:history, _from, state) do
    entries =
      :dets.foldl(
        fn
          {{:op, _id}, %{status: s} = op}, acc when s in @terminal -> [op | acc]
          _, acc -> acc
        end,
        [],
        state.table
      )

    {:reply, Enum.sort_by(entries, & &1.finished_at, :desc), state}
  end

  # ---------------------------------------------------------------------------
  # Mensagens assíncronas
  # ---------------------------------------------------------------------------

  # Recuperação: reassume operações não finalizadas a partir do último checkpoint.
  @impl true
  def handle_info(:recover, state) do
    pending =
      :dets.foldl(
        fn {{:op, id}, op}, acc ->
          if op.status in @terminal, do: acc, else: Map.put(acc, id, op)
        end,
        %{},
        state.table
      )

    state =
      Enum.reduce(pending, %{state | ops: pending}, fn {id, _op}, st ->
        run_step(st, id)
      end)

    {:noreply, state}
  end

  # Execução periódica de um job; cada execução roda isolada em um Task.
  def handle_info({:job, mod, name, ms}, state) do
    Task.Supervisor.start_child(@task_sup, fn -> apply(mod, name, []) end)
    Process.send_after(self(), {:job, mod, name, ms}, ms)
    {:noreply, state}
  end

  # Reexecução de uma operação após backoff.
  def handle_info({:retry, id}, state) do
    {:noreply, run_step(state, id)}
  end

  # Resultado de um passo (Task.async_nolink).
  def handle_info({ref, result}, state) when is_reference(ref) do
    case Map.pop(state.tasks, ref) do
      {nil, _} ->
        {:noreply, state}

      {id, tasks} ->
        Process.demonitor(ref, [:flush])
        {:noreply, on_result(%{state | tasks: tasks}, id, result)}
    end
  end

  # Task que travou durante um passo.
  def handle_info({:DOWN, ref, :process, _pid, reason}, state) do
    case Map.pop(state.tasks, ref) do
      {nil, _} ->
        {:noreply, state}

      {id, tasks} ->
        {:noreply, on_result(%{state | tasks: tasks}, id, {:error, {:crash, reason}})}
    end
  end

  def handle_info(_msg, state), do: {:noreply, state}

  # ---------------------------------------------------------------------------
  # Motor de execução
  # ---------------------------------------------------------------------------

  # Executa o passo atual da operação em um Task; ao terminar a lista, finaliza.
  defp run_step(state, id) do
    op = Map.fetch!(state.ops, id)

    case Enum.at(apply(op.mod, op.name, []), op.idx) do
      nil ->
        finish(state, op, :done)

      {label, fun} ->
        op = %{op | status: :running, step: label}

        task =
          Task.Supervisor.async_nolink(@task_sup, fn ->
            safe(fn -> fun.(op.ctx) end)
          end)

        state
        |> put_op(op)
        |> Map.update!(:tasks, &Map.put(&1, task.ref, id))
    end
  end

  defp on_result(state, id, {:ok, ctx}) do
    op = Map.fetch!(state.ops, id)
    op = %{op | ctx: ctx, idx: op.idx + 1, attempts: 0, error: nil}

    state
    |> put_op(op)
    |> run_step(id)
  end

  defp on_result(state, id, {:error, reason}) do
    op = Map.fetch!(state.ops, id)

    if op.attempts + 1 < @max_attempts do
      op = %{op | attempts: op.attempts + 1, error: reason, status: :retrying}
      Process.send_after(self(), {:retry, id}, @backoff_ms * op.attempts)
      put_op(state, op)
    else
      op = %{op | attempts: op.attempts + 1, error: reason}
      finish(state, op, :failed)
    end
  end

  # Normaliza exceções e retornos inválidos em {:error, _}.
  defp safe(fun) do
    case fun.() do
      {:ok, _} = ok -> ok
      {:error, _} = err -> err
      other -> {:error, {:invalid_return, other}}
    end
  rescue
    e -> {:error, {:exception, Exception.message(e)}}
  end

  # ---------------------------------------------------------------------------
  # Persistência e utilidades
  # ---------------------------------------------------------------------------

  # Cada transição de estado é gravada antes de qualquer efeito seguinte (checkpoint).
  defp put_op(state, op) do
    :dets.insert(state.table, {{:op, op.id}, op})
    :dets.sync(state.table)
    %{state | ops: Map.put(state.ops, op.id, op)}
  end

  # Marca a operação como terminal, grava no histórico e remove da lista em andamento.
  defp finish(state, op, status) do
    op = %{op | status: status, finished_at: now()}
    :dets.insert(state.table, {{:op, op.id}, op})
    :dets.sync(state.table)
    %{state | ops: Map.delete(state.ops, op.id)}
  end

  defp load_op(state, id) do
    case Map.fetch(state.ops, id) do
      {:ok, op} ->
        {:ok, op}

      :error ->
        case :dets.lookup(state.table, {:op, id}) do
          [{_, op}] -> {:ok, op}
          [] -> {:error, :not_found}
        end
    end
  end

  defp new_id do
    "#{System.system_time(:microsecond)}-#{System.unique_integer([:positive, :monotonic])}"
  end

  defp now, do: System.system_time(:millisecond)
end

defmodule FinTx.Root do
  @moduledoc false
  # Supervisor raiz: reinicia o Engine e o Task.Supervisor em caso de falha.
  use Supervisor

  def start_link(opts), do: Supervisor.start_link(__MODULE__, opts, name: __MODULE__)

  @impl true
  def init(opts) do
    children = [
      {Task.Supervisor, name: FinTx.TaskSup},
      {FinTx.Engine, opts}
    ]

    Supervisor.init(children, strategy: :one_for_one, max_restarts: 10, max_seconds: 60)
  end
end

defmodule FinTx do
  @moduledoc false
  # Ponto de entrada do sistema.

  @doc "Inicia o gerenciador. Opções: `:modules` (módulos com defoperation/defjob) e `:file` (caminho do :dets)."
  def start(opts \\ []), do: FinTx.Root.start_link(opts)
end
