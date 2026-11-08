# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de transações financeiras, atendendo somente aos seguintes requisitos:
#
# 1. Permitir consultar o estado atual das operações em andamento.
# 2. Executar determinadas operações periodicamente.
# 3. Permitir comunicação entre diferentes processos.
# 4. Impedir que uma mesma operação seja executada mais de uma vez.
# 5. Implementar novas tentativas após falhas temporárias.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule Finance.TransactionManager do
  @moduledoc """
  Gerenciador de transações financeiras baseado em GenServer.

  Recursos:
    * Consulta do estado atual das operações (`status/1`, `list/1`).
    * Execução periódica de operações (`schedule_periodic/4`).
    * Comunicação entre processos por assinatura (`subscribe/0`): os assinantes
      recebem `{:transaction_event, id, status, view}` a cada mudança de estado.
    * Idempotência: cada `id` é reservado no momento da submissão e nunca é
      executado novamente, mesmo após concluir, falhar ou ser cancelado.
    * Novas tentativas com backoff exponencial e jitter para falhas temporárias
      (`{:error, :transient, reason}`) ou falhas de processo.

  Contrato das funções de operação (arity 0):
    * `{:ok, valor}`                       -> sucesso
    * `{:error, :transient, motivo}`       -> falha temporária (será re-tentada)
    * `{:error, :permanent, motivo}`       -> falha definitiva (não será re-tentada)
    * qualquer outro valor                 -> tratado como falha permanente
    * exceção                              -> falha permanente
    * saída (`exit`) ou morte do processo  -> falha temporária
  """

  use GenServer

  @default_max_attempts 5
  @base_backoff_ms 200
  @max_backoff_ms 10_000
  @active_statuses [:pending, :running, :retrying, :scheduled]
  @terminal_statuses [:completed, :failed, :cancelled]

  # ---------------------------------------------------------------------------
  # API pública
  # ---------------------------------------------------------------------------

  @doc "Inicia o gerenciador registrado com o nome do módulo."
  def start_link(_opts \\ []) do
    GenServer.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  @doc """
  Submete uma operação única. Retorna `{:ok, :accepted}` ou `{:error, :duplicate}`.
  Opções: `:max_attempts` (padrão #{@default_max_attempts}).
  """
  def submit(id, fun, opts \\ []) when is_function(fun, 0) do
    GenServer.call(__MODULE__, {:submit, id, fun, opts})
  end

  @doc """
  Agenda uma operação para execução periódica a cada `interval_ms`.
  Opções: `:initial_delay` (ms, padrão 0), `:max_attempts` por ciclo.
  Retorna `{:ok, :scheduled}` ou `{:error, :duplicate}`.
  """
  def schedule_periodic(id, interval_ms, fun, opts \\ [])
      when is_integer(interval_ms) and interval_ms > 0 and is_function(fun, 0) do
    GenServer.call(__MODULE__, {:schedule_periodic, id, interval_ms, fun, opts})
  end

  @doc "Cancela uma operação não finalizada. O id permanece reservado."
  def cancel(id), do: GenServer.call(__MODULE__, {:cancel, id})

  @doc "Consulta o estado atual de uma operação."
  def status(id), do: GenServer.call(__MODULE__, {:status, id})

  @doc "Lista operações. Filtros: `:all`, `:active` ou um status específico."
  def list(filter \\ :all), do: GenServer.call(__MODULE__, {:list, filter})

  @doc "Assina eventos de transação do processo chamador."
  def subscribe, do: GenServer.call(__MODULE__, {:subscribe, self()})

  @doc "Remove a assinatura do processo chamador."
  def unsubscribe, do: GenServer.call(__MODULE__, {:unsubscribe, self()})

  # ---------------------------------------------------------------------------
  # Callbacks do GenServer
  # ---------------------------------------------------------------------------

  @impl GenServer
  def init(:ok) do
    # ops:         id => operação
    # running:     id => {pid_do_worker, monitor_ref}
    # subscribers: pid => monitor_ref
    {:ok, %{ops: %{}, running: %{}, subscribers: %{}}}
  end

  @impl GenServer
  def handle_call({:submit, id, fun, opts}, _from, state) do
    if Map.has_key?(state.ops, id) do
      # Idempotência: o id já foi reservado anteriormente.
      {:reply, {:error, :duplicate}, state}
    else
      op = new_op(id, fun, :once, opts)
      state = update_op(state, op)
      state = start_attempt(state, id)
      {:reply, {:ok, :accepted}, state}
    end
  end

  def handle_call({:schedule_periodic, id, interval, fun, opts}, _from, state) do
    if Map.has_key?(state.ops, id) do
      {:reply, {:error, :duplicate}, state}
    else
      op = %{new_op(id, fun, {:periodic, interval}, opts) | status: :scheduled}
      Process.send_after(self(), {:run, id, op.gen}, Keyword.get(opts, :initial_delay, 0))
      {:reply, {:ok, :scheduled}, update_op(state, op)}
    end
  end

  def handle_call({:cancel, id}, _from, state) do
    case Map.fetch(state.ops, id) do
      :error ->
        {:reply, {:error, :not_found}, state}

      {:ok, op} ->
        if op.status in @terminal_statuses do
          {:reply, {:error, :not_cancellable}, state}
        else
          # Mata o worker em execução (se houver) e troca o gen para invalidar timers.
          state = kill_running(state, id)
          op = %{op | status: :cancelled, gen: make_ref()}
          {:reply, :ok, update_op(state, op)}
        end
    end
  end

  def handle_call({:status, id}, _from, state) do
    reply =
      case Map.fetch(state.ops, id) do
        {:ok, op} -> {:ok, view(op)}
        :error -> {:error, :not_found}
      end

    {:reply, reply, state}
  end

  def handle_call({:list, filter}, _from, state) do
    views =
      state.ops
      |> Map.values()
      |> Enum.filter(&matches?(&1, filter))
      |> Enum.map(&view/1)

    {:reply, views, state}
  end

  def handle_call({:subscribe, pid}, _from, state) do
    subs =
      if Map.has_key?(state.subscribers, pid) do
        state.subscribers
      else
        Map.put(state.subscribers, pid, Process.monitor(pid))
      end

    {:reply, :ok, %{state | subscribers: subs}}
  end

  def handle_call({:unsubscribe, pid}, _from, state) do
    case Map.pop(state.subscribers, pid) do
      {nil, _} ->
        {:reply, :ok, state}

      {mon, subs} ->
        Process.demonitor(mon, [:flush])
        {:reply, :ok, %{state | subscribers: subs}}
    end
  end

  @impl GenServer
  # Disparo de execução (primeira execução, próximo ciclo periódico ou retry).
  def handle_info({:run, id, gen}, state) do
    case Map.get(state.ops, id) do
      %{gen: ^gen, status: status} when status in [:pending, :scheduled, :retrying] ->
        {:noreply, start_attempt(state, id)}

      # Timer obsoleto (operação cancelada, concluída ou já em execução).
      _ ->
        {:noreply, state}
    end
  end

  # Resultado reportado pelo worker.
  def handle_info({:attempt_result, id, result}, state) do
    case Map.pop(state.running, id) do
      {nil, _} ->
        {:noreply, state}

      {{_pid, mon}, running} ->
        Process.demonitor(mon, [:flush])
        state = %{state | running: running}
        {:noreply, finish_attempt(state, id, result)}
    end
  end

  # Monitores: assinantes que morreram ou workers que caíram sem reportar resultado.
  def handle_info({:DOWN, mon, :process, pid, reason}, state) do
    cond do
      Map.has_key?(state.subscribers, pid) ->
        {:noreply, %{state | subscribers: Map.delete(state.subscribers, pid)}}

      id = running_id_for(state, mon) ->
        state = %{state | running: Map.delete(state.running, id)}
        {:noreply, finish_attempt(state, id, {:error, :transient, {:crashed, reason}})}

      true ->
        {:noreply, state}
    end
  end

  def handle_info(_msg, state), do: {:noreply, state}

  # ---------------------------------------------------------------------------
  # Execução e controle de tentativas
  # ---------------------------------------------------------------------------

  defp new_op(id, fun, kind, opts) do
    %{
      id: id,
      fun: fun,
      kind: kind,
      status: :pending,
      attempts: 0,
      max_attempts: Keyword.get(opts, :max_attempts, @default_max_attempts),
      last_error: nil,
      last_result: nil,
      gen: make_ref(),
      updated_at: DateTime.utc_now()
    }
  end

  # Inicia um worker monitorado que executa a função e envia o resultado de volta.
  defp start_attempt(state, id) do
    op = Map.fetch!(state.ops, id)
    manager = self()
    fun = op.fun

    {pid, mon} =
      spawn_monitor(fn ->
        result = run_safely(fun)
        send(manager, {:attempt_result, id, result})
      end)

    op = %{op | status: :running, attempts: op.attempts + 1}
    state = %{state | running: Map.put(state.running, id, {pid, mon})}
    update_op(state, op)
  end

  defp run_safely(fun) do
    fun.()
  rescue
    e -> {:error, :permanent, {:exception, Exception.message(e)}}
  catch
    :exit, reason -> {:error, :transient, {:exit, reason}}
    :throw, value -> {:error, :permanent, {:throw, value}}
  end

  defp finish_attempt(state, id, result) do
    op = Map.fetch!(state.ops, id)

    case result do
      {:ok, value} ->
        end_cycle(state, %{op | last_result: value, last_error: nil}, :completed)

      {:error, :transient, reason} ->
        op = %{op | last_error: reason}

        if op.attempts < op.max_attempts do
          schedule_retry(state, op)
        else
          end_cycle(state, op, :failed)
        end

      {:error, :permanent, reason} ->
        end_cycle(state, %{op | last_error: reason}, :failed)

      other ->
        end_cycle(state, %{op | last_error: {:invalid_result, other}}, :failed)
    end
  end

  # Nova tentativa com backoff exponencial (com jitter).
  defp schedule_retry(state, op) do
    Process.send_after(self(), {:run, op.id, op.gen}, backoff(op.attempts))
    update_op(state, %{op | status: :retrying})
  end

  # Operações periódicas reagendam o próximo ciclo; as únicas são as de ciclo único.
  defp end_cycle(state, %{kind: {:periodic, interval}} = op, _outcome) do
    Process.send_after(self(), {:run, op.id, op.gen}, interval)
    update_op(state, %{op | status: :scheduled, attempts: 0})
  end

  defp end_cycle(state, op, outcome) do
    update_op(state, %{op | status: outcome})
  end

  defp backoff(attempt) do
    base = min(@base_backoff_ms * Integer.pow(2, attempt - 1), @max_backoff_ms)
    base + :rand.uniform(div(base, 4) + 1) - 1
  end

  defp kill_running(state, id) do
    case Map.pop(state.running, id) do
      {nil, _} ->
        state

      {{pid, mon}, running} ->
        Process.demonitor(mon, [:flush])
        Process.exit(pid, :kill)
        %{state | running: running}
    end
  end

  defp running_id_for(state, mon) do
    Enum.find_value(state.running, fn {id, {_pid, m}} -> if m == mon, do: id end)
  end

  # ---------------------------------------------------------------------------
  # Estado, notificação e visões
  # ---------------------------------------------------------------------------

  # Armazena a operação e notifica todos os assinantes sobre a mudança.
  defp update_op(state, op) do
    op = %{op | updated_at: DateTime.utc_now()}
    event = {:transaction_event, op.id, op.status, view(op)}

    Enum.each(state.subscribers, fn {pid, _mon} -> send(pid, event) end)

    %{state | ops: Map.put(state.ops, op.id, op)}
  end

  defp view(op) do
    Map.take(op, [:id, :kind, :status, :attempts, :max_attempts, :last_error, :last_result, :updated_at])
  end

  defp matches?(_op, :all), do: true
  defp matches?(op, :active), do: op.status in @active_statuses
  defp matches?(op, status) when is_atom(status), do: op.status == status
end
