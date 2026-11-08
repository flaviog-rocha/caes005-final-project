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

# Arquivo único: gerenciamento de transações financeiras com OTP.
# Uso: {:ok, _} = FinTx.start_link(data_file: "fintx.dets")
# Dependências: somente a biblioteca padrão (Elixir/OTP).

defmodule FinTx.DSL do
  # Macros que permitem declarar operações e jobs periódicos de forma declarativa.

  defmacro __using__(_opts) do
    quote do
      import FinTx.DSL, only: [deftx: 2, defperiodic: 3]
      Module.register_attribute(__MODULE__, :fintx_ops, accumulate: true)
      Module.register_attribute(__MODULE__, :fintx_periodic, accumulate: true)
      @before_compile FinTx.DSL
    end
  end

  # Gera funções introspectivas com a lista de operações e jobs declarados.
  defmacro __before_compile__(env) do
    ops = env.module |> Module.get_attribute(:fintx_ops) |> Enum.reverse()
    periodic = env.module |> Module.get_attribute(:fintx_periodic) |> Enum.reverse()

    quote do
      @doc false
      def __operations__, do: unquote(Macro.escape(ops))

      @doc false
      def __periodic__, do: unquote(Macro.escape(periodic))
    end
  end

  # Declara uma operação transacional.
  # A função deve retornar {:ok, postings} | {:error, reason},
  # onde postings é uma lista de {conta, delta}.
  defmacro deftx({name, _meta, [arg]}, do: body) when is_atom(name) do
    quote do
      @fintx_ops unquote(name)

      def unquote(name)(unquote(arg)) do
        unquote(body)
      end
    end
  end

  # Declara um job executado periodicamente (a cada `every` milissegundos).
  # A função deve retornar uma lista de {nome_da_operacao, params} a submeter.
  defmacro defperiodic({name, _meta, [arg]}, opts, do: body) do
    interval = Keyword.fetch!(opts, :every)

    quote do
      @fintx_periodic {unquote(name), unquote(interval)}

      def unquote(name)(unquote(arg)) do
        unquote(body)
      end
    end
  end
end

defmodule FinTx.Operations do
  # Catálogo de operações e jobs declarados com as macros do DSL.
  use FinTx.DSL

  # Transferência entre contas: gera lançamentos de débito e crédito.
  deftx transfer(params) do
    from = Map.get(params, :from)
    to = Map.get(params, :to)
    amount = Map.get(params, :amount)

    cond do
      not (is_integer(amount) and amount > 0) -> {:error, :invalid_amount}
      from == nil or to == nil or from == to -> {:error, :invalid_accounts}
      true -> {:ok, [{from, -amount}, {to, amount}]}
    end
  end

  # Depósito simplificado (sem contrapartida, apenas para fins de exemplo).
  deftx deposit(params) do
    to = Map.get(params, :to)
    amount = Map.get(params, :amount)

    if is_integer(amount) and amount > 0 and to != nil,
      do: {:ok, [{to, amount}]},
      else: {:error, :invalid_deposit}
  end

  # Job periódico de exemplo: cobra uma taxa fixa da carteira do cliente.
  # Transferências sem saldo suficiente são rejeitadas pelo motor.
  defperiodic fee_collection(_ctx), every: 10_000 do
    [{:transfer, %{from: "wallet:customer", to: "revenue", amount: 10}}]
  end
end

defmodule FinTx.Engine do
  # Motor transacional: mantém operações em andamento, executa jobs periódicos,
  # persiste checkpoints em DETS (histórico, saldos e estado em voo) e
  # recupera o trabalho pendente após falhas.
  use GenServer

  @tab :fintx_store
  @max_attempts 3

  # ---------------------------------------------------------------------------
  # API pública
  # ---------------------------------------------------------------------------

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def submit(name, params), do: GenServer.call(__MODULE__, {:submit, name, params})
  def status, do: GenServer.call(__MODULE__, :status)
  def history(limit \\ 20), do: GenServer.call(__MODULE__, {:history, limit})
  def balance(account), do: GenServer.call(__MODULE__, {:balance, account})

  # ---------------------------------------------------------------------------
  # Inicialização e recuperação
  # ---------------------------------------------------------------------------

  @impl true
  def init(opts) do
    path = opts |> Keyword.get(:data_file, "fintx.dets") |> String.to_charlist()
    {:ok, @tab} = :dets.open_file(@tab, file: path, type: :set)

    # Recuperação: operações finalizadas são removidas do estado em voo;
    # operações pendentes ou em execução (interrompidas por falha) voltam a pendente.
    all_ops = @tab |> :dets.match_object({{:op, :_}, :_}) |> Enum.map(fn {_, op} -> op end)
    {inflight, finished} = Enum.split_with(all_ops, &(&1.status in [:pending, :running]))

    Enum.each(finished, fn op -> :ok = :dets.delete(@tab, {:op, op.id}) end)
    :ok = :dets.sync(@tab)

    ops = Map.new(inflight, fn op -> {op.id, %{op | status: :pending}} end)

    seq =
      case :dets.lookup(@tab, {:meta, :seq}) do
        [{_, s}] -> s
        [] -> 0
      end

    # Agenda os jobs periódicos declarados via macro.
    Enum.each(FinTx.Operations.__periodic__(), fn {name, ms} ->
      Process.send_after(self(), {:tick, name, ms}, ms)
    end)

    send(self(), :dispatch)
    {:ok, %{ops: ops, tasks: %{}, seq: seq}}
  end

  @impl true
  def terminate(_reason, _state) do
    :dets.close(@tab)
    :ok
  end

  # ---------------------------------------------------------------------------
  # Consultas e comandos síncronos
  # ---------------------------------------------------------------------------

  @impl true
  def handle_call({:submit, name, params}, _from, state) do
    {reply, state} = enqueue(state, name, params)
    {:reply, reply, state}
  end

  def handle_call(:status, _from, state) do
    ops = state.ops |> Map.values() |> Enum.sort_by(& &1.id)
    {:reply, ops, state}
  end

  def handle_call({:history, limit}, _from, state) do
    entries =
      @tab
      |> :dets.match_object({{:hist, :_}, :_})
      |> Enum.map(fn {_, entry} -> entry end)
      |> Enum.sort_by(& &1.op_id, :desc)
      |> Enum.take(limit)

    {:reply, entries, state}
  end

  def handle_call({:balance, account}, _from, state) do
    {:reply, balance_of(account), state}
  end

  # ---------------------------------------------------------------------------
  # Despacho e eventos assíncronos
  # ---------------------------------------------------------------------------

  @impl true
  # Inicia execução de todas as operações pendentes.
  def handle_info(:dispatch, state) do
    state =
      Enum.reduce(state.ops, state, fn
        {id, %{status: :pending} = op}, acc -> start_op(acc, id, op)
        _other, acc -> acc
      end)

    {:noreply, state}
  end

  # Job periódico disparado pelo timer.
  def handle_info({:tick, name, ms}, state) do
    Process.send_after(self(), {:tick, name, ms}, ms)
    task = Task.Supervisor.async_nolink(FinTx.TaskSup, FinTx.Operations, name, [%{at: now()}])
    {:noreply, %{state | tasks: Map.put(state.tasks, task.ref, {:periodic, name, ms})}}
  end

  # Resultado de uma tarefa concluída com sucesso.
  def handle_info({ref, result}, state) when is_reference(ref) do
    Process.demonitor(ref, [:flush])

    case Map.pop(state.tasks, ref) do
      {nil, _} ->
        {:noreply, state}

      {tag, tasks} ->
        {:noreply, complete(tag, result, %{state | tasks: tasks})}
    end
  end

  # Tarefa que falhou inesperadamente.
  def handle_info({:DOWN, ref, :process, _pid, reason}, state) do
    case Map.pop(state.tasks, ref) do
      {nil, _} ->
        {:noreply, state}

      {tag, tasks} ->
        {:noreply, crashed(tag, reason, %{state | tasks: tasks})}
    end
  end

  def handle_info(_msg, state), do: {:noreply, state}

  # ---------------------------------------------------------------------------
  # Lógica interna
  # ---------------------------------------------------------------------------

  defp enqueue(state, name, params) do
    if name in FinTx.Operations.__operations__() do
      id = state.seq + 1

      op = %{
        id: id,
        name: name,
        params: params,
        status: :pending,
        attempts: 0,
        submitted_at: now()
      }

      # Checkpoint: a submissão é persistida antes de ser reconhecida.
      :ok = persist([{{:op, id}, op}, {{:meta, :seq}, id}])
      send(self(), :dispatch)
      {{:ok, id}, %{state | seq: id, ops: Map.put(state.ops, id, op)}}
    else
      {{:error, :unknown_operation}, state}
    end
  end

  defp start_op(state, id, op) do
    task = Task.Supervisor.async_nolink(FinTx.TaskSup, FinTx.Operations, op.name, [op.params])
    op = %{op | status: :running, attempts: op.attempts + 1}
    :ok = persist([{{:op, id}, op}])

    %{
      state
      | ops: Map.put(state.ops, id, op),
        tasks: Map.put(state.tasks, task.ref, {:op, id})
    }
  end

  defp complete({:periodic, _name, _ms}, result, state) do
    case result do
      submissions when is_list(submissions) ->
        Enum.reduce(submissions, state, fn {name, params}, acc ->
          {_reply, acc} = enqueue(acc, name, params)
          acc
        end)

      _other ->
        state
    end
  end

  defp complete({:op, id}, result, state) do
    op = Map.fetch!(state.ops, id)

    case result do
      {:ok, postings} when is_list(postings) ->
        if Enum.all?(postings, &valid_posting?/1) do
          commit(state, op, postings)
        else
          settle(state, op, :rejected, postings, :invalid_postings)
        end

      {:error, reason} ->
        settle(state, op, :rejected, [], reason)

      other ->
        settle(state, op, :rejected, [], {:invalid_result, other})
    end
  end

  defp valid_posting?({_account, amount}), do: is_integer(amount)
  defp valid_posting?(_), do: false

  # Aplica os lançamentos somente se nenhum saldo ficar negativo.
  defp commit(state, op, postings) do
    deltas =
      Enum.reduce(postings, %{}, fn {acct, amt}, acc ->
        Map.update(acc, acct, amt, &(&1 + amt))
      end)

    new_balances = Enum.map(deltas, fn {acct, delta} -> {acct, balance_of(acct) + delta} end)

    if Enum.all?(new_balances, fn {_acct, value} -> value >= 0 end) do
      settle(state, op, :committed, postings, nil, new_balances)
    else
      settle(state, op, :rejected, postings, :insufficient_funds)
    end
  end

  # Finaliza a operação: histórico e saldos são gravados em uma única
  # chamada de inserção, evitando estados parcialmente aplicados.
  defp settle(state, op, status, postings, reason, balances \\ []) do
    op = %{op | status: status}

    entry = %{
      op_id: op.id,
      name: op.name,
      params: op.params,
      status: status,
      postings: postings,
      reason: reason,
      attempts: op.attempts,
      settled_at: now()
    }

    balance_objs = Enum.map(balances, fn {acct, value} -> {{:bal, acct}, value} end)
    :ok = persist([{{:op, op.id}, op}, {{:hist, op.id}, entry} | balance_objs])

    %{state | ops: Map.delete(state.ops, op.id)}
  end

  # Trata falha de tarefa: reexecuta com backoff até o limite de tentativas.
  defp crashed({:op, id}, reason, state) do
    op = Map.fetch!(state.ops, id)

    if op.attempts < @max_attempts do
      op = %{op | status: :pending}
      :ok = persist([{{:op, id}, op}])
      Process.send_after(self(), :dispatch, op.attempts * 100)
      %{state | ops: Map.put(state.ops, id, op)}
    else
      settle(state, op, :failed, [], {:crashed, reason})
    end
  end

  # Jobs periódicos já foram reagendados; uma falha apenas é ignorada.
  defp crashed({:periodic, _name, _ms}, _reason, state), do: state

  defp balance_of(account) do
    case :dets.lookup(@tab, {:bal, account}) do
      [{_, value}] -> value
      [] -> 0
    end
  end

  defp persist(objects) do
    :ok = :dets.insert(@tab, objects)
    :dets.sync(@tab)
  end

  defp now, do: System.system_time(:millisecond)
end

defmodule FinTx.Sup do
  # Supervisor raiz: reinicia o motor em caso de falha inesperada.
  use Supervisor

  def start_link(opts) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    children = [
      {Task.Supervisor, name: FinTx.TaskSup},
      {FinTx.Engine, opts}
    ]

    Supervisor.init(children, strategy: :one_for_one, max_restarts: 5, max_seconds: 10)
  end
end

defmodule FinTx do
  # Interface pública do sistema.

  def start_link(opts \\ []), do: FinTx.Sup.start_link(opts)
  def submit(name, params), do: FinTx.Engine.submit(name, params)
  def status, do: FinTx.Engine.status()
  def history(limit \\ 20), do: FinTx.Engine.history(limit)
  def balance(account), do: FinTx.Engine.balance(account)
end
