# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de um sistema de cache, atendendo somente aos seguintes requisitos:
#
# 1. Permitir limitar a quantidade de operações simultâneas.
# 2. Manter um histórico das operações realizadas.
# 3. Garantir a consistência dos dados durante operações concorrentes.
# 4. Recuperar processos após falhas inesperadas.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# Arquivo único: sistema de cache concorrente e tolerante a falhas (somente biblioteca padrão).
#
# Requisitos:
#   1. Limite de operações simultâneas: CacheSystem.Limiter é um semáforo com N permissões.
#      Chamadas excedentes entram numa fila e expiram por timeout.
#   2. Histórico: cada operação é registrada na tabela ETS ordenada :cache_system_history
#      (com limite de tamanho).
#   3. Consistência: os dados ficam em ETS :public, mas todas as escritas passam pelo
#      CacheSystem.Server (escritor único, serializado). Atualizações read-modify-write
#      são atômicas (update/3). Leituras são diretas na ETS, sem gargalo.
#   4. Recuperação: a supervisão usa :rest_for_one. O CacheSystem.Keeper é dono das tabelas
#      ETS e sobrevive às falhas do Limiter e do Server. Além disso, o estado é salvo em
#      disco periodicamente e no encerramento, e restaurado na inicialização.
#
# Uso (exemplo em comentário):
#   {:ok, _pid} = CacheSystem.start_link(max_concurrency: 5)
#   :ok = CacheSystem.put(:contador, 0)
#   {:ok, 1} = CacheSystem.update(:contador, 0, &(&1 + 1))
#   {:ok, 0} = CacheSystem.get(:contador)
#   CacheSystem.history(10)

defmodule CacheSystem do
  @moduledoc """
  API pública do sistema de cache.

  Retornos:
    * get/1    -> {:ok, valor} | :not_found | {:error, motivo}
    * put/2    -> :ok | {:error, motivo}
    * delete/1 -> :ok | {:error, motivo}
    * update/3 -> {:ok, novo_valor} | {:error, motivo}

  {:error, :timeout} indica que não foi possível obter permissão de concorrência a tempo.
  """

  use Supervisor

  alias CacheSystem.{Keeper, Limiter, Server, History}

  # Tempo máximo de espera na fila do limitador de concorrência (ms).
  @acquire_timeout 5_000

  # ---------------------------------------------------------------------------
  # Supervisão
  # ---------------------------------------------------------------------------

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    children = [
      # 1º: dono das tabelas ETS e da persistência em disco.
      {Keeper, opts},
      # 2º: semáforo de concorrência.
      {Limiter, opts},
      # 3º: escritor único (serializa as escritas).
      {Server, []}
    ]

    # :rest_for_one: se um processo cai, ele e os que dependem dele (à direita) são reiniciados.
    # Como os dados estão no Keeper, reiniciar o Server ou o Limiter não perde dados.
    Supervisor.init(children, strategy: :rest_for_one)
  end

  # ---------------------------------------------------------------------------
  # Operações públicas
  # ---------------------------------------------------------------------------

  def get(key) do
    run(fn ->
      result =
        case :ets.lookup(Keeper.data_table(), key) do
          [{^key, value}] -> {:ok, value}
          [] -> :not_found
        end

      History.record(:get, key, if(match?({:ok, _}, result), do: :hit, else: :miss))
      result
    end)
  end

  def put(key, value) do
    run(fn -> GenServer.call(Server, {:put, key, value}) end)
  end

  def delete(key) do
    run(fn -> GenServer.call(Server, {:delete, key}) end)
  end

  @doc """
  Atualização atômica: lê o valor atual (ou `default`), aplica `fun` e grava o resultado.
  Executada dentro do escritor único, portanto sem condições de corrida.
  """
  def update(key, default, fun) when is_function(fun, 1) do
    run(fn -> GenServer.call(Server, {:update, key, default, fun}) end)
  end

  @doc "Retorna as últimas `limit` operações registradas, em ordem cronológica."
  def history(limit \\ 100), do: History.list(limit)

  @doc "Força a gravação de um snapshot em disco."
  def snapshot, do: Keeper.snapshot()

  # Executa a operação sob o limite de concorrência e captura falhas,
  # convertendo-as em {:error, motivo} em vez de derrubar o chamador.
  defp run(fun) do
    Limiter.run(
      fn ->
        try do
          fun.()
        catch
          kind, reason -> {:error, {kind, reason}}
        end
      end,
      @acquire_timeout
    )
  end
end

defmodule CacheSystem.Keeper do
  @moduledoc false
  # Dono das tabelas ETS. Se Limiter ou Server falham, as tabelas permanecem.
  # Se o próprio Keeper falha, as tabelas são recriadas e restauradas do snapshot em disco.

  use GenServer

  @data :cache_system_data
  @history :cache_system_history

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def data_table, do: @data
  def history_table, do: @history

  def snapshot, do: GenServer.call(__MODULE__, :snapshot)

  @impl true
  def init(opts) do
    # Trap de exits garante que terminate/2 seja chamado no encerramento pela supervisão.
    Process.flag(:trap_exit, true)

    path = Keyword.get(opts, :snapshot_path, "cache_system.snapshot")
    interval = Keyword.get(opts, :snapshot_interval, 30_000)

    :ets.new(@data, [:set, :public, :named_table, read_concurrency: true])
    :ets.new(@history, [:ordered_set, :public, :named_table, write_concurrency: true])

    restore(path)
    schedule(interval)

    {:ok, %{path: path, interval: interval}}
  end

  @impl true
  def handle_call(:snapshot, _from, state) do
    {:reply, save(state.path), state}
  end

  @impl true
  def handle_info(:periodic, state) do
    save(state.path)
    schedule(state.interval)
    {:noreply, state}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, state) do
    save(state.path)
    :ok
  end

  defp schedule(interval) when is_integer(interval) and interval > 0 do
    Process.send_after(self(), :periodic, interval)
  end

  defp schedule(_), do: :ok

  # Escrita atômica: grava em arquivo temporário e renomeia.
  defp save(path) do
    tmp = path <> ".tmp"

    bin =
      :erlang.term_to_binary(%{
        data: :ets.tab2list(@data),
        history: :ets.tab2list(@history)
      })

    with :ok <- File.write(tmp, bin), do: File.rename(tmp, path)
  end

  # Restaura o snapshot, se existir. Arquivo ausente ou corrompido é ignorado.
  defp restore(path) do
    with {:ok, bin} <- File.read(path),
         %{data: data, history: history} <- :erlang.binary_to_term(bin) do
      :ets.insert(@data, data)
      :ets.insert(@history, history)
    end

    :ok
  rescue
    _ -> :ok
  end
end

defmodule CacheSystem.Limiter do
  @moduledoc false
  # Semáforo com N permissões. Cada permissão é identificada pela referência de monitor
  # do processo que a adquiriu. Se o processo morre, a permissão é liberada automaticamente.

  use GenServer

  def start_link(opts) do
    GenServer.start_link(__MODULE__, Keyword.get(opts, :max_concurrency, 10), name: __MODULE__)
  end

  @doc false
  def run(fun, timeout) do
    case acquire(timeout) do
      {:ok, token} ->
        try do
          fun.()
        after
          release(token)
        end

      {:error, _} = error ->
        error
    end
  end

  defp acquire(timeout) do
    deadline = now() + timeout
    GenServer.call(__MODULE__, {:acquire, deadline}, timeout)
  catch
    :exit, {:timeout, _} -> {:error, :timeout}
    :exit, _ -> {:error, :unavailable}
  end

  defp release(token), do: GenServer.cast(__MODULE__, {:release, token})

  defp now, do: System.monotonic_time(:millisecond)

  @impl true
  def init(max) do
    {:ok, %{max: max, active: %{}, waiting: :queue.new()}}
  end

  @impl true
  def handle_call({:acquire, deadline}, {pid, _} = from, state) do
    if free?(state) and :queue.is_empty(state.waiting) do
      ref = Process.monitor(pid)
      {:reply, {:ok, ref}, %{state | active: Map.put(state.active, ref, pid)}}
    else
      # Entradas com prazo expirado são descartadas em dispatch/1.
      {:noreply, %{state | waiting: :queue.in({from, deadline, pid}, state.waiting)}}
    end
  end

  @impl true
  def handle_cast({:release, ref}, state) do
    Process.demonitor(ref, [:flush])
    {:noreply, state |> drop(ref) |> dispatch()}
  end

  @impl true
  def handle_info({:DOWN, ref, :process, _pid, _reason}, state) do
    # Chamador morreu durante a operação: libera a permissão.
    {:noreply, state |> drop(ref) |> dispatch()}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  defp free?(state), do: map_size(state.active) < state.max

  defp drop(state, ref), do: %{state | active: Map.delete(state.active, ref)}

  # Concede permissões a aguardantes válidos (prazo não expirado e processo vivo).
  defp dispatch(state) do
    if free?(state) do
      case :queue.out(state.waiting) do
        {:empty, _} ->
          state

        {{:value, {from, deadline, pid}}, rest} ->
          state = %{state | waiting: rest}

          if deadline > now() and Process.alive?(pid) do
            ref = Process.monitor(pid)
            GenServer.reply(from, {:ok, ref})
            dispatch(%{state | active: Map.put(state.active, ref, pid)})
          else
            dispatch(state)
          end
      end
    else
      state
    end
  end
end

defmodule CacheSystem.Server do
  @moduledoc false
  # Escritor único: todas as mutações são serializadas aqui, garantindo consistência.
  # Como as tabelas pertencem ao Keeper, uma falha aqui não perde dados.

  use GenServer

  alias CacheSystem.{Keeper, History}

  def start_link(_opts \\ []) do
    GenServer.start_link(__MODULE__, nil, name: __MODULE__)
  end

  @impl true
  def init(_), do: {:ok, nil}

  @impl true
  def handle_call({:put, key, value}, _from, state) do
    :ets.insert(Keeper.data_table(), {key, value})
    History.record(:put, key, :ok)
    {:reply, :ok, state}
  end

  def handle_call({:delete, key}, _from, state) do
    :ets.delete(Keeper.data_table(), key)
    History.record(:delete, key, :ok)
    {:reply, :ok, state}
  end

  def handle_call({:update, key, default, fun}, _from, state) do
    table = Keeper.data_table()

    current =
      case :ets.lookup(table, key) do
        [{^key, value}] -> value
        [] -> default
      end

    # Se fun/1 falhar, o processo cai e é reiniciado; os dados anteriores permanecem intactos.
    new_value = fun.(current)
    :ets.insert(table, {key, new_value})
    History.record(:update, key, :ok)

    {:reply, {:ok, new_value}, state}
  end
end

defmodule CacheSystem.History do
  @moduledoc false
  # Histórico de operações em ETS ordenada. A chave é um inteiro monotônico único,
  # o que dá uma ordem global consistente mesmo com chamadas concorrentes.

  alias CacheSystem.Keeper

  @max_entries 1_000

  def record(op, key, result) do
    table = Keeper.history_table()
    seq = System.unique_integer([:monotonic, :positive])

    :ets.insert(table, {seq, System.os_time(:millisecond), self(), op, key, result})
    trim(table)
    :ok
  end

  def list(limit) do
    Keeper.history_table()
    |> :ets.tab2list()
    |> Enum.take(-limit)
    |> Enum.map(fn {seq, at, pid, op, key, result} ->
      %{seq: seq, at: at, pid: pid, op: op, key: key, result: result}
    end)
  end

  # Remove a entrada mais antiga quando o limite é excedido.
  defp trim(table) do
    if :ets.info(table, :size) > @max_entries do
      case :ets.first(table) do
        :"$end_of_table" -> :ok
        oldest -> :ets.delete(table, oldest)
      end
    end

    :ok
  end
end
