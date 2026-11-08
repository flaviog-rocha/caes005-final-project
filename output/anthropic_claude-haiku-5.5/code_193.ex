# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de um sistema de cache, atendendo somente aos seguintes requisitos:
#
# 1. Permitir limitar a quantidade de operações simultâneas.
# 2. Manter um histórico das operações realizadas.
# 3. Garantir a consistência dos dados durante operações concorrentes.
# 4. Recuperar processos após falhas inesperadas.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# Arquivo único: sistema de cache concorrente, tolerante a falhas e com histórico.
#
# Árvore de supervisão (rest_for_one):
#   CacheSystem (Supervisor)
#   ├── CacheSystem.Storage  -> dono das tabelas ETS (dados e histórico)
#   ├── CacheSystem.Limiter  -> semáforo que limita operações simultâneas
#   └── CacheSystem.Cache    -> serializa escritas, grava histórico e consistência
#
# Dependências: somente a biblioteca padrão (OTP/Elixir).

defmodule CacheSystem do
  @moduledoc """
  Ponto de entrada do sistema de cache.

  Opções de `start_link/1`:
    * `:max_concurrency` - máximo de operações simultâneas (padrão: schedulers online)
    * `:history_limit`   - quantidade de entradas mantidas no histórico (padrão: 1000)
  """

  use Supervisor

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    children = [
      # 1. Storage primeiro: as tabelas ETS precisam existir antes dos demais processos.
      {CacheSystem.Storage, []},

      # 2. Limiter: controla o número de operações concorrentes.
      {CacheSystem.Limiter,
       max_concurrency: Keyword.get(opts, :max_concurrency, System.schedulers_online())},

      # 3. Cache: executa as operações. Se ele falhar, é reiniciado e retoma
      #    dados e sequência do histórico a partir do ETS (que sobrevive).
      {CacheSystem.Cache, history_limit: Keyword.get(opts, :history_limit, 1000)}
    ]

    # rest_for_one: se o Limiter cair, o Cache (que depende dele) também é reiniciado.
    # Se o Cache cair, apenas ele é reiniciado. Se o Storage cair, tudo é reiniciado.
    Supervisor.init(children, strategy: :rest_for_one)
  end
end

defmodule CacheSystem.Storage do
  @moduledoc """
  Dono das tabelas ETS. Mantê-las em um processo separado do Cache permite que
  os dados sobrevivam a reinícios do processo que executa as operações.
  """

  use GenServer

  @data_table :cache_system_data
  @history_table :cache_system_history

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    # Dados: leitura concorrente habilitada; escrita feita apenas pelo CacheSystem.Cache,
    # que é processo único (serialização das escritas garante consistência).
    :ets.new(@data_table, [:set, :public, :named_table, read_concurrency: true])

    # Histórico: ordered_set indexado por sequência monotônica.
    :ets.new(@history_table, [:ordered_set, :public, :named_table, read_concurrency: true])

    {:ok, %{}}
  end
end

defmodule CacheSystem.Limiter do
  @moduledoc """
  Semáforo com fila de espera.

    * Limita quantas operações podem executar ao mesmo tempo.
    * Monitora o processo que detém cada permissão: se ele morrer, a permissão
      é liberada automaticamente (evita vazamento de slots após falhas).
    * Expira pedidos que aguardam tempo demais, retornando `{:error, :timeout}`.
  """

  use GenServer

  @doc """
  Executa `fun` dentro de uma permissão do semáforo.
  Retorna `{:ok, resultado}` ou `{:error, motivo}`.
  """
  def run(fun, wait_timeout \\ 5_000) when is_function(fun, 0) do
    case acquire(wait_timeout) do
      {:ok, permit} ->
        try do
          {:ok, fun.()}
        after
          release(permit)
        end

      error ->
        error
    end
  end

  defp acquire(wait_timeout) do
    GenServer.call(__MODULE__, {:acquire, wait_timeout}, :infinity)
  catch
    :exit, _ -> {:error, :unavailable}
  end

  defp release(permit) do
    GenServer.cast(__MODULE__, {:release, permit})
  end

  def start_link(opts) do
    GenServer.start_link(__MODULE__, Keyword.fetch!(opts, :max_concurrency), name: __MODULE__)
  end

  @impl true
  def init(max) when is_integer(max) and max > 0 do
    # active: %{permit_ref => pid}; waiting: fila de {token, from, pid}
    {:ok, %{max: max, active: %{}, waiting: :queue.new()}}
  end

  @impl true
  def handle_call({:acquire, wait_timeout}, {pid, _} = from, state) do
    token = make_ref()

    if is_integer(wait_timeout) do
      Process.send_after(self(), {:expire, token}, wait_timeout)
    end

    waiting = :queue.in({token, from, pid}, state.waiting)
    {:noreply, dispatch(%{state | waiting: waiting})}
  end

  @impl true
  def handle_cast({:release, permit}, state) do
    # Remove o monitor para não receber um DOWN desnecessário depois.
    Process.demonitor(permit, [:flush])
    {:noreply, dispatch(%{state | active: Map.delete(state.active, permit)})}
  end

  @impl true
  def handle_info({:expire, token}, state) do
    {expired, kept} =
      state.waiting
      |> :queue.to_list()
      |> Enum.split_with(fn {t, _, _} -> t == token end)

    case expired do
      # Pedido ainda na fila: responde timeout e remove da fila.
      [{_, from, _}] -> GenServer.reply(from, {:error, :timeout})
      # Pedido já foi atendido: nada a fazer.
      [] -> :ok
    end

    {:noreply, %{state | waiting: :queue.from_list(kept)}}
  end

  # Processo detentor de permissão morreu: libera o slot.
  def handle_info({:DOWN, permit, :process, _pid, _reason}, state) do
    {:noreply, dispatch(%{state | active: Map.delete(state.active, permit)})}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  # Enquanto houver slots livres e pedidos na fila, concede permissões.
  defp dispatch(%{active: active, max: max} = state) when map_size(active) >= max, do: state

  defp dispatch(state) do
    case :queue.out(state.waiting) do
      {:empty, _} ->
        state

      {{:value, {_token, from, pid}}, rest} ->
        # Monitor sobre o processo solicitante. Se ele já estiver morto,
        # o DOWN chega imediatamente e o slot é liberado.
        permit = Process.monitor(pid)
        GenServer.reply(from, {:ok, permit})

        dispatch(%{state | waiting: rest, active: Map.put(state.active, permit, pid)})
    end
  end
end

defmodule CacheSystem.Cache do
  @moduledoc """
  Operações de cache.

  Todas as operações são serializadas por este processo, o que garante:
    * Atomicidade: cada operação (incluindo gravação no histórico) completa integralmente.
    * Consistência: versões são incrementadas sem condições de corrida.
    * Compare-and-swap (`cas/3`) para atualizações condicionais.

  Entradas de dados: `{key, value, version}`.
  Entradas de histórico: `{seq, timestamp_us, op, key, status}`.
  """

  use GenServer

  @data_table :cache_system_data
  @history_table :cache_system_history

  # ---------------------------------------------------------------------------
  # API pública (cada chamada passa pelo Limiter)
  # ---------------------------------------------------------------------------

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc "Busca uma chave. Retorna `{:ok, value, version}` ou `{:error, :not_found}`."
  def get(key), do: request({:get, key})

  @doc "Grava um valor. Retorna `{:ok, new_version}`."
  def put(key, value), do: request({:put, key, value})

  @doc "Remove uma chave. Retorna `:ok` ou `{:error, :not_found}`."
  def delete(key), do: request({:delete, key})

  @doc """
  Atualiza somente se a versão atual for igual a `expected_version`.
  Chaves inexistentes têm versão 0.
  Retorna `{:ok, new_version}` ou `{:error, {:conflict, current_version}}`.
  """
  def cas(key, expected_version, value), do: request({:cas, key, expected_version, value})

  @doc "Retorna as últimas `n` entradas do histórico, da mais antiga para a mais recente."
  def history(n \\ 50), do: request({:history, n})

  defp request(msg) do
    case CacheSystem.Limiter.run(fn -> call_server(msg) end) do
      {:ok, result} -> result
      {:error, reason} -> {:error, reason}
    end
  end

  # Captura falhas do processo (ex.: reinício em andamento) sem derrubar o chamador.
  defp call_server(msg) do
    GenServer.call(__MODULE__, msg)
  catch
    :exit, _ -> {:error, :unavailable}
  end

  # ---------------------------------------------------------------------------
  # Callbacks do GenServer
  # ---------------------------------------------------------------------------

  @impl true
  def init(opts) do
    # Recuperação: retoma a sequência a partir do último registro existente no ETS.
    seq =
      case :ets.last(@history_table) do
        :"$end_of_table" -> 0
        last -> last
      end

    {:ok, %{seq: seq, history_limit: Keyword.get(opts, :history_limit, 1000)}}
  end

  @impl true
  def handle_call({:get, key}, _from, state) do
    case :ets.lookup(@data_table, key) do
      [{^key, value, version}] ->
        {:reply, {:ok, value, version}, record(state, :get, key, :hit)}

      [] ->
        {:reply, {:error, :not_found}, record(state, :get, key, :miss)}
    end
  end

  def handle_call({:put, key, value}, _from, state) do
    new_version = current_version(key) + 1
    :ets.insert(@data_table, {key, value, new_version})
    {:reply, {:ok, new_version}, record(state, :put, key, :ok)}
  end

  def handle_call({:delete, key}, _from, state) do
    existed? = :ets.member(@data_table, key)
    :ets.delete(@data_table, key)

    reply = if existed?, do: :ok, else: {:error, :not_found}
    status = if existed?, do: :ok, else: :not_found

    {:reply, reply, record(state, :delete, key, status)}
  end

  def handle_call({:cas, key, expected_version, value}, _from, state) do
    current = current_version(key)

    if current == expected_version do
      new_version = current + 1
      :ets.insert(@data_table, {key, value, new_version})
      {:reply, {:ok, new_version}, record(state, :cas, key, :ok)}
    else
      {:reply, {:error, {:conflict, current}}, record(state, :cas, key, :conflict)}
    end
  end

  def handle_call({:history, n}, _from, state) do
    entries =
      @history_table
      |> :ets.tab2list()
      |> Enum.take(-n)

    {:reply, entries, state}
  end

  # ---------------------------------------------------------------------------
  # Auxiliares
  # ---------------------------------------------------------------------------

  defp current_version(key) do
    case :ets.lookup(@data_table, key) do
      [{_, _, version}] -> version
      [] -> 0
    end
  end

  # Registra a operação no histórico e remove a entrada mais antiga,
  # mantendo o histórico limitado a `history_limit` registros.
  defp record(state, op, key, status) do
    seq = state.seq + 1
    :ets.insert(@history_table, {seq, System.system_time(:microsecond), op, key, status})

    old = seq - state.history_limit
    if old > 0, do: :ets.delete(@history_table, old)

    %{state | seq: seq}
  end
end
