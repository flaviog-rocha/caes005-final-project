# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de um sistema de cache, atendendo somente aos seguintes requisitos:
#
# 1. Manter um histórico das operações realizadas.
# 2. Persistir os dados para recuperação após reinicializações.
# 3. Permitir comunicação entre diferentes processos.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# Arquivo único: cache_system.ex
#
# Sistema de cache com:
#   1. Histórico de operações (log em memória, limitado por :max_history).
#   2. Persistência em disco (snapshot binário, gravado de forma atômica).
#   3. Comunicação entre processos (API via GenServer.call de qualquer processo
#      e assinatura de eventos com envio de mensagens {:cache_event, evento}).
#
# Uso (exemplo, em iex):
#
#   {:ok, _} = CacheSystem.start_link(name: :meu_cache, path: "meu_cache.state")
#   :ok = CacheSystem.subscribe(:meu_cache)
#   :ok = CacheSystem.put(:meu_cache, :usuario, %{nome: "Ana"}, 60_000)
#   {:ok, %{nome: "Ana"}} = CacheSystem.get(:meu_cache, :usuario)
#   CacheSystem.history(:meu_cache, 10)
#   # Após reiniciar o processo com o mesmo :path, os dados são recarregados.

defmodule CacheSystem do
  @moduledoc """
  Cache em memória com histórico de operações, persistência em disco e
  comunicação entre processos.
  """

  use GenServer

  @default_path "cache_system.state"
  @default_max_history 1_000
  @default_sweep_interval 5_000

  ## ---------------------------------------------------------------------------
  ## API pública (pode ser chamada a partir de qualquer processo)
  ## ---------------------------------------------------------------------------

  @doc "Inicia o cache. Opções: :name, :path, :max_history, :sweep_interval."
  def start_link(opts \\ []) do
    {name, opts} = Keyword.pop(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc "Armazena um valor. `ttl_ms` é opcional (nil = sem expiração)."
  def put(server, key, value, ttl_ms \\ nil) do
    GenServer.call(server, {:put, key, value, ttl_ms})
  end

  @doc "Recupera um valor. Retorna {:ok, valor} ou {:error, :not_found}."
  def get(server, key), do: GenServer.call(server, {:get, key})

  @doc "Remove uma chave."
  def delete(server, key), do: GenServer.call(server, {:delete, key})

  @doc "Remove todas as chaves."
  def clear(server), do: GenServer.call(server, :clear)

  @doc "Lista as chaves válidas (não expiradas)."
  def keys(server), do: GenServer.call(server, :keys)

  @doc "Retorna o histórico de operações, do mais recente para o mais antigo."
  def history(server, limit \\ :all), do: GenServer.call(server, {:history, limit})

  @doc "Inscreve o processo chamador para receber {:cache_event, evento}."
  def subscribe(server), do: GenServer.call(server, {:subscribe, self()})

  @doc "Cancela a inscrição do processo chamador."
  def unsubscribe(server), do: GenServer.call(server, {:unsubscribe, self()})

  ## ---------------------------------------------------------------------------
  ## Callbacks do GenServer
  ## ---------------------------------------------------------------------------

  @impl true
  def init(opts) do
    path = Keyword.get(opts, :path, @default_path)

    # Estado = dados recuperados do disco + configuração + controle de assinantes.
    state =
      path
      |> load_state()
      |> Map.merge(%{
        path: path,
        max_history: Keyword.get(opts, :max_history, @default_max_history),
        sweep_interval: Keyword.get(opts, :sweep_interval, @default_sweep_interval),
        subscribers: %{}
      })

    schedule_sweep(state.sweep_interval)
    {:ok, state}
  end

  @impl true
  def handle_call({:put, key, value, ttl_ms}, _from, state) do
    now = now_ms()
    expires_at = if ttl_ms, do: now + ttl_ms, else: nil
    entry = %{value: value, expires_at: expires_at, updated_at: now}

    state = %{state | data: Map.put(state.data, key, entry)}
    {event, state} = log(state, :put, key, now)
    commit(state, [event], :ok)
  end

  def handle_call({:get, key}, _from, state) do
    now = now_ms()

    case Map.fetch(state.data, key) do
      :error ->
        {event, state} = log(state, :get_miss, key, now)
        commit(state, [event], {:error, :not_found})

      {:ok, entry} ->
        if expired?(entry, now) do
          # Expiração preguiçosa: remove a entrada vencida ao ser acessada.
          state = %{state | data: Map.delete(state.data, key)}
          {event, state} = log(state, :expire, key, now)
          commit(state, [event], {:error, :not_found})
        else
          {event, state} = log(state, :get_hit, key, now)
          commit(state, [event], {:ok, entry.value})
        end
    end
  end

  def handle_call({:delete, key}, _from, state) do
    now = now_ms()
    state = %{state | data: Map.delete(state.data, key)}
    {event, state} = log(state, :delete, key, now)
    commit(state, [event], :ok)
  end

  def handle_call(:clear, _from, state) do
    now = now_ms()
    state = %{state | data: %{}}
    {event, state} = log(state, :clear, nil, now)
    commit(state, [event], :ok)
  end

  def handle_call(:keys, _from, state) do
    now = now_ms()

    keys =
      for {key, entry} <- state.data, not expired?(entry, now) do
        key
      end

    {:reply, keys, state}
  end

  def handle_call({:history, limit}, _from, state) do
    events =
      if limit == :all,
        do: state.history,
        else: Enum.take(state.history, limit)

    {:reply, events, state}
  end

  def handle_call({:subscribe, pid}, _from, state) do
    if Map.has_key?(state.subscribers, pid) do
      {:reply, :ok, state}
    else
      # O monitor permite remover o assinante automaticamente quando o processo morre.
      ref = Process.monitor(pid)
      {:reply, :ok, %{state | subscribers: Map.put(state.subscribers, pid, ref)}}
    end
  end

  def handle_call({:unsubscribe, pid}, _from, state) do
    case Map.pop(state.subscribers, pid) do
      {nil, _subs} ->
        {:reply, :ok, state}

      {ref, subs} ->
        Process.demonitor(ref, [:flush])
        {:reply, :ok, %{state | subscribers: subs}}
    end
  end

  @impl true
  def handle_info(:sweep, state) do
    now = now_ms()

    expired_keys =
      for {key, entry} <- state.data, expired?(entry, now) do
        key
      end

    state = %{state | data: Map.drop(state.data, expired_keys)}

    {events, state} =
      Enum.map_reduce(expired_keys, state, fn key, acc ->
        log(acc, :expire, key, now)
      end)

    schedule_sweep(state.sweep_interval)
    {:noreply, publish(state, events)}
  end

  def handle_info({:DOWN, _ref, :process, pid, _reason}, state) do
    {:noreply, %{state | subscribers: Map.delete(state.subscribers, pid)}}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  ## ---------------------------------------------------------------------------
  ## Funções auxiliares privadas
  ## ---------------------------------------------------------------------------

  # Registra um evento no histórico, incrementando a sequência.
  # O histórico é mantido do mais recente para o mais antigo e limitado.
  defp log(state, op, key, now) do
    seq = state.seq + 1
    event = %{seq: seq, op: op, key: key, at: now}
    history = Enum.take([event | state.history], state.max_history)
    {event, %{state | seq: seq, history: history}}
  end

  # Persiste (quando há eventos), notifica assinantes e devolve a resposta ao chamador.
  defp commit(state, events, reply) do
    {:reply, reply, publish(state, events)}
  end

  defp publish(state, []), do: state

  defp publish(state, events) do
    persist(state)

    subscribers = Map.keys(state.subscribers)

    Enum.each(events, fn event ->
      Enum.each(subscribers, &send(&1, {:cache_event, event}))
    end)

    state
  end

  defp expired?(%{expires_at: nil}, _now), do: false
  defp expired?(%{expires_at: expires_at}, now), do: now >= expires_at

  defp schedule_sweep(interval), do: Process.send_after(self(), :sweep, interval)

  # Tempo de parede, pois precisa ser válido entre reinicializações.
  defp now_ms, do: System.system_time(:millisecond)

  # Gravação atômica: escreve em arquivo temporário e renomeia.
  # Se o processo cair durante a escrita, o snapshot anterior permanece íntegro.
  defp persist(state) do
    snapshot = :erlang.term_to_binary(Map.take(state, [:data, :history, :seq]))
    tmp_path = state.path <> ".tmp"

    File.write!(tmp_path, snapshot)
    File.rename!(tmp_path, state.path)
  end

  # Carrega o snapshot do disco. Se não existir ou estiver corrompido, inicia vazio.
  defp load_state(path) do
    with {:ok, bin} <- File.read(path),
         {:ok, %{data: _, history: _, seq: _} = saved} <- safe_decode(bin) do
      saved
    else
      _ -> %{data: %{}, history: [], seq: 0}
    end
  end

  # :safe impede a criação de átomos novos a partir de dados não confiáveis.
  defp safe_decode(bin) do
    {:ok, :erlang.binary_to_term(bin, [:safe])}
  rescue
    _ -> :error
  end
end
