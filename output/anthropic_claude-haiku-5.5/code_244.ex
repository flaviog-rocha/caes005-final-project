# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de um sistema de cache, atendendo somente aos seguintes requisitos:
#
# 1. Manter um histórico das operações realizadas.
# 2. Persistir os dados para recuperação após reinicializações.
# 3. Permitir comunicação entre diferentes processos.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# Arquivo autocontido: sistema de cache com histórico, persistência e
# comunicação entre processos, usando apenas a biblioteca padrão do Elixir/OTP.
#
# Uso de exemplo (comentado):
#
#   {:ok, _} = Cache.Store.start_link(path: "meu_cache.bin", max_history: 500)
#   :ok = Cache.Store.put(Cache.Store, :usuario, %{nome: "Ana"})
#   %{nome: "Ana"} = Cache.Store.get(Cache.Store, :usuario)
#   Cache.Store.subscribe(Cache.Store)          # o processo atual recebe eventos
#   Cache.Store.history(Cache.Store, 10)        # últimas 10 operações
#   receive do
#     {:cache_event, entry} -> IO.inspect(entry)
#   end

defmodule Cache.Store do
  @moduledoc """
  Cache em memória implementado como GenServer.

  * Histórico: cada operação gera uma entrada `%{seq, at, op, key}`,
    mantida em ordem decrescente e limitada a `:max_history` entradas.
  * Persistência: o estado (dados, histórico e contador) é gravado em disco
    a cada operação, de forma atômica (escrita em arquivo temporário seguida
    de `rename`). Na inicialização, o arquivo é carregado se existir.
  * Comunicação: API síncrona via mensagens (`call`), e assinatura de
    processos interessados, que recebem `{:cache_event, entry}` a cada operação.
  """

  use GenServer

  @default_path "cache_store.bin"
  @default_max_history 1_000
  @format_version 1

  # ---------------------------------------------------------------------------
  # API pública
  # ---------------------------------------------------------------------------

  @doc """
  Inicia o servidor. Opções:
    * `:name` - nome de registro do processo (padrão: `#{inspect(__MODULE__)}`)
    * `:path` - caminho do arquivo de persistência
    * `:max_history` - tamanho máximo do histórico
  """
  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc "Armazena `value` sob `key`."
  def put(srv, key, value), do: GenServer.call(srv, {:put, key, value})

  @doc "Recupera o valor de `key`, ou `default` se não existir."
  def get(srv, key, default \\ nil), do: GenServer.call(srv, {:get, key, default})

  @doc "Remove `key`. Retorna `:ok` ou `:not_found`."
  def delete(srv, key), do: GenServer.call(srv, {:delete, key})

  @doc "Lista todas as chaves."
  def keys(srv), do: GenServer.call(srv, :keys)

  @doc "Remove todos os dados do cache."
  def clear(srv), do: GenServer.call(srv, :clear)

  @doc "Retorna o histórico (mais recente primeiro). `limit` é um inteiro ou `:all`."
  def history(srv, limit \\ :all), do: GenServer.call(srv, {:history, limit})

  @doc "Inscreve `pid` para receber `{:cache_event, entry}` a cada operação."
  def subscribe(srv, pid \\ self()), do: GenServer.call(srv, {:subscribe, pid})

  @doc "Cancela a inscrição de `pid`."
  def unsubscribe(srv, pid \\ self()), do: GenServer.call(srv, {:unsubscribe, pid})

  # ---------------------------------------------------------------------------
  # Callbacks do GenServer
  # ---------------------------------------------------------------------------

  @impl true
  def init(opts) do
    path = Keyword.get(opts, :path, @default_path)
    max_history = Keyword.get(opts, :max_history, @default_max_history)

    state = %{
      path: path,
      max_history: max_history,
      data: %{},
      history: [],
      seq: 0,
      subscribers: %{}
    }

    {:ok, load(state)}
  end

  @impl true
  def handle_call({:put, key, value}, _from, state) do
    state = %{state | data: Map.put(state.data, key, value)}
    {:reply, :ok, commit(state, :put, key)}
  end

  def handle_call({:get, key, default}, _from, state) do
    value = Map.get(state.data, key, default)
    {:reply, value, commit(state, :get, key)}
  end

  def handle_call({:delete, key}, _from, state) do
    reply = if Map.has_key?(state.data, key), do: :ok, else: :not_found
    state = %{state | data: Map.delete(state.data, key)}
    {:reply, reply, commit(state, :delete, key)}
  end

  def handle_call(:keys, _from, state) do
    {:reply, Map.keys(state.data), commit(state, :keys, nil)}
  end

  def handle_call(:clear, _from, state) do
    state = %{state | data: %{}}
    {:reply, :ok, commit(state, :clear, nil)}
  end

  def handle_call({:history, limit}, _from, state) do
    entries =
      case limit do
        :all -> state.history
        n when is_integer(n) and n >= 0 -> Enum.take(state.history, n)
      end

    {:reply, entries, state}
  end

  def handle_call({:subscribe, pid}, _from, state) do
    subs =
      case Map.fetch(state.subscribers, pid) do
        {:ok, _ref} -> state.subscribers
        :error -> Map.put(state.subscribers, pid, Process.monitor(pid))
      end

    {:reply, :ok, %{state | subscribers: subs}}
  end

  def handle_call({:unsubscribe, pid}, _from, state) do
    case Map.pop(state.subscribers, pid) do
      {nil, _} ->
        {:reply, :not_subscribed, state}

      {ref, subs} ->
        Process.demonitor(ref, [:flush])
        {:reply, :ok, %{state | subscribers: subs}}
    end
  end

  @impl true
  def handle_info({:DOWN, _ref, :process, pid, _reason}, state) do
    # Remove assinantes que terminaram
    {:noreply, %{state | subscribers: Map.delete(state.subscribers, pid)}}
  end

  # ---------------------------------------------------------------------------
  # Funções privadas
  # ---------------------------------------------------------------------------

  # Registra a operação no histórico, persiste o estado e notifica assinantes.
  # A persistência ocorre antes da resposta ao chamador, garantindo durabilidade.
  defp commit(state, op, key) do
    seq = state.seq + 1
    entry = %{seq: seq, at: System.system_time(:millisecond), op: op, key: key}
    history = Enum.take([entry | state.history], state.max_history)

    state = %{state | seq: seq, history: history}
    persist!(state)
    notify(state, entry)
    state
  end

  defp notify(state, entry) do
    Enum.each(state.subscribers, fn {pid, _ref} ->
      send(pid, {:cache_event, entry})
    end)
  end

  # Carrega o estado do disco, se o arquivo existir.
  # Um arquivo corrompido gera erro explícito para não perder dados silenciosamente.
  defp load(%{path: path} = state) do
    case File.read(path) do
      {:ok, bin} ->
        case safe_decode(bin) do
          {:ok, %{version: @format_version, data: data, history: history, seq: seq}} ->
            %{state | data: data, history: history, seq: seq}

          _ ->
            raise ArgumentError,
                  "arquivo de cache inválido ou de versão incompatível: #{path}"
        end

      {:error, :enoent} ->
        state

      {:error, reason} ->
        raise File.Error, reason: reason, action: "ler", path: path
    end
  end

  defp safe_decode(bin) do
    {:ok, :erlang.binary_to_term(bin, [:safe])}
  rescue
    ArgumentError -> :error
  end

  # Escrita atômica: grava em arquivo temporário, sincroniza no disco
  # e só então substitui o arquivo original.
  defp persist!(%{path: path} = state) do
    snapshot = %{
      version: @format_version,
      data: state.data,
      history: state.history,
      seq: state.seq
    }

    tmp = path <> ".tmp"
    bin = :erlang.term_to_binary(snapshot)

    {:ok, fd} = :file.open(String.to_charlist(tmp), [:write, :binary])
    :ok = :file.write(fd, bin)
    :ok = :file.sync(fd)
    :ok = :file.close(fd)
    :ok = File.rename(tmp, path)
    :ok
  end
end
