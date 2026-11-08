# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de troca de mensagens entre usuários, atendendo somente aos seguintes requisitos:
#
# 1. Persistir os dados para recuperação após reinicializações.
# 2. Implementar novas tentativas após falhas temporárias.
# 3. Processar grandes volumes de dados sem carregá-los integralmente na memória.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# mensageria.exs
#
# Uso (exemplo):
#   {:ok, _} = Mensageria.Store.start_link(
#     path: "dados/mensagens.log",
#     deliver: fn msg -> IO.inspect(msg); :ok end
#   )
#   {:ok, _id} = Mensageria.Store.send_message("ana", "bruno", "Olá!")
#   Mensageria.Store.inbox("bruno", 10)
#   Mensageria.Store.stats()
#
# Execução: elixir mensageria.exs

defmodule Mensageria.Log do
  @moduledoc false
  # Log de escrita antecipada (write-ahead log), somente de anexação.
  # Cada registro é: <<tamanho::32, termo_binario::binary-size(tamanho)>>.
  # Um registro truncado no fim (queda durante escrita) é ignorado na leitura,
  # preservando a consistência do que já foi confirmado (fsync).

  @header 4

  # Abre (ou cria) o arquivo para anexação e leitura aleatória.
  def open(path) do
    File.mkdir_p!(Path.dirname(path))
    File.touch!(path)
    {:ok, fd} = :file.open(String.to_charlist(path), [:raw, :binary, :read, :append])
    fd
  end

  # Anexa um termo e força gravação em disco antes de retornar.
  # Retorna a localização {posicao, tamanho} do registro para leitura posterior.
  def append(fd, term) do
    bin = :erlang.term_to_binary(term)
    size = byte_size(bin)
    {:ok, pos} = :file.position(fd, :eof)

    with :ok <- :file.write(fd, <<size::32, bin::binary>>),
         :ok <- :file.datasync(fd) do
      {:ok, {pos, size}}
    end
  end

  # Lê um único registro pelo offset, sem carregar o arquivo inteiro.
  def read_at(fd, {pos, size}) do
    {:ok, bin} = :file.pread(fd, pos + @header, size)
    :erlang.binary_to_term(bin)
  end

  # Stream preguiçosa que percorre o log registro a registro.
  # Usa memória proporcional a um único registro, independentemente do tamanho do log.
  def stream(path) do
    Stream.resource(
      fn ->
        {:ok, fd} = :file.open(String.to_charlist(path), [:raw, :binary, :read])
        {fd, 0}
      end,
      &next_record/1,
      fn {fd, _pos} -> :file.close(fd) end
    )
  end

  defp next_record({fd, pos} = state) do
    with {:ok, <<size::32>>} <- :file.read(fd, @header),
         {:ok, bin} when byte_size(bin) == size <- :file.read(fd, size) do
      {[{pos, size, :erlang.binary_to_term(bin)}], {fd, pos + @header + size}}
    else
      _ -> {:halt, state}
    end
  end
end

defmodule Mensageria.Store do
  @moduledoc """
  Armazena mensagens entre usuários com:

    * Persistência: cada transição de estado é gravada no log antes de ser
      aplicada na memória. Ao reiniciar, o índice é reconstruído lendo o log.
    * Novas tentativas: falhas temporárias são reagendadas com backoff
      exponencial e jitter, até `max_attempts`. Falhas permanentes vão para
      o estado `:dead`.
    * Volume grande: a memória guarda apenas metadados (localização no log,
      status, tentativas). Os corpos das mensagens são lidos do disco sob
      demanda, em lotes limitados (`batch_size`).

  Garantia de entrega: pelo menos uma vez. Se o processo cair após a entrega
  mas antes de gravar o resultado, a mensagem poderá ser entregue novamente.
  """

  use GenServer
  require Logger

  alias Mensageria.Log

  @defaults %{
    max_attempts: 5,
    base_backoff_ms: 500,
    max_backoff_ms: 60_000,
    batch_size: 100,
    tick_ms: 1_000
  }

  # ---------------------------------------------------------------------------
  # API pública
  # ---------------------------------------------------------------------------

  def start_link(opts) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc "Enfileira uma mensagem de `from` para `to`. Retorna `{:ok, id}` após persistir."
  def send_message(server \\ __MODULE__, from, to, body) do
    GenServer.call(server, {:send, from, to, body})
  end

  @doc "Lista até `limit` mensagens já entregues para `user`, da mais antiga para a mais recente."
  def inbox(server \\ __MODULE__, user, limit \\ 50) do
    GenServer.call(server, {:inbox, user, limit})
  end

  @doc "Retorna contagem de mensagens por status (:pending, :done, :dead)."
  def stats(server \\ __MODULE__) do
    GenServer.call(server, :stats)
  end

  # ---------------------------------------------------------------------------
  # GenServer
  # ---------------------------------------------------------------------------

  @impl true
  def init(opts) do
    opts = Map.new(opts)
    path = Map.fetch!(opts, :path)
    _ = Map.fetch!(opts, :deliver)

    cfg = Map.merge(@defaults, opts)
    fd = Log.open(path)
    {index, max_id} = recover(path)

    state = %{fd: fd, path: path, cfg: cfg, index: index, next_id: max_id + 1}
    schedule_tick(state)
    {:ok, state}
  end

  @impl true
  def handle_call({:send, from, to, body}, _caller, st) do
    id = st.next_id

    case Log.append(st.fd, {:enqueued, id, from, to, body}) do
      {:ok, loc} ->
        meta = %{loc: loc, to: to, status: :pending, attempts: 0, next_at: 0}
        st = %{st | next_id: id + 1, index: Map.put(st.index, id, meta)}
        {:reply, {:ok, id}, st}

      {:error, reason} ->
        {:reply, {:error, reason}, st}
    end
  end

  def handle_call({:inbox, user, limit}, _caller, st) do
    msgs =
      st.index
      |> Enum.filter(fn {_id, m} -> m.to == user and m.status == :done end)
      |> Enum.map(fn {id, _m} -> id end)
      |> Enum.sort()
      |> Enum.take(limit)
      |> Enum.map(fn id ->
        {:enqueued, ^id, from, _to, body} = Log.read_at(st.fd, st.index[id].loc)
        %{id: id, from: from, body: body}
      end)

    {:reply, msgs, st}
  end

  def handle_call(:stats, _caller, st) do
    {:reply, Enum.frequencies_by(st.index, fn {_id, m} -> m.status end), st}
  end

  @impl true
  def handle_info(:tick, st) do
    now = now_ms()

    # Seleção preguiçosa: percorre o índice e para ao atingir o tamanho do lote.
    due =
      st.index
      |> Stream.filter(fn {_id, m} -> m.status == :pending and m.next_at <= now end)
      |> Stream.take(st.cfg.batch_size)
      |> Enum.map(fn {id, _m} -> id end)

    st = Enum.reduce(due, st, &deliver_one(&1, &2, now))
    schedule_tick(st)
    {:noreply, st}
  end

  # ---------------------------------------------------------------------------
  # Entrega e transições de estado
  # ---------------------------------------------------------------------------

  defp deliver_one(id, st, now) do
    m = Map.fetch!(st.index, id)
    {:enqueued, ^id, from, _to, body} = Log.read_at(st.fd, m.loc)

    msg = %{id: id, from: from, to: m.to, body: body, attempt: m.attempts + 1}

    result =
      try do
        st.cfg.deliver.(msg)
      catch
        _kind, _reason -> {:error, :temporary}
      end

    case result do
      :ok ->
        commit(st, id, {:done, id}, &%{&1 | status: :done})

      {:error, :permanent} ->
        commit(st, id, {:dead, id}, &%{&1 | status: :dead})

      _temporary ->
        attempts = m.attempts + 1

        if attempts >= st.cfg.max_attempts do
          commit(st, id, {:dead, id}, &%{&1 | status: :dead, attempts: attempts})
        else
          next_at = now + backoff(attempts, st.cfg)
          commit(st, id, {:attempt, id, attempts, next_at},
            &%{&1 | attempts: attempts, next_at: next_at})
        end
    end
  end

  # Grava o evento no log primeiro; só depois aplica no índice em memória.
  defp commit(st, id, event, update_fun) do
    case Log.append(st.fd, event) do
      {:ok, _loc} ->
        %{st | index: Map.update!(st.index, id, update_fun)}

      {:error, reason} ->
        # Sem gravação, o estado em memória não muda; a mensagem será tentada de novo.
        Logger.error("falha ao persistir #{inspect(event)}: #{inspect(reason)}")
        st
    end
  end

  # Backoff exponencial limitado com jitter de ±10%.
  defp backoff(attempts, cfg) do
    exp = min(cfg.max_backoff_ms, cfg.base_backoff_ms * Integer.pow(2, attempts - 1))
    jitter = div(exp, 10)
    exp + :rand.uniform(2 * jitter + 1) - jitter - 1
  end

  # ---------------------------------------------------------------------------
  # Recuperação
  # ---------------------------------------------------------------------------

  # Reconstrói o índice (apenas metadados) percorrendo o log em streaming.
  defp recover(path) do
    path
    |> Log.stream()
    |> Enum.reduce({%{}, 0}, &replay/2)
  end

  defp replay({pos, size, event}, {index, max_id}) do
    loc = {pos, size}

    case event do
      {:enqueued, id, _from, to, _body} ->
        meta = %{loc: loc, to: to, status: :pending, attempts: 0, next_at: 0}
        {Map.put(index, id, meta), max(max_id, id)}

      {:attempt, id, attempts, next_at} ->
        {Map.update!(index, id, &%{&1 | attempts: attempts, next_at: next_at}), max_id}

      {:done, id} ->
        {Map.update!(index, id, &%{&1 | status: :done}), max_id}

      {:dead, id} ->
        {Map.update!(index, id, &%{&1 | status: :dead}), max_id}
    end
  end

  # ---------------------------------------------------------------------------
  # Utilitários
  # ---------------------------------------------------------------------------

  defp schedule_tick(st), do: Process.send_after(self(), :tick, st.cfg.tick_ms)

  defp now_ms, do: System.system_time(:millisecond)
end
