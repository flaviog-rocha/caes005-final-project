# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de troca de mensagens entre usuários, atendendo somente aos seguintes requisitos:
#
# 1. Persistir os dados para recuperação após reinicializações.
# 2. Implementar novas tentativas após falhas temporárias.
# 3. Processar grandes volumes de dados sem carregá-los integralmente na memória.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule MessageExchange do
  @moduledoc """
  Sistema de troca de mensagens entre usuários.

  Persistência:
    Cada evento é gravado em um log binário append-only com frames
    `<<tamanho::32, crc32::32, payload::binary>>` e sincronizado em disco
    (`datasync`) antes do retorno ao chamador. Na inicialização o log é
    reproduzido registro a registro (streaming), reconstruindo o estado
    pendente. Um registro final incompleto ou corrompido é descartado e o
    arquivo é truncado nesse ponto.

  Novas tentativas:
    Falhas transitórias são reagendadas com backoff exponencial e jitter,
    até `max_attempts`. Falhas permanentes e esgotamento de tentativas
    levam a mensagem ao estado `dead`. O número de tentativas é persistido,
    portanto sobrevive a reinicializações.

  Grandes volumes:
    Em memória há apenas um índice `id => %{from, to, loc, attempts}`, onde
    `loc` é a posição do registro no log. Os corpos das mensagens são lidos
    sob demanda (`pread`) em lotes de tamanho `batch_size`. A recuperação
    não carrega o log inteiro, apenas processa um registro por vez.

  O `transport` é uma função `(mensagem -> :ok | {:error, :transient | :permanent})`
  fornecida pelo usuário. A entrega é at-least-once: se o processo cair entre a
  entrega e a gravação de `:delivered`, a mensagem pode ser reentregue.

  Exemplo:

      {:ok, _} = MessageExchange.start_link(
        data_dir: "/var/lib/exchange",
        transport: fn msg -> MyInbox.push(msg.to, msg) end
      )
      {:ok, _id} = MessageExchange.send_message("alice", "bob", "oi")
  """

  use GenServer

  @header_size 8

  # ---------------------------------------------------------------------------
  # API pública
  # ---------------------------------------------------------------------------

  @doc "Inicia o servidor. Veja `build_config/1` para as opções disponíveis."
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))
  end

  @doc "Enfileira uma mensagem de `from` para `to`. Retorna `{:ok, id}` após persistir."
  def send_message(server \\ __MODULE__, from, to, body) do
    GenServer.call(server, {:send, from, to, body})
  end

  @doc "Retorna métricas básicas do servidor."
  def stats(server \\ __MODULE__) do
    GenServer.call(server, :stats)
  end

  # ---------------------------------------------------------------------------
  # Callbacks do GenServer
  # ---------------------------------------------------------------------------

  @impl true
  def init(opts) do
    cfg = build_config(opts)
    File.mkdir_p!(cfg.data_dir)

    path = Path.join(cfg.data_dir, "exchange.log")
    {:ok, fd} = File.open(path, [:read, :write, :binary])

    state = recover(%{cfg: cfg, fd: fd, size: 0, pending: %{}, ready: :queue.new()})
    schedule_tick(cfg.tick_ms)
    {:ok, state}
  end

  @impl true
  def handle_call({:send, from, to, body}, _from, state) do
    id = new_id()
    state = persist!(state, {:message, id, from, to, body})
    {:reply, {:ok, id}, state}
  end

  def handle_call(:stats, _from, state) do
    stats = %{
      pending: map_size(state.pending),
      queued: :queue.len(state.ready),
      log_bytes: state.size
    }

    {:reply, stats, state}
  end

  @impl true
  def handle_info(:tick, state) do
    state = dispatch_batch(state)
    schedule_tick(state.cfg.tick_ms)
    {:noreply, state}
  end

  # Reenfileira uma mensagem após o backoff de uma falha transitória.
  def handle_info({:requeue, id}, state) do
    {:noreply, %{state | ready: :queue.in(id, state.ready)}}
  end

  @impl true
  def terminate(_reason, state) do
    :file.close(state.fd)
    :ok
  end

  # ---------------------------------------------------------------------------
  # Configuração
  # ---------------------------------------------------------------------------

  defp build_config(opts) do
    defaults = [
      data_dir: "data/exchange",
      transport: &default_transport/1,
      batch_size: 100,
      concurrency: System.schedulers_online(),
      delivery_timeout: 5_000,
      max_attempts: 5,
      base_backoff_ms: 200,
      max_backoff_ms: 30_000,
      tick_ms: 50
    ]

    defaults
    |> Keyword.merge(opts)
    |> Map.new()
  end

  # Stub de transporte. Substitua pela função real de entrega ao usuário.
  defp default_transport(_msg), do: :ok

  # ---------------------------------------------------------------------------
  # Persistência
  # ---------------------------------------------------------------------------

  # Grava o evento no log, sincroniza e aplica ao estado em memória.
  # Em caso de falha de I/O o processo cai; o supervisor o reinicia e a
  # recuperação reconstrói um estado consistente a partir do log.
  defp persist!(state, event) do
    payload = :erlang.term_to_binary(event)
    len = byte_size(payload)
    crc = :erlang.crc32(payload)
    frame = <<len::32, crc::32, payload::binary>>
    frame_len = byte_size(frame)
    offset = state.size

    :ok = :file.pwrite(state.fd, offset, frame)
    :ok = :file.datasync(state.fd)

    apply_event(event, {offset, frame_len}, %{state | size: offset + frame_len})
  end

  # Aplica um evento ao estado. Usado tanto na escrita quanto na recuperação,
  # garantindo que ambos os caminhos produzam o mesmo estado.
  defp apply_event({:message, id, from, to, _body}, loc, st) do
    entry = %{id: id, from: from, to: to, loc: loc, attempts: 0}

    %{
      st
      | pending: Map.put(st.pending, id, entry),
        ready: :queue.in(id, st.ready)
    }
  end

  defp apply_event({:retry, id, attempts}, _loc, st) do
    case st.pending do
      %{^id => entry} ->
        %{st | pending: Map.put(st.pending, id, %{entry | attempts: attempts})}

      _ ->
        st
    end
  end

  defp apply_event({:delivered, id}, _loc, st) do
    %{st | pending: Map.delete(st.pending, id)}
  end

  defp apply_event({:dead, id, _reason}, _loc, st) do
    %{st | pending: Map.delete(st.pending, id)}
  end

  # Reproduz o log em streaming. Retorna o offset válido final e o estado.
  defp recover(st) do
    {:ok, 0} = :file.position(st.fd, 0)
    {:ok, end_pos, st} = replay(st, 0)

    # Descarta qualquer cauda incompleta ou corrompida.
    {:ok, ^end_pos} = :file.position(st.fd, end_pos)
    :ok = :file.truncate(st.fd)

    %{st | size: end_pos}
  end

  defp replay(st, offset) do
    with {:ok, <<len::32, crc::32>>} <- :file.read(st.fd, @header_size),
         {:ok, payload} when byte_size(payload) == len <- :file.read(st.fd, len),
         true <- :erlang.crc32(payload) == crc do
      event = :erlang.binary_to_term(payload, [:safe])
      st = apply_event(event, {offset, @header_size + len}, st)
      replay(st, offset + @header_size + len)
    else
      _ -> {:ok, offset, st}
    end
  end

  # Lê apenas o registro necessário, validando o CRC.
  defp read_event(st, {offset, len}) do
    with {:ok, <<_len::32, crc::32, payload::binary>>} <- :file.pread(st.fd, offset, len),
         true <- :erlang.crc32(payload) == crc do
      {:ok, :erlang.binary_to_term(payload, [:safe])}
    else
      _ -> :error
    end
  end

  # ---------------------------------------------------------------------------
  # Processamento em lotes
  # ---------------------------------------------------------------------------

  defp dispatch_batch(st) do
    {ids, queue} = take_ready(st.ready, st.pending, st.cfg.batch_size, [])
    {loaded, st} = load_batch(%{st | ready: queue}, ids)

    loaded
    |> run_deliveries(st.cfg)
    |> Enum.reduce(st, &settle/2)
  end

  # Extrai até `n` ids prontos, descartando ids obsoletos.
  defp take_ready(queue, _pending, 0, acc), do: {Enum.reverse(acc), queue}

  defp take_ready(queue, pending, n, acc) do
    case :queue.out(queue) do
      {:empty, queue} ->
        {Enum.reverse(acc), queue}

      {{:value, id}, rest} when is_map_key(pending, id) ->
        take_ready(rest, pending, n - 1, [id | acc])

      {{:value, _stale}, rest} ->
        take_ready(rest, pending, n, acc)
    end
  end

  # Carrega os corpos do lote a partir do log. Registros corrompidos são
  # movidos para `dead` para não bloquear a fila.
  defp load_batch(st, ids) do
    {loaded, st} =
      Enum.reduce(ids, {[], st}, fn id, {acc, st} ->
        entry = Map.fetch!(st.pending, id)

        case read_event(st, entry.loc) do
          {:ok, {:message, ^id, _from, _to, body}} ->
            {[{entry, body} | acc], st}

          _ ->
            {acc, persist!(st, {:dead, id, :corrupted})}
        end
      end)

    {Enum.reverse(loaded), st}
  end

  # Executa as entregas com concorrência e timeout limitados.
  defp run_deliveries(loaded, cfg) do
    loaded
    |> Task.async_stream(
      fn {entry, body} -> {entry, deliver(cfg.transport, to_message(entry, body))} end,
      max_concurrency: cfg.concurrency,
      timeout: cfg.delivery_timeout,
      on_timeout: :kill_task,
      zip_input_on_exit: true
    )
    |> Enum.map(fn
      {:ok, outcome} -> outcome
      {:exit, {{entry, _body}, _reason}} -> {entry, {:error, :transient}}
    end)
  end

  defp to_message(entry, body) do
    %{
      id: entry.id,
      from: entry.from,
      to: entry.to,
      body: body,
      attempt: entry.attempts + 1
    }
  end

  # Qualquer exceção, exit ou retorno inesperado é tratado como falha transitória.
  defp deliver(transport, msg) do
    case transport.(msg) do
      :ok -> :ok
      {:error, :permanent} -> {:error, :permanent}
      _ -> {:error, :transient}
    end
  rescue
    _ -> {:error, :transient}
  catch
    :exit, _ -> {:error, :transient}
  end

  # Aplica o resultado de uma entrega, persistindo a transição de estado.
  defp settle({entry, :ok}, st) do
    persist!(st, {:delivered, entry.id})
  end

  defp settle({entry, {:error, :permanent}}, st) do
    persist!(st, {:dead, entry.id, :permanent})
  end

  defp settle({entry, {:error, :transient}}, st) do
    attempts = entry.attempts + 1

    if attempts >= st.cfg.max_attempts do
      persist!(st, {:dead, entry.id, :max_attempts})
    else
      Process.send_after(self(), {:requeue, entry.id}, backoff(st.cfg, attempts))
      persist!(st, {:retry, entry.id, attempts})
    end
  end

  # Backoff exponencial limitado com jitter (entre 50% e 100% do valor).
  defp backoff(cfg, attempt) do
    exp = min(cfg.base_backoff_ms * Integer.pow(2, attempt - 1), cfg.max_backoff_ms)
    half = div(exp, 2)
    half + :rand.uniform(half + 1) - 1
  end

  # ---------------------------------------------------------------------------
  # Utilidades
  # ---------------------------------------------------------------------------

  defp schedule_tick(ms), do: Process.send_after(self(), :tick, ms)

  # Identificador único com baixa probabilidade de colisão entre reinicializações.
  defp new_id do
    {System.system_time(:nanosecond), System.unique_integer([:positive])}
  end
end
