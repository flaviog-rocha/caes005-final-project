# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de troca de mensagens entre usuários, atendendo somente aos seguintes requisitos:
#
# 1. Persistir os dados para recuperação após reinicializações.
# 2. Implementar novas tentativas após falhas temporárias.
# 3. Processar grandes volumes de dados sem carregá-los integralmente na memória.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule MessageSystem do
  @moduledoc """
  Sistema de mensagens com journal persistente, processamento incremental e
  repetição automática de falhas temporárias.

  A função `:delivery` recebida em `start_link/1` deve retornar `:ok` quando
  a mensagem for entregue, `{:retry, motivo}` ou `{:error, :temporary}` para
  falhas temporárias, e `{:error, motivo}` para falhas permanentes. A entrega
  é pelo menos uma vez: uma falha do processo após a entrega, mas antes da
  gravação do checkpoint, pode causar uma nova entrega da mesma mensagem.
  """

  use GenServer
  require Logger

  @type message :: %{
          id: non_neg_integer(),
          from: binary(),
          to: binary(),
          body: binary()
        }

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, Keyword.take(opts, [:name]))
  end

  @doc "Persiste uma mensagem e agenda seu processamento."
  def send_message(server \\ __MODULE__, from, to, body)
      when is_binary(from) and is_binary(to) and is_binary(body) do
    GenServer.call(server, {:send_message, from, to, body})
  end

  @doc """
  Retorna um `Stream` de mensagens persistidas no journal, sem carregá-las
  todas na memória.
  """
  def stream_messages(path) do
    path
    |> File.stream!([], :line)
    |> Stream.map(&decode_record!/1)
  end

  @impl true
  def init(opts) do
    path = Keyword.fetch!(opts, :path)
    delivery = Keyword.fetch!(opts, :delivery)
    cursor_path = Keyword.get(opts, :cursor_path, path <> ".cursor")

    unless is_function(delivery, 1) do
      raise ArgumentError, ":delivery deve ser uma função de aridade 1"
    end

    File.mkdir_p!(Path.dirname(path))
    File.mkdir_p!(Path.dirname(cursor_path))

    journal = File.open!(path, [:append, :binary])
    cursor = load_cursor(cursor_path)

    send(self(), :drain)

    {:ok,
     %{
       path: path,
       cursor_path: cursor_path,
       journal: journal,
       cursor: cursor,
       delivery: delivery,
       base_delay_ms: Keyword.get(opts, :base_delay_ms, 250),
       max_delay_ms: Keyword.get(opts, :max_delay_ms, 30_000)
     }}
  end

  @impl true
  def handle_call({:send_message, from, to, body}, _from, state) do
    {:ok, id} = :file.position(state.journal, :eof)
    message = %{id: id, from: from, to: to, body: body}
    record = encode_record(message)

    :ok = :file.write(state.journal, record)
    :ok = :file.sync(state.journal)

    send(self(), :drain)
    {:reply, {:ok, id}, state}
  end

  @impl true
  def handle_info(:drain, state) do
    {:noreply, drain(state)}
  end

  @impl true
  def terminate(_reason, state) do
    :file.close(state.journal)
    :ok
  end

  defp drain(state) do
    file = File.open!(state.path, [:read, :binary])

    try do
      {:ok, _offset} = :file.position(file, state.cursor)
      drain_next(file, state)
    after
      :file.close(file)
    end
  end

  defp drain_next(file, state) do
    case :file.read_line(file) do
      :eof ->
        state

      {:ok, line} ->
        next_offset = {:ok, offset} = :file.position(file, :cur)
        message = decode_record!(line)

        case deliver_with_retry(message, state, 0) do
          :ok ->
            save_cursor!(state.cursor_path, offset)
            drain_next(file, %{state | cursor: elem(next_offset, 1)})

          {:permanent, reason} ->
            Logger.error(
              "Entrega permanente falhou para a mensagem #{message.id}: #{inspect(reason)}"
            )

            state
        end
    end
  end

  defp deliver_with_retry(message, state, attempt) do
    case invoke_delivery(state.delivery, message) do
      :ok ->
        :ok

      {:temporary, reason} ->
        delay = retry_delay(state.base_delay_ms, state.max_delay_ms, attempt)
        Logger.warning(
          "Falha temporária na mensagem #{message.id} (#{inspect(reason)}); nova tentativa em #{delay} ms"
        )

        Process.sleep(delay)
        deliver_with_retry(message, state, attempt + 1)

      {:permanent, reason} ->
        {:permanent, reason}
    end
  end

  defp invoke_delivery(delivery, message) do
    try do
      case delivery.(message) do
        :ok -> :ok
        {:retry, reason} -> {:temporary, reason}
        {:error, :temporary} -> {:temporary, :temporary}
        {:error, {:temporary, reason}} -> {:temporary, reason}
        {:error, reason} -> {:permanent, reason}
        other -> {:permanent, {:invalid_delivery_result, other}}
      end
    rescue
      error -> {:temporary, {:exception, error.__struct__}}
    catch
      kind, reason -> {:temporary, {kind, reason}}
    end
  end

  defp retry_delay(base, maximum, attempt) do
    Enum.reduce_while(1..attempt, min(base, maximum), fn _, delay ->
      next = min(delay * 2, maximum)
      if next >= maximum, do: {:halt, maximum}, else: {:cont, next}
    end)
  end

  defp encode_record(message) do
    [message |> :erlang.term_to_binary() |> Base.encode64(), "\n"]
  end

  defp decode_record!(line) do
    line
    |> String.trim_trailing("\n")
    |> Base.decode64!()
    |> :erlang.binary_to_term([:safe])
  end

  defp load_cursor(path) do
    case File.read(path) do
      {:ok, contents} ->
        case Integer.parse(String.trim(contents)) do
          {offset, ""} when offset >= 0 -> offset
          _ -> raise ArgumentError, "checkpoint inválido: #{path}"
        end

      {:error, :enoent} ->
        0

      {:error, reason} ->
        raise File.Error, reason: reason, action: "ler checkpoint", path: path
    end
  end

  defp save_cursor!(path, offset) do
    temporary_path = path <> ".tmp"

    File.write!(temporary_path, Integer.to_string(offset))

    temporary_file = File.open!(temporary_path, [:read, :write, :binary])
    :ok = :file.sync(temporary_file)
    :ok = :file.close(temporary_file)

    File.rename!(temporary_path, path)
  end
end
