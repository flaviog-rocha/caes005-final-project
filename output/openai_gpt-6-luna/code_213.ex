# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o processamento de arquivos CSV, atendendo somente aos seguintes requisitos:
#
# 1. Executar determinadas operações periodicamente.
# 2. Detectar encerramentos inesperados de processos.
# 3. Permitir comunicação entre diferentes processos.
# 4. Diferenciar erros de validação de falhas operacionais.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule CsvProcessor do
  use GenServer

  @default_interval 60_000

  # Public API

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  def subscribe(server, pid \\ self()) when is_pid(pid) do
    GenServer.call(server, {:subscribe, pid})
  end

  def unsubscribe(server, pid \\ self()) when is_pid(pid) do
    GenServer.call(server, {:unsubscribe, pid})
  end

  def watch(server, pid) when is_pid(pid) do
    GenServer.call(server, {:watch, pid})
  end

  def process_now(server) do
    GenServer.call(server, :process_now, :infinity)
  end

  def status(server) do
    GenServer.call(server, :status)
  end

  # GenServer callbacks

  @impl true
  def init(opts) do
    path = Keyword.fetch!(opts, :path)
    interval = Keyword.get(opts, :interval, @default_interval)

    if not is_binary(path) or not is_integer(interval) or interval <= 0 do
      {:stop, {:validation_error, :invalid_options}}
    else
      timer = Process.send_after(self(), :tick, interval)

      {:ok,
       %{
         path: path,
         interval: interval,
         timer: timer,
         subscribers: %{},
         watched: %{}
       }}
    end
  end

  @impl true
  def handle_call({:subscribe, pid}, _from, state) do
    if Map.has_key?(state.subscribers, pid) do
      {:reply, :ok, state}
    else
      ref = Process.monitor(pid)
      {:reply, :ok, put_in(state.subscribers[pid], ref)}
    end
  end

  def handle_call({:unsubscribe, pid}, _from, state) do
    case Map.pop(state.subscribers, pid) do
      {nil, _subscribers} ->
        {:reply, :ok, state}

      {ref, subscribers} ->
        Process.demonitor(ref, [:flush])
        {:reply, :ok, %{state | subscribers: subscribers}}
    end
  end

  def handle_call({:watch, pid}, _from, state) do
    if Map.has_key?(state.watched, pid) do
      {:reply, :ok, state}
    else
      ref = Process.monitor(pid)
      {:reply, :ok, put_in(state.watched[pid], ref)}
    end
  end

  def handle_call(:process_now, _from, state) do
    result = process_file(state.path)
    notify_subscribers(state.subscribers, {:csv_result, self(), result})
    {:reply, result, state}
  end

  def handle_call(:status, _from, state) do
    {:reply,
     %{
       path: state.path,
       interval: state.interval,
       subscribers: Map.keys(state.subscribers),
       watched: Map.keys(state.watched)
     }, state}
  end

  @impl true
  def handle_info(:tick, state) do
    result = process_file(state.path)
    notify_subscribers(state.subscribers, {:csv_result, self(), result})

    timer = Process.send_after(self(), :tick, state.interval)
    {:noreply, %{state | timer: timer}}
  end

  def handle_info({:DOWN, ref, :process, pid, reason}, state) do
    {subscriber_ref, subscribers} = Map.pop(state.subscribers, pid)
    {watched_ref, watched} = Map.pop(state.watched, pid)

    state = %{state | subscribers: subscribers, watched: watched}

    if subscriber_ref == ref or watched_ref == ref do
      if watched_ref == ref do
        notify_subscribers(
          subscribers,
          {:process_down, self(), pid, reason}
        )
      end

      {:noreply, state}
    else
      {:noreply, state}
    end
  end

  @impl true
  def terminate(_reason, state) do
    Process.cancel_timer(state.timer)
    :ok
  end

  # CSV processing

  defp process_file(path) do
    case File.read(path) do
      {:ok, contents} ->
        case parse_csv(contents) do
          {:ok, []} ->
            {:validation_error, [:empty_file]}

          {:ok, [header | rows]} ->
            validate_rows(header, rows)

          {:error, reason} ->
            {:validation_error, [reason]}
        end

      {:error, reason} ->
        {:operational_error, {:file_read, reason}}
    end
  end

  defp validate_rows(header, rows) do
    errors =
      []
      |> validate_header(header)
      |> validate_row_widths(header, rows)

    case errors do
      [] ->
        {:ok, %{header: header, rows: rows}}

      errors ->
        {:validation_error, Enum.reverse(errors)}
    end
  end

  defp validate_header(errors, header) do
    cond do
      Enum.any?(header, &(&1 == "")) ->
        [:empty_header_field | errors]

      length(Enum.uniq(header)) != length(header) ->
        [:duplicate_header_field | errors]

      true ->
        errors
    end
  end

  defp validate_row_widths(errors, header, rows) do
    rows
    |> Enum.with_index(2)
    |> Enum.reduce(errors, fn {row, line}, acc ->
      if length(row) == length(header) do
        acc
      else
        [{:invalid_column_count, line, length(header), length(row)} | acc]
      end
    end)
  end

  defp parse_csv(contents) do
    scan(
      String.to_charlist(contents),
      [],
      [],
      [],
      false,
      false,
      false
    )
  end

  defp scan([], rows, fields, field, true, _after_quote, _touched) do
    {:error, :unterminated_quoted_field}
  end

  defp scan([], rows, fields, field, false, _after_quote, touched) do
    if fields != [] or field != [] or touched do
      {:ok, Enum.reverse([finish_row(fields, field) | rows])}
    else
      {:ok, Enum.reverse(rows)}
    end
  end

  defp scan([?", ?" | rest], rows, fields, field, true, false, _touched) do
    scan(rest, rows, [?" | field] |> then(fn _ -> fields end), [?" | field], true, false, true)
  end

  defp scan([?" | rest], rows, fields, field, true, false, touched) do
    scan(rest, rows, fields, field, false, true, touched)
  end

  defp scan([char | rest], rows, fields, field, true, false, touched) do
    scan(rest, rows, fields, [char | field], true, false, touched)
  end

  defp scan([?, | rest], rows, fields, field, false, true, _touched) do
    scan(rest, rows, [Enum.reverse(field) | fields], [], false, false, false)
  end

  defp scan([?\n | rest], rows, fields, field, false, true, _touched) do
    row = finish_row(fields, field)
    scan(rest, [row | rows], [], [], false, false, false)
  end

  defp scan([?\r, ?\n | rest], rows, fields, field, false, true, _touched) do
    row = finish_row(fields, field)
    scan(rest, [row | rows], [], [], false, false, false)
  end

  defp scan([char | _rest], _rows, _fields, _field, false, true, _touched) do
    {:error, {:unexpected_character_after_quote, char}}
  end

  defp scan([?" | rest], rows, fields, [], false, false, false) do
    scan(rest, rows, fields, [], true, false, true)
  end

  defp scan([?" | _rest], _rows, _fields, _field, false, false, _touched) do
    {:error, :quote_in_unquoted_field}
  end

  defp scan([?, | rest], rows, fields, field, false, false, _touched) do
    scan(rest, rows, [Enum.reverse(field) | fields], [], false, false, false)
  end

  defp scan([?\n | rest], rows, fields, field, false, false, _touched) do
    row = finish_row(fields, field)
    scan(rest, [row | rows], [], [], false, false, false)
  end

  defp scan([?\r, ?\n | rest], rows, fields, field, false, false, _touched) do
    row = finish_row(fields, field)
    scan(rest, [row | rows], [], [], false, false, false)
  end

  defp scan([char | rest], rows, fields, field, false, false, touched) do
    scan(rest, rows, fields, [char | field], false, false, touched or char != nil)
  end

  defp finish_row(fields, field) do
    [Enum.reverse(field) | fields]
    |> Enum.reverse()
    |> Enum.map(&List.to_string/1)
  end

  defp notify_subscribers(subscribers, message) do
    Enum.each(Map.keys(subscribers), fn pid ->
      send(pid, message)
    end)
  end
end
