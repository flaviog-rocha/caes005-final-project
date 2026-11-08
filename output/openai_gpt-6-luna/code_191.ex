# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o processamento de arquivos CSV, atendendo somente aos seguintes requisitos:
#
# 1. Manter um histórico das operações realizadas.
# 2. Impedir atualizações parcialmente concluídas.
# 3. Recuperar processos após falhas inesperadas.
# 4. Persistir os dados para recuperação após reinicializações.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule CsvProcessor do
  use GenServer

  @moduledoc """
  Processa arquivos CSV e mantém operações e conteúdo de saída persistidos
  em um único arquivo de estado. As substituições de arquivos são atômicas;
  saídas pendentes são restauradas ao iniciar ou chamar `recover/1`.
  """

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, Keyword.take(opts, [:name]))
  end

  def process_file(server, input_path, output_path) do
    GenServer.call(server, {:process_file, input_path, output_path}, :infinity)
  end

  def history(server) do
    GenServer.call(server, :history)
  end

  def recover(server) do
    GenServer.call(server, :recover, :infinity)
  end

  @impl true
  def init(opts) do
    state_path =
      opts
      |> Keyword.fetch!(:state_path)
      |> Path.expand()

    with {:ok, persisted_state} <- load_state(state_path),
         :ok <- recover_outputs(state_path, persisted_state) do
      {:ok, %{state_path: state_path, data: persisted_state}}
    else
      {:error, reason} -> {:stop, reason}
    end
  end

  @impl true
  def handle_call(:history, _from, state) do
    {:reply, Enum.reverse(state.data.history), state}
  end

  def handle_call(:recover, _from, state) do
    {:reply, recover_outputs(state.state_path, state.data), state}
  end

  def handle_call({:process_file, input_path, output_path}, _from, state) do
    input_path = Path.expand(input_path)
    output_path = Path.expand(output_path)

    result =
      with false <- output_path == state.state_path,
           :ok <- recover_outputs(state.state_path, state.data),
           {:ok, contents} <- File.read(input_path),
           {:ok, row_count} <- count_csv_rows(contents) do
        commit_file(state, input_path, output_path, contents, row_count)
      else
        true -> {:error, :output_cannot_be_state_file}
        {:error, reason} -> {:error, reason}
      end

    case result do
      {:ok, entry, new_data} ->
        {:reply, {:ok, entry}, %{state | data: new_data}}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  defp commit_file(state, input_path, output_path, contents, row_count) do
    entry = %{
      source: input_path,
      output: output_path,
      rows: row_count,
      bytes: byte_size(contents),
      committed_at: DateTime.utc_now() |> DateTime.to_iso8601()
    }

    new_data = %{
      state.data
      | history: [entry | state.data.history],
        outputs: Map.put(state.data.outputs, output_path, contents)
    }

    case persist_state(state.state_path, new_data) do
      :ok ->
        case atomic_write(output_path, contents) do
          :ok -> {:ok, entry, new_data}
          {:error, reason} -> {:error, {:committed_output_pending_recovery, reason}}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp load_state(path) do
    case File.read(path) do
      {:ok, bytes} ->
        try do
          case :erlang.binary_to_term(bytes, [:safe]) do
            %{version: 1, history: history, outputs: outputs}
            when is_list(history) and is_map(outputs) ->
              if Enum.all?(outputs, fn {key, value} ->
                   is_binary(key) and is_binary(value)
                 end) do
                {:ok, %{version: 1, history: history, outputs: outputs}}
              else
                {:error, :invalid_state}
              end

            _ ->
              {:error, :invalid_state}
          end
        rescue
          _ -> {:error, :invalid_state}
        end

      {:error, :enoent} ->
        {:ok, %{version: 1, history: [], outputs: %{}}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp persist_state(path, data) do
    atomic_write(path, :erlang.term_to_binary(data))
  end

  defp recover_outputs(state_path, data) do
    Enum.reduce_while(data.outputs, :ok, fn {path, contents}, :ok ->
      if Path.expand(path) == state_path do
        {:halt, {:error, :invalid_output_path_in_state}}
      else
        case atomic_write(path, contents) do
          :ok -> {:cont, :ok}
          {:error, reason} -> {:halt, {:error, {:recovery_failed, path, reason}}}
        end
      end
    end)
  end

  defp atomic_write(path, contents) do
    directory = Path.dirname(path)
    temporary = path <> ".tmp." <> Integer.to_string(System.unique_integer([:positive]))

    result =
      with :ok <- File.mkdir_p(directory),
           :ok <- write_and_sync(temporary, contents),
           :ok <- File.rename(temporary, path) do
        :ok
      end

    if result != :ok do
      File.rm(temporary)
    end

    result
  end

  defp write_and_sync(path, contents) do
    case File.open(path, [:write, :binary]) do
      {:ok, device} ->
        write_result =
          with :ok <- IO.binwrite(device, contents),
               :ok <- :file.sync(device) do
            :ok
          end

        close_result = File.close(device)

        case {write_result, close_result} do
          {:ok, :ok} -> :ok
          {{:error, _} = error, _} -> error
          {_, {:error, _} = error} -> error
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  # Conta registros CSV e valida aspas, delimitadores e quebras de linha.
  defp count_csv_rows(contents), do: scan_csv(contents, :start, 0, false)

  defp scan_csv(<<>>, :quoted, _rows, _open), do: {:error, :unterminated_quoted_field}

  defp scan_csv(<<>>, _state, rows, open) do
    {:ok, rows + if(open, do: 1, else: 0)}
  end

  defp scan_csv(<<34, 34, rest::binary>>, :quoted, rows, open) do
    scan_csv(rest, :quoted, rows, open)
  end

  defp scan_csv(<<34, rest::binary>>, :quoted, rows, open) do
    scan_csv(rest, :closed, rows, open)
  end

  defp scan_csv(<<_byte, rest::binary>>, :quoted, rows, open) do
    scan_csv(rest, :quoted, rows, open)
  end

  defp scan_csv(<<13, 10, rest::binary>>, state, rows, _open)
       when state in [:start, :plain, :closed] do
    scan_csv(rest, :start, rows + 1, false)
  end

  defp scan_csv(<<10, rest::binary>>, state, rows, _open)
       when state in [:start, :plain, :closed] do
    scan_csv(rest, :start, rows + 1, false)
  end

  defp scan_csv(<<13, _rest::binary>>, state, _rows, _open)
       when state in [:start, :plain, :closed] do
    {:error, :bare_carriage_return}
  end

  defp scan_csv(<<34, rest::binary>>, :start, rows, _open) do
    scan_csv(rest, :quoted, rows, true)
  end

  defp scan_csv(<<34, _rest::binary>>, :plain, _rows, _open) do
    {:error, :unexpected_quote}
  end

  defp scan_csv(<<34, _rest::binary>>, :closed, _rows, _open) do
    {:error, :unexpected_character_after_quote}
  end

  defp scan_csv(<<44, rest::binary>>, state, rows, _open)
       when state in [:start, :plain, :closed] do
    scan_csv(rest, :start, rows, true)
  end

  defp scan_csv(<<_byte, rest::binary>>, :closed, _rows, _open) do
    if rest == <<>> do
      {:error, :unexpected_character_after_quote}
    else
      {:error, :unexpected_character_after_quote}
    end
  end

  defp scan_csv(<<_byte, rest::binary>>, :start, rows, _open) do
    scan_csv(rest, :plain, rows, true)
  end

  defp scan_csv(<<_byte, rest::binary>>, :plain, rows, open) do
    scan_csv(rest, :plain, rows, open)
  end
end
