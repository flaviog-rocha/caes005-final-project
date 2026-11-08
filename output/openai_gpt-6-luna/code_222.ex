# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de reservas de salas, atendendo somente aos seguintes requisitos:
#
# 1. Garantir a consistência dos dados durante operações concorrentes.
# 2. Processar grandes volumes de dados sem carregá-los integralmente na memória.
# 3. Quando fizer sentido para o domínio, considerar a comunicação com um banco de dados fictício e, opcionalmente, o uso de Ecto.Migration, sem tornar isso obrigatório.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule RoomReservations do
  use GenServer

  @default_path "room_reservations.log"
  @max_record_size 16 * 1024 * 1024

  def start_link(opts \\ []) do
    path = Keyword.get(opts, :path, @default_path)

    case Keyword.get(opts, :name) do
      nil -> GenServer.start_link(__MODULE__, path)
      name -> GenServer.start_link(__MODULE__, path, name: name)
    end
  end

  def reserve(server, id, room, starts_at, ends_at) do
    GenServer.call(server, {:reserve, id, room, starts_at, ends_at}, :infinity)
  end

  def cancel(server, id) do
    GenServer.call(server, {:cancel, id}, :infinity)
  end

  @impl true
  def init(path) do
    path = Path.expand(path)

    with :ok <- File.mkdir_p(Path.dirname(path)),
         :ok <- File.touch(path),
         {:ok, file} <- :file.open(String.to_charlist(path), [:raw, :binary, :append]) do
      {:ok, %{path: path, file: file}}
    else
      {:error, reason} -> {:stop, reason}
    end
  end

  @impl true
  def handle_call({:reserve, id, room, starts_at, ends_at}, _from, state) do
    result =
      with :ok <- validate_reservation(id, room, starts_at, ends_at),
           {:ok, result} <-
             locked(state, fn ->
               with {:ok, exists?} <- reservation_id_used?(state.path, id),
                    false <- exists? || {:error, :reservation_id_already_used},
                    {:ok, conflict?} <-
                      conflicting_reservation?(
                        state.path,
                        room,
                        starts_at,
                        ends_at
                      ),
                    false <- conflict? || {:error, :room_already_reserved},
                    :ok <- append_event(state, {:reserve, id, room, starts_at, ends_at}) do
                 {:ok, {:ok, id}}
               end
             end) do
        result
      else
        {:error, reason} -> {:error, reason}
        false -> {:error, :reservation_conflict}
      end

    reply_or_stop(result, state)
  end

  def handle_call({:cancel, id}, _from, state) do
    result =
      with :ok <- validate_id(id),
           {:ok, result} <-
             locked(state, fn ->
               with {:ok, reservation} <- find_reservation(state.path, id),
                    {:ok, cancelled?} <- reservation_cancelled?(state.path, id),
                    false <- cancelled? || {:error, :reservation_already_cancelled},
                    {:reserve, ^id, _room, _starts_at, _ends_at} <- reservation,
                    :ok <- append_event(state, {:cancel, id}) do
                 {:ok, :ok}
               else
                 nil -> {:error, :reservation_not_found}
                 {:error, _reason} = error -> error
                 false -> {:error, :reservation_already_cancelled}
               end
             end) do
        result
      else
        {:error, reason} -> {:error, reason}
      end

    reply_or_stop(result, state)
  end

  def handle_call(_request, _from, state) do
    {:reply, {:error, :unsupported_request}, state}
  end

  @impl true
  def terminate(_reason, state) do
    :file.close(state.file)
    :ok
  end

  defp validate_reservation(id, room, starts_at, ends_at) do
    with :ok <- validate_id(id),
         :ok <- validate_room(room),
         true <- is_integer(starts_at) and is_integer(ends_at) and starts_at < ends_at do
      :ok
    else
      false -> {:error, :invalid_time_range}
      {:error, _reason} = error -> error
    end
  end

  defp validate_id(id) when is_binary(id) and byte_size(id) > 0, do: :ok
  defp validate_id(_id), do: {:error, :invalid_reservation_id}

  defp validate_room(room) when is_binary(room) and byte_size(room) > 0, do: :ok
  defp validate_room(_room), do: {:error, :invalid_room}

  defp locked(state, operation) do
    lock = {{__MODULE__, state.path}, self()}

    case :global.trans(lock, operation) do
      :aborted -> {:error, :lock_aborted}
      {:aborted, reason} -> {:error, reason}
      result -> result
    end
  end

  defp reservation_id_used?(path, id) do
    scan_events(path, fn
      {:reserve, ^id, _room, _starts_at, _ends_at}, _acc -> {:halt, true}
      _event, acc -> {:cont, acc}
    end, false)
  end

  defp conflicting_reservation?(path, room, starts_at, ends_at) do
    scan_events(
      path,
      fn
        {:reserve, id, ^room, existing_start, existing_end}, _acc
        when starts_at < existing_end and existing_start < ends_at ->
          case reservation_cancelled?(path, id) do
            {:ok, true} -> {:cont, false}
            {:ok, false} -> {:halt, true}
            {:error, reason} -> {:error, reason}
          end

        _event, acc ->
          {:cont, acc}
      end,
      false
    )
  end

  defp find_reservation(path, id) do
    scan_events(
      path,
      fn
        {:reserve, ^id, _room, _starts_at, _ends_at} = event, _acc ->
          {:halt, event}

        _event, acc ->
          {:cont, acc}
      end,
      nil
    )
  end

  defp reservation_cancelled?(path, id) do
    scan_events(
      path,
      fn
        {:cancel, ^id}, _acc -> {:halt, true}
        _event, acc -> {:cont, acc}
      end,
      false
    )
  end

  defp append_event(state, event) do
    payload = :erlang.term_to_binary(event)
    size = byte_size(payload)

    if size > @max_record_size do
      {:error, :record_too_large}
    else
      case :file.write(state.file, [<<size::unsigned-big-32>>, payload]) do
        :ok ->
          :file.sync(state.file)

        {:error, reason} ->
          {:error, reason}
      end
    end
  end

  # Os eventos são lidos do arquivo um por vez; a memória não cresce com o histórico.
  defp scan_events(path, callback, acc) do
    case :file.open(String.to_charlist(path), [:raw, :binary, :read]) do
      {:ok, file} ->
        try do
          scan_loop(file, callback, acc)
        after
          :file.close(file)
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp scan_loop(file, callback, acc) do
    case read_record(file) do
      :eof ->
        {:ok, acc}

      {:ok, event} ->
        case callback.(event, acc) do
          {:cont, next_acc} -> scan_loop(file, callback, next_acc)
          {:halt, result} -> {:ok, result}
          {:error, reason} -> {:error, reason}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp read_record(file) do
    case read_exact(file, 4) do
      :eof ->
        :eof

      {:ok, <<size::unsigned-big-32>>} when size <= @max_record_size ->
        with {:ok, payload} <- read_exact(file, size),
             {:ok, event} <- decode_event(payload) do
          {:ok, event}
        end

      {:ok, <<_size::unsigned-big-32>>} ->
        {:error, :record_too_large}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp read_exact(_file, 0), do: {:ok, <<>>}

  defp read_exact(file, size) do
    read_exact(file, size, [])
  end

  defp read_exact(_file, 0, chunks), do: {:ok, chunks |> Enum.reverse() |> IO.iodata_to_binary()}

  defp read_exact(file, remaining, chunks) do
    case :file.read(file, remaining) do
      {:ok, data} ->
        read_exact(file, remaining - byte_size(data), [data | chunks])

      :eof when remaining == 0 ->
        {:ok, chunks |> Enum.reverse() |> IO.iodata_to_binary()}

      :eof ->
        {:error, :truncated_log}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp decode_event(payload) do
    try do
      case :erlang.binary_to_term(payload, [:safe]) do
        {:reserve, id, room, starts_at, ends_at}
        when is_binary(id) and is_binary(room) and is_integer(starts_at) and
               is_integer(ends_at) ->
          {:ok, {:reserve, id, room, starts_at, ends_at}}

        {:cancel, id} when is_binary(id) ->
          {:ok, {:cancel, id}}

        _ ->
          {:error, :invalid_log_record}
      end
    rescue
      _ -> {:error, :invalid_log_record}
    end
  end

  defp reply_or_stop({:ok, reply}, state), do: {:reply, reply, state}

  defp reply_or_stop({:error, reason}, state) do
    {:reply, {:error, reason}, state}
  end

  defp reply_or_stop(other, state), do: {:reply, other, state}
end
