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

  @default_chunk_size 500

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, Keyword.take(opts, [:name]))
  end

  def reserve(server, room, starts_at, ends_at, metadata \\ %{}) do
    GenServer.call(server, {:reserve, room, starts_at, ends_at, metadata})
  end

  def cancel(server, id) do
    GenServer.call(server, {:cancel, id})
  end

  # Produces a lazy stream; reservations are fetched in bounded-size chunks.
  def list_stream(server, room, chunk_size \\ @default_chunk_size)

  def list_stream(server, room, chunk_size)
      when is_integer(chunk_size) and chunk_size > 0 do
    Stream.resource(
      fn -> {nil, false} end,
      fn
        {_cursor, true} = state ->
          {:halt, state}

        {cursor, false} ->
          case GenServer.call(server, {:stream_chunk, room, cursor, chunk_size}) do
            {reservations, next_cursor, done?} ->
              {reservations, {next_cursor, done?}}
          end
      end,
      fn _state -> :ok end
    )
  end

  def list_stream(_server, _room, _chunk_size) do
    raise ArgumentError, "chunk_size must be a positive integer"
  end

  @impl true
  def init(_opts) do
    reservations =
      :ets.new(:room_reservations, [
        :ordered_set,
        :protected,
        read_concurrency: true
      ])

    ids =
      :ets.new(:room_reservation_ids, [
        :set,
        :protected,
        read_concurrency: true
      ])

    {:ok, %{reservations: reservations, ids: ids, next_id: 1}}
  end

  @impl true
  def handle_call(
        {:reserve, room, starts_at, ends_at, metadata},
        _from,
        state
      ) do
    cond do
      not is_number(starts_at) or not is_number(ends_at) or starts_at >= ends_at ->
        {:reply, {:error, :invalid_interval}, state}

      has_overlap?(
        state.reservations,
        room,
        starts_at,
        ends_at,
        :ets.first(state.reservations)
      ) ->
        {:reply, {:error, :overlap}, state}

      true ->
        id = state.next_id

        reservation = %{
          id: id,
          room: room,
          starts_at: starts_at,
          ends_at: ends_at,
          metadata: metadata
        }

        key = {room, starts_at, id}
        true = :ets.insert(state.reservations, {key, ends_at, reservation})
        true = :ets.insert(state.ids, {id, key})

        {:reply, {:ok, reservation}, %{state | next_id: id + 1}}
    end
  end

  def handle_call({:cancel, id}, _from, state) do
    case :ets.lookup(state.ids, id) do
      [{^id, key}] ->
        true = :ets.delete(state.reservations, key)
        true = :ets.delete(state.ids, id)
        {:reply, :ok, state}

      [] ->
        {:reply, {:error, :not_found}, state}
    end
  end

  def handle_call({:stream_chunk, room, cursor, chunk_size}, _from, state) do
    first_key =
      if cursor == nil do
        :ets.first(state.reservations)
      else
        :ets.next(state.reservations, cursor)
      end

    {reservations, next_cursor, done?} =
      collect_chunk(state.reservations, room, first_key, chunk_size, [])

    {:reply, {reservations, next_cursor, done?}, state}
  end

  # All reservation mutations are serialized by this GenServer, so checking
  # for conflicts and inserting a reservation form one atomic operation.
  defp has_overlap?(_table, _room, _starts_at, _ends_at, :"$end_of_table"),
    do: false

  defp has_overlap?(table, room, starts_at, ends_at, {other_room, other_start, _id} = key) do
    cond do
      other_room == room and other_start >= ends_at ->
        false

      other_room == room ->
        [{^key, existing_end, _reservation}] = :ets.lookup(table, key)

        existing_end > starts_at or
          has_overlap?(table, room, starts_at, ends_at, :ets.next(table, key))

      other_room < room ->
        has_overlap?(table, room, starts_at, ends_at, :ets.next(table, key))

      true ->
        false
    end
  end

  defp collect_chunk(_table, _room, :"$end_of_table", _limit, acc),
    do: {Enum.reverse(acc), nil, true}

  defp collect_chunk(table, room, {other_room, _start, _id} = key, limit, acc) do
    cond do
      other_room == room ->
        [{^key, _ends_at, reservation}] = :ets.lookup(table, key)
        next_acc = [reservation | acc]

        if length(next_acc) >= limit do
          {Enum.reverse(next_acc), key, false}
        else
          collect_chunk(table, room, :ets.next(table, key), limit, next_acc)
        end

      other_room < room ->
        collect_chunk(table, room, :ets.next(table, key), limit, acc)

      true ->
        {Enum.reverse(acc), nil, true}
    end
  end
end
