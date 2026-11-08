# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de reservas de salas, atendendo somente aos seguintes requisitos:
#
# 1. Garantir a consistência dos dados durante operações concorrentes.
# 2. Processar grandes volumes de dados sem carregá-los integralmente na memória.
# 3. Quando fizer sentido para o domínio, considerar a comunicação com um banco de dados fictício e, opcionalmente, o uso de Ecto.Migration, sem tornar isso obrigatório.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# reservas_salas.exs
# Execute com: elixir reservas_salas.exs
#
# Requisitos atendidos:
# 1. Consistência em concorrência: todas as escritas passam por um único processo
#    (GenServer). Verificação de conflito e inserção ocorrem de forma atômica dentro
#    dele, então duas requisições concorrentes não conseguem reservar o mesmo horário.
#    Leituras são concorrentes, feitas diretamente na ETS.
# 2. Grandes volumes: importação via File.stream! em lotes (Stream.chunk_every) e leitura
#    paginada por cursor (Stream.resource). Nada é carregado inteiro em memória.
# 3. Banco fictício em ETS, com contrato (behaviour). Equivalente opcional com Ecto.Migration
#    descrito ao final, em comentário.

defmodule RoomBooking.Booking do
  @moduledoc """
  Reserva de sala. O intervalo é semiaberto: [starts_at, ends_at).
  Timestamps são inteiros unix (segundos), o que torna a comparação barata.
  """

  @enforce_keys [:id, :room_id, :user_id, :starts_at, :ends_at]
  defstruct [:id, :room_id, :user_id, :starts_at, :ends_at]

  @type t :: %__MODULE__{
          id: pos_integer() | nil,
          room_id: String.t(),
          user_id: String.t(),
          starts_at: integer(),
          ends_at: integer()
        }

  @doc "Valida os dados e constrói uma reserva ainda sem id (id é atribuído pelo repositório)."
  @spec new(String.t(), String.t(), integer(), integer()) :: {:ok, t()} | {:error, atom()}
  def new(room_id, user_id, starts_at, ends_at)
      when is_binary(room_id) and is_binary(user_id) and
             is_integer(starts_at) and is_integer(ends_at) do
    if starts_at < ends_at do
      {:ok,
       %__MODULE__{
         id: nil,
         room_id: room_id,
         user_id: user_id,
         starts_at: starts_at,
         ends_at: ends_at
       }}
    else
      {:error, :invalid_interval}
    end
  end

  def new(_, _, _, _), do: {:error, :invalid_params}
end

defmodule RoomBooking.Repo do
  @moduledoc """
  Contrato do repositório. Pode ser implementado pelo banco fictício (ETS)
  ou por um adaptador real (ex.: Ecto), sem alterar o restante do código.
  """

  alias RoomBooking.Booking

  @callback insert_if_available(Booking.t()) :: {:ok, Booking.t()} | {:error, :conflict}
  @callback cancel(pos_integer()) :: :ok | {:error, :not_found}
  @callback fetch(pos_integer()) :: {:ok, Booking.t()} | {:error, :not_found}
  @callback page(pos_integer() | nil, pos_integer()) :: [Booking.t()]
end

defmodule RoomBooking.FakeDB do
  @moduledoc """
  Banco fictício baseado em ETS.

  Consistência: um único processo escritor (este GenServer) executa a verificação de
  conflito e a inserção. Como as mensagens são processadas uma por vez, não há
  condição de corrida entre "verificar" e "inserir".

  Leitura: feita diretamente na tabela ETS com read_concurrency, sem passar pelo
  processo escritor, então escala com o número de leitores.
  """

  use GenServer
  @behaviour RoomBooking.Repo

  alias RoomBooking.Booking

  # ordered_set: id -> %Booking{}. Permite paginação por cursor com :ets.next/2.
  @bookings :fakedb_bookings_by_id
  # ordered_set: {room_id, starts_at, id} -> ends_at. Permite buscar conflitos por sala
  # usando o prefixo da chave (partial key), sem varrer a tabela inteira.
  @index :fakedb_bookings_index

  ## API pública

  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, :ok, name: name)
  end

  @impl RoomBooking.Repo
  def insert_if_available(%Booking{} = booking) do
    GenServer.call(__MODULE__, {:insert, booking})
  end

  @doc """
  Insere um lote inteiro numa única mensagem ao escritor. Retorna apenas contagens,
  então o resultado tem tamanho constante, independentemente do lote.
  """
  def insert_batch(bookings) when is_list(bookings) do
    GenServer.call(__MODULE__, {:insert_batch, bookings}, :infinity)
  end

  @impl RoomBooking.Repo
  def cancel(id) do
    GenServer.call(__MODULE__, {:cancel, id})
  end

  @impl RoomBooking.Repo
  def fetch(id) do
    case :ets.lookup(@bookings, id) do
      [{^id, booking}] -> {:ok, booking}
      [] -> {:error, :not_found}
    end
  end

  @impl RoomBooking.Repo
  def page(after_id, limit) when is_integer(limit) and limit > 0 do
    # Cursor: começa após `after_id`. Com nil, começa do primeiro registro.
    first =
      case after_id do
        nil -> :ets.first(@bookings)
        id -> :ets.next(@bookings, id)
      end

    collect(first, limit, [])
  end

  @doc """
  Stream preguiçosa de todas as reservas. Busca páginas sob demanda, então o uso
  de memória é limitado ao tamanho da página, não ao total de registros.
  """
  def stream_all(page_size \\ 500) do
    Stream.resource(
      fn -> nil end,
      fn cursor ->
        case page(cursor, page_size) do
          [] -> {:halt, cursor}
          items -> {items, List.last(items).id}
        end
      end,
      fn _ -> :ok end
    )
  end

  ## Callbacks do GenServer (processo escritor)

  @impl true
  def init(:ok) do
    :ets.new(@bookings, [:ordered_set, :named_table, :protected, read_concurrency: true])
    :ets.new(@index, [:ordered_set, :named_table, :protected])
    {:ok, %{next_id: 1}}
  end

  @impl true
  def handle_call({:insert, booking}, _from, %{next_id: id} = state) do
    stored = %{booking | id: id}

    case do_insert(stored) do
      :ok -> {:reply, {:ok, stored}, %{state | next_id: id + 1}}
      {:error, _} = error -> {:reply, error, state}
    end
  end

  def handle_call({:insert_batch, bookings}, _from, %{next_id: id0} = state) do
    {counts, next_id} =
      Enum.reduce(bookings, {%{ok: 0, conflict: 0}, id0}, fn booking, {acc, id} ->
        case do_insert(%{booking | id: id}) do
          :ok -> {Map.update!(acc, :ok, &(&1 + 1)), id + 1}
          {:error, :conflict} -> {Map.update!(acc, :conflict, &(&1 + 1)), id}
        end
      end)

    {:reply, counts, %{state | next_id: next_id}}
  end

  def handle_call({:cancel, id}, _from, state) do
    case :ets.lookup(@bookings, id) do
      [{^id, %Booking{room_id: room, starts_at: s}}] ->
        :ets.delete(@index, {room, s, id})
        :ets.delete(@bookings, id)
        {:reply, :ok, state}

      [] ->
        {:reply, {:error, :not_found}, state}
    end
  end

  ## Funções privadas

  defp do_insert(%Booking{id: id, room_id: room, starts_at: s, ends_at: e} = booking) do
    if conflicts?(room, s, e) do
      {:error, :conflict}
    else
      :ets.insert(@index, {{room, s, id}, e})
      :ets.insert(@bookings, {id, booking})
      :ok
    end
  end

  # Sobreposição de intervalos semiabertos: existente.start < novo.end AND existente.end > novo.start
  defp conflicts?(room, s, e) do
    match_spec = [{{{room, :"$1", :_}, :"$2"}, [{:<, :"$1", e}, {:>, :"$2", s}], [true]}]
    :ets.select_count(@index, match_spec) > 0
  end

  defp collect(:"$end_of_table", _limit, acc), do: Enum.reverse(acc)
  defp collect(_key, 0, acc), do: Enum.reverse(acc)

  defp collect(key, n, acc) do
    case :ets.lookup(@bookings, key) do
      [{^key, booking}] ->
        collect(:ets.next(@bookings, key), n - 1, [booking | acc])

      # Registro removido entre o next e o lookup (leitura concorrente com cancelamento).
      [] ->
        collect(:ets.next(@bookings, key), n, acc)
    end
  end
end

defmodule RoomBooking.Importer do
  @moduledoc """
  Importa reservas de um CSV no formato: room_id,user_id,starts_at,ends_at
  Lê o arquivo como stream e envia em lotes, de modo que a memória seja limitada
  ao tamanho do lote.
  """

  alias RoomBooking.{Booking, FakeDB}

  @spec import_csv(Path.t(), pos_integer()) :: %{ok: non_neg_integer(), conflict: non_neg_integer(), invalid: non_neg_integer()}
  def import_csv(path, batch_size \\ 1_000) do
    path
    |> File.stream!()
    |> Stream.map(&String.trim/1)
    |> Stream.reject(&(&1 == ""))
    |> Stream.map(&parse_line/1)
    |> Stream.chunk_every(batch_size)
    |> Enum.reduce(%{ok: 0, conflict: 0, invalid: 0}, fn chunk, acc ->
      {valid, invalid} = Enum.split_with(chunk, &match?({:ok, _}, &1))
      bookings = Enum.map(valid, fn {:ok, booking} -> booking end)

      %{ok: ok, conflict: conflict} = FakeDB.insert_batch(bookings)

      %{
        acc
        | ok: acc.ok + ok,
          conflict: acc.conflict + conflict,
          invalid: acc.invalid + length(invalid)
      }
    end)
  end

  defp parse_line(line) do
    with [room, user, s, e] <- String.split(line, ",", parts: 4),
         {starts_at, ""} <- Integer.parse(String.trim(s)),
         {ends_at, ""} <- Integer.parse(String.trim(e)),
         {:ok, booking} <- Booking.new(String.trim(room), String.trim(user), starts_at, ends_at) do
      {:ok, booking}
    else
      _ -> {:error, :invalid}
    end
  end
end

defmodule RoomBooking do
  @moduledoc """
  Fachada pública do gerenciamento de reservas de salas.
  """

  alias RoomBooking.{Booking, FakeDB}

  @doc "Tenta reservar uma sala. Retorna {:error, :conflict} se o horário estiver ocupado."
  def book(room_id, user_id, starts_at, ends_at) do
    with {:ok, booking} <- Booking.new(room_id, user_id, starts_at, ends_at) do
      FakeDB.insert_if_available(booking)
    end
  end

  def cancel(id), do: FakeDB.cancel(id)

  @doc """
  Soma os segundos reservados de uma sala, percorrendo as reservas como stream.
  Não materializa a lista de reservas em memória.
  """
  def utilization_seconds(room_id) do
    FakeDB.stream_all()
    |> Stream.filter(&(&1.room_id == room_id))
    |> Enum.reduce(0, fn %Booking{starts_at: s, ends_at: e}, acc -> acc + (e - s) end)
  end
end

defmodule RoomBooking.Demo do
  @moduledoc "Demonstração de concorrência e de importação em grande volume."

  alias RoomBooking.Importer

  def run do
    {:ok, _} = RoomBooking.FakeDB.start_link()

    IO.inspect(concurrency_check(200), label: "Concorrência (esperado: 1 ok, restante conflito)")

    path = Path.join(System.tmp_dir!(), "reservas_#{System.unique_integer([:positive])}.csv")

    # Gera um CSV grande por stream, sem montar o conteúdo em memória.
    1..10_000
    |> Stream.map(fn i ->
      start = i * 3_600
      "sala-#{rem(i, 20)},user-#{i},#{start},#{start + 1_800}\n"
    end)
    |> Stream.into(File.stream!(path))
    |> Stream.run()

    IO.inspect(Importer.import_csv(path, 1_000), label: "Importação em lote")
    IO.inspect(RoomBooking.utilization_seconds("sala-1"), label: "Segundos reservados em sala-1")

    File.rm!(path)
  end

  # Muitas requisições disputam o mesmo horário ao mesmo tempo.
  defp concurrency_check(n) do
    results =
      1..n
      |> Task.async_stream(
        fn i -> RoomBooking.book("sala-concorrente", "user-#{i}", 1_000, 2_000) end,
        max_concurrency: System.schedulers_online() * 2,
        timeout: :infinity
      )
      |> Enum.map(fn {:ok, result} -> result end)

    %{
      ok: Enum.count(results, &match?({:ok, _}, &1)),
      conflict: Enum.count(results, &match?({:error, :conflict}, &1))
    }
  end
end

# Equivalente opcional com Ecto (não obrigatório, não executado por este arquivo):
#
# defmodule MyApp.Repo.Migrations.CreateBookings do
#   use Ecto.Migration
#
#   def change do
#     create table(:bookings) do
#       add :room_id, :string, null: false
#       add :user_id, :string, null: false
#       add :starts_at, :bigint, null: false
#       add :ends_at, :bigint, null: false
#       timestamps()
#     end
#
#     create index(:bookings, [:room_id, :starts_at])
#
#     # Com PostgreSQL, a garantia de não sobreposição fica no próprio banco:
#     # execute "CREATE EXTENSION IF NOT EXISTS btree_gist"
#     # execute \"\"\"
#     # ALTER TABLE bookings ADD CONSTRAINT no_overlap EXCLUDE USING gist (
#     #   room_id WITH =, int8range(starts_at, ends_at) WITH &&
#     # )
#     # \"\"\"
#   end
# end

RoomBooking.Demo.run()
