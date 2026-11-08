# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de reservas de salas, atendendo somente aos seguintes requisitos:
#
# 1. Permitir a execução de operações simultâneas.
# 2. Quando fizer sentido para o domínio, considerar a comunicação com um banco de dados fictício e, opcionalmente, o uso de Ecto.Migration, sem tornar isso obrigatório.
# 3. Permitir limitar a quantidade de operações simultâneas.
# 4. Manter um histórico das operações realizadas.
# 5. Gerar notificações quando determinadas condições forem atingidas.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - ETS
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# Gerenciamento de reservas de salas com concorrência controlada, histórico e notificações.
# Arquivo único e autocontido, usando apenas a biblioteca padrão (ETS, GenServer, Supervisor,
# :global, Date/DateTime). Ecto não é necessário; o "banco" é um adaptador fictício atrás de
# um behaviour, que poderia ser substituído por um Ecto.Repo sem alterar a API pública.

defmodule RoomBooking do
  @moduledoc """
  API pública do sistema de reservas de salas.

  - Operações executam concorrentemente (cada chamada roda no processo do chamador).
  - O número de operações simultâneas é limitado por `RoomBooking.Limiter`.
  - Todas as operações são registradas em histórico (tabela ETS ordenada).
  - Notificações são geradas quando condições de ocupação são atingidas.
  - Conflitos de horário são evitados com um lock por sala (`:global.trans`),
    garantindo que a verificação de conflito e a escrita sejam atômicas.

  Horário de funcionamento: 08h às 18h (intervalos de horas inteiras).
  """

  alias RoomBooking.{Store, Limiter, History, Notifier}

  # Adaptador de persistência (fictício). Pode ser trocado por um módulo que implemente
  # o behaviour RoomBooking.Repo (por exemplo, um Ecto.Repo com migrations).
  @repo RoomBooking.FakeDB

  @opening_hour 8
  @closing_hour 18
  @open_hours @closing_hour - @opening_hour
  # Limite de alerta de "quase lotada": 80% de 10 horas = 8 horas.
  @alert_hours 8

  @doc """
  Inicia o sistema. Opções:
    * `:max_concurrency` - máximo de operações simultâneas (padrão: 4)
    * `:queue_alert` - tamanho da fila de espera que dispara notificação (padrão: 10)
  """
  def start_link(opts \\ []) do
    children = [
      {Store, []},
      {Limiter,
       max_concurrency: Keyword.get(opts, :max_concurrency, 4),
       queue_alert: Keyword.get(opts, :queue_alert, 10)}
    ]

    Supervisor.start_link(children, strategy: :one_for_all, name: :room_booking_sup)
  end

  @doc "Cadastra uma sala."
  def create_room(room_id, name) do
    execute(:create_room, %{room_id: room_id, name: name}, fn ->
      room = %{id: room_id, name: name}

      if :ets.insert_new(:rb_rooms, {room_id, room}) do
        {:ok, room}
      else
        {:error, :already_exists}
      end
    end)
  end

  @doc "Reserva a sala no intervalo [from, to) de uma data."
  def book(room_id, user, %Date{} = date, from, to) do
    args = %{room_id: room_id, user: user, date: date, from: from, to: to}

    execute(:book, args, fn ->
      with :ok <- validate_range(from, to),
           :ok <- room_exists(room_id) do
        # Serializa apenas as reservas da MESMA sala; salas diferentes não se bloqueiam.
        locked(room_id, fn -> do_book(room_id, user, date, from, to) end)
      end
    end)
  end

  @doc "Cancela uma reserva pelo id."
  def cancel(reservation_id) do
    execute(:cancel, %{reservation_id: reservation_id}, fn ->
      case :ets.lookup(:rb_reservations_by_id, reservation_id) do
        [{_, %{room_id: room_id}}] ->
          locked(room_id, fn -> do_cancel(reservation_id) end)

        [] ->
          {:error, :not_found}
      end
    end)
  end

  @doc "Lista as reservas ativas de uma sala em uma data."
  def reservations(room_id, %Date{} = date) do
    execute(:list_reservations, %{room_id: room_id, date: date}, fn ->
      list =
        :ets.lookup(:rb_reservations_by_day, {room_id, date})
        |> Enum.map(fn {_, r} -> r end)
        |> Enum.sort_by(& &1.from)

      {:ok, list}
    end)
  end

  @doc "Retorna as últimas `n` operações do histórico (mais recentes primeiro)."
  def history(n \\ 50), do: History.recent(n)

  @doc "Retorna as últimas `n` notificações (mais recentes primeiro)."
  def notifications(n \\ 50), do: Notifier.recent(n)

  @doc "Inscreve o processo atual para receber notificações como `{:room_booking, notif}`."
  def subscribe, do: Notifier.subscribe()

  @doc "Estatísticas do limitador de concorrência."
  def stats, do: Limiter.stats()

  # ---------------------------------------------------------------------------
  # Operações internas (executadas sob o lock da sala)
  # ---------------------------------------------------------------------------

  defp do_book(room_id, user, date, from, to) do
    key = {room_id, date}

    existing =
      :ets.lookup(:rb_reservations_by_day, key)
      |> Enum.map(fn {_, r} -> r end)

    if Enum.any?(existing, &overlaps?(&1, from, to)) do
      {:error, :conflict}
    else
      booked_before = Enum.reduce(existing, 0, fn r, acc -> acc + (r.to - r.from) end)

      res = %{
        id: next_id(),
        room_id: room_id,
        user: user,
        date: date,
        from: from,
        to: to,
        status: :confirmed,
        created_at: DateTime.utc_now()
      }

      # Persiste no "banco" antes de atualizar o índice em ETS.
      with :ok <- @repo.insert_reservation(res) do
        :ets.insert(:rb_reservations_by_day, {key, res})
        :ets.insert(:rb_reservations_by_id, {res.id, res})
        check_occupancy(room_id, date, booked_before, booked_before + (to - from))
        {:ok, res}
      end
    end
  end

  defp do_cancel(reservation_id) do
    case :ets.lookup(:rb_reservations_by_id, reservation_id) do
      [{_, res}] ->
        with :ok <- @repo.update_reservation(reservation_id, %{status: :cancelled}) do
          :ets.delete(:rb_reservations_by_id, reservation_id)
          :ets.delete_object(:rb_reservations_by_day, {{res.room_id, res.date}, res})
          {:ok, %{res | status: :cancelled}}
        end

      [] ->
        {:error, :not_found}
    end
  end

  # ---------------------------------------------------------------------------
  # Notificações de ocupação (disparadas por cruzamento de limiar)
  # ---------------------------------------------------------------------------

  defp check_occupancy(room_id, date, before, after_booked) do
    if before < @alert_hours and after_booked >= @alert_hours do
      Notifier.emit(:room_near_capacity, %{
        room_id: room_id,
        date: date,
        booked_hours: after_booked
      })
    end

    if before < @open_hours and after_booked >= @open_hours do
      Notifier.emit(:room_fully_booked, %{room_id: room_id, date: date})
    end

    :ok
  end

  # ---------------------------------------------------------------------------
  # Utilitários
  # ---------------------------------------------------------------------------

  # Executa a operação pelo limitador de concorrência e registra no histórico.
  defp execute(op, args, fun) do
    result =
      try do
        Limiter.run(fun)
      rescue
        e -> {:error, {:exception, Exception.message(e)}}
      end

    History.record(op, args, result)
    result
  end

  defp validate_range(from, to)
       when is_integer(from) and is_integer(to) and from >= @opening_hour and
              to <= @closing_hour and from < to,
       do: :ok

  defp validate_range(_, _), do: {:error, :invalid_range}

  defp room_exists(room_id) do
    case :ets.lookup(:rb_rooms, room_id) do
      [_] -> :ok
      [] -> {:error, :room_not_found}
    end
  end

  # Lock distribuído nativo da OTP, identificado por sala.
  defp locked(room_id, fun) do
    :global.trans({{__MODULE__, room_id}, self()}, fun, [node()], :infinity)
  end

  defp overlaps?(r, from, to), do: r.from < to and from < r.to

  defp next_id, do: :erlang.unique_integer([:positive, :monotonic])
end

# -----------------------------------------------------------------------------
# Dono das tabelas ETS. Mantém as tabelas vivas enquanto o sistema estiver ativo.
# -----------------------------------------------------------------------------
defmodule RoomBooking.Store do
  @moduledoc false
  use GenServer

  @tables [
    # id_sala => sala
    {:rb_rooms, :set},
    # {room_id, date} => reserva (bag para múltiplas reservas por dia)
    {:rb_reservations_by_day, :bag},
    # id_reserva => reserva
    {:rb_reservations_by_id, :set},
    # histórico de operações, ordenado por id monotônico
    {:rb_history, :ordered_set},
    # notificações, ordenadas por id monotônico
    {:rb_notifications, :ordered_set},
    # pids inscritos para receber notificações
    {:rb_subscribers, :bag},
    # tabela que simula o banco de dados fictício
    {:rb_fake_db, :set}
  ]

  def start_link(_opts), do: GenServer.start_link(__MODULE__, nil, name: __MODULE__)

  @impl true
  def init(_) do
    Enum.each(@tables, fn {name, type} ->
      :ets.new(name, [
        type,
        :named_table,
        :public,
        read_concurrency: true,
        write_concurrency: true
      ])
    end)

    {:ok, nil}
  end
end

# -----------------------------------------------------------------------------
# Limitador de concorrência: no máximo N operações simultâneas.
# Demais chamadas aguardam em fila. Tokens são monitores do processo chamador,
# então, se o chamador morrer, a vaga é liberada automaticamente.
# -----------------------------------------------------------------------------
defmodule RoomBooking.Limiter do
  @moduledoc false
  use GenServer

  alias RoomBooking.Notifier

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  @doc "Executa `fun` respeitando o limite de operações simultâneas."
  def run(fun) when is_function(fun, 0) do
    {:ok, token} = GenServer.call(__MODULE__, :acquire, :infinity)

    try do
      fun.()
    after
      GenServer.cast(__MODULE__, {:release, token})
    end
  end

  def stats, do: GenServer.call(__MODULE__, :stats)

  @impl true
  def init(opts) do
    state = %{
      max: Keyword.fetch!(opts, :max_concurrency),
      alert: Keyword.get(opts, :queue_alert, 10),
      active: MapSet.new(),
      waiting: :queue.new()
    }

    {:ok, state}
  end

  @impl true
  def handle_call(:acquire, {pid, _} = from, state) do
    token = Process.monitor(pid)

    if MapSet.size(state.active) < state.max do
      {:reply, {:ok, token}, %{state | active: MapSet.put(state.active, token)}}
    else
      waiting = :queue.in({from, token}, state.waiting)
      len = :queue.len(waiting)

      # Notifica ao atingir o limiar de fila (evita spam: dispara apenas na transição).
      if len == state.alert do
        Notifier.emit(:limiter_saturated, %{waiting: len, max: state.max})
      end

      {:noreply, %{state | waiting: waiting}}
    end
  end

  def handle_call(:stats, _from, state) do
    stats = %{
      active: MapSet.size(state.active),
      waiting: :queue.len(state.waiting),
      max: state.max
    }

    {:reply, stats, state}
  end

  @impl true
  def handle_cast({:release, token}, state) do
    Process.demonitor(token, [:flush])
    state = %{state | active: MapSet.delete(state.active, token)}
    {:noreply, dispatch(state)}
  end

  @impl true
  def handle_info({:DOWN, token, :process, _pid, _reason}, state) do
    if MapSet.member?(state.active, token) do
      {:noreply, dispatch(%{state | active: MapSet.delete(state.active, token)})}
    else
      # Chamador em espera morreu: remove da fila.
      waiting = :queue.filter(fn {_from, t} -> t != token end, state.waiting)
      {:noreply, %{state | waiting: waiting}}
    end
  end

  # Concede vagas livres aos próximos da fila.
  defp dispatch(state) do
    if MapSet.size(state.active) < state.max and not :queue.is_empty(state.waiting) do
      {{:value, {from, token}}, waiting} = :queue.out(state.waiting)
      GenServer.reply(from, {:ok, token})
      dispatch(%{state | active: MapSet.put(state.active, token), waiting: waiting})
    else
      state
    end
  end
end

# -----------------------------------------------------------------------------
# Histórico de operações (ETS ordered_set).
# -----------------------------------------------------------------------------
defmodule RoomBooking.History do
  @moduledoc false

  def record(op, args, result) do
    id = :erlang.unique_integer([:positive, :monotonic])

    entry = %{
      id: id,
      at: DateTime.utc_now(),
      op: op,
      args: args,
      result: result,
      pid: self()
    }

    :ets.insert(:rb_history, {id, entry})
    entry
  end

  def recent(n \\ 50), do: RoomBooking.Util.recent(:rb_history, n)
end

# -----------------------------------------------------------------------------
# Notificações: armazenadas em ETS e entregues a processos inscritos.
# -----------------------------------------------------------------------------
defmodule RoomBooking.Notifier do
  @moduledoc false

  def subscribe(pid \\ self()) do
    :ets.insert(:rb_subscribers, {pid})
    :ok
  end

  def unsubscribe(pid \\ self()) do
    :ets.delete_object(:rb_subscribers, {pid})
    :ok
  end

  def emit(type, data) do
    id = :erlang.unique_integer([:positive, :monotonic])
    notif = %{id: id, type: type, data: data, at: DateTime.utc_now()}

    :ets.insert(:rb_notifications, {id, notif})

    for {pid} <- :ets.tab2list(:rb_subscribers) do
      send(pid, {:room_booking, notif})
    end

    notif
  end

  def recent(n \\ 50), do: RoomBooking.Util.recent(:rb_notifications, n)
end

# -----------------------------------------------------------------------------
# Behaviour de persistência e adaptador fictício.
# Para usar Ecto no futuro, basta implementar este behaviour com um Ecto.Repo
# e ajustar @repo em RoomBooking. Migrations não são obrigatórias aqui.
# -----------------------------------------------------------------------------
defmodule RoomBooking.Repo do
  @moduledoc false

  @callback insert_reservation(map()) :: :ok | {:error, term()}
  @callback update_reservation(id :: integer(), changes :: map()) :: :ok | {:error, term()}
  @callback list_reservations() :: [map()]
end

defmodule RoomBooking.FakeDB do
  @moduledoc false
  @behaviour RoomBooking.Repo

  @table :rb_fake_db
  # Latência simulada de I/O do banco, em milissegundos.
  @latency_ms 5

  @impl true
  def insert_reservation(res) do
    Process.sleep(@latency_ms)
    :ets.insert(@table, {res.id, res})
    :ok
  end

  @impl true
  def update_reservation(id, changes) do
    Process.sleep(@latency_ms)

    case :ets.lookup(@table, id) do
      [{^id, res}] ->
        :ets.insert(@table, {id, Map.merge(res, changes)})
        :ok

      [] ->
        {:error, :not_found}
    end
  end

  @impl true
  def list_reservations do
    for {_id, res} <- :ets.tab2list(@table), do: res
  end
end

# -----------------------------------------------------------------------------
# Utilitário: lê os últimos N registros de uma ordered_set (mais recentes primeiro).
# -----------------------------------------------------------------------------
defmodule RoomBooking.Util do
  @moduledoc false

  def recent(table, n) do
    Stream.unfold(:ets.last(table), fn
      :"$end_of_table" -> nil
      key -> {key, :ets.prev(table, key)}
    end)
    |> Enum.take(n)
    |> Enum.flat_map(fn key ->
      for {_, v} <- :ets.lookup(table, key), do: v
    end)
  end
end

# -----------------------------------------------------------------------------
# Demonstração opcional: RoomBooking.Demo.run()
# -----------------------------------------------------------------------------
defmodule RoomBooking.Demo do
  @moduledoc false

  def run do
    {:ok, _} = RoomBooking.start_link(max_concurrency: 3, queue_alert: 5)
    RoomBooking.subscribe()
    RoomBooking.create_room("sala-1", "Sala Alfa")

    date = Date.utc_today()

    # 20 reservas concorrentes, com algumas sobreposições propositais.
    results =
      1..20
      |> Task.async_stream(
        fn i ->
          from = 8 + rem(i, 9)
          RoomBooking.book("sala-1", "user-#{i}", date, from, from + 1)
        end,
        max_concurrency: 20,
        timeout: :infinity
      )
      |> Enum.map(fn {:ok, r} -> r end)

    IO.inspect(Enum.frequencies_by(results, &elem(&1, 0)), label: "resultados")
    IO.inspect(RoomBooking.stats(), label: "limitador")

    collect = fn f, acc ->
      receive do
        {:room_booking, n} -> f.(f, [n.type | acc])
      after
        200 -> Enum.reverse(acc)
      end
    end

    IO.inspect(collect.(collect, []), label: "notificações recebidas")
    IO.inspect(length(RoomBooking.history()), label: "operações no histórico")
  end
end
