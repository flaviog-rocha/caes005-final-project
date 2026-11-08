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

defmodule RoomBooking do
  @moduledoc """
  Gerenciamento de reservas de salas.

  Recursos:
    * Operações concorrentes, com limite configurável de simultaneidade (`RoomBooking.Limiter`).
    * Reservas por faixa de horas, com verificação atômica de conflitos via `:ets.insert_new/2`.
    * Persistência em um adaptador de banco fictício (`RoomBooking.Repo`), padrão `RoomBooking.FakeDB`.
      Um adaptador baseado em Ecto (com `Ecto.Migration`) poderia implementar o mesmo behaviour.
    * Histórico de operações em ETS (`RoomBooking.History`).
    * Notificações quando condições são atingidas (`RoomBooking.Notifier`).

  Uso básico:

      {:ok, _} = RoomBooking.start_link(max_concurrency: 8, occupancy_threshold: 75)
      RoomBooking.subscribe()
      {:ok, res} = RoomBooking.book("sala-1", "ana", 9, 11)
      {:error, {:conflict, 9}} = RoomBooking.book("sala-1", "bruno", 10, 12)
      :ok = RoomBooking.cancel(res.id)
  """

  alias RoomBooking.{Limiter, Notifier, History}

  # ---------------------------------------------------------------------------
  # Supervisão
  # ---------------------------------------------------------------------------

  @doc "Inicia o sistema de reservas. Opções: `:max_concurrency`, `:occupancy_threshold` (0..100) e `:adapter`."
  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    max = Keyword.get(opts, :max_concurrency, System.schedulers_online())

    children = [
      # Owner das tabelas ETS: deve iniciar antes de qualquer operação.
      {RoomBooking.Tables, opts},
      # Controle de concorrência.
      {Limiter, max}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  # ---------------------------------------------------------------------------
  # API pública (todas passam pelo limitador de concorrência)
  # ---------------------------------------------------------------------------

  @doc "Reserva a sala no intervalo `[from_hour, to_hour)`, com horas inteiras entre 0 e 24."
  def book(room, user, from_hour, to_hour) do
    Limiter.run(fn -> do_book(room, user, from_hour, to_hour) end)
  end

  @doc "Cancela uma reserva pelo id, liberando suas horas."
  def cancel(id) do
    Limiter.run(fn -> do_cancel(id) end)
  end

  @doc "Lista as horas livres (0..23) de uma sala."
  def free_hours(room) do
    Limiter.run(fn ->
      for h <- 0..23, not :ets.member(:rb_slots, {room, h}), do: h
    end)
  end

  @doc "Lista todas as reservas ativas."
  def reservations do
    Limiter.run(fn ->
      :ets.select(:rb_reservations, [{{:_, :"$1"}, [], [:"$1"]}])
    end)
  end

  @doc "Retorna as últimas `limit` entradas do histórico (mais recentes primeiro)."
  def history(limit \\ 50) do
    Limiter.run(fn -> History.list(limit) end)
  end

  @doc "Recarrega o índice ETS a partir do adaptador de banco."
  def restore do
    Limiter.run(fn ->
      adapter = adapter()

      Enum.each(adapter.all_reservations(), fn res ->
        :ets.insert(:rb_reservations, {res.id, res})

        Enum.each(res.from..(res.to - 1), fn h ->
          :ets.insert(:rb_slots, {{res.room, h}, res.id})
        end)
      end)

      :ok
    end)
  end

  @doc "Inscreve um processo para receber notificações `{:room_booking_notification, datetime, evento}`."
  def subscribe(pid \\ self()), do: Notifier.subscribe(pid)

  @doc "Cancela a inscrição de notificações."
  def unsubscribe(pid \\ self()), do: Notifier.unsubscribe(pid)

  # ---------------------------------------------------------------------------
  # Implementação interna
  # ---------------------------------------------------------------------------

  defp do_book(room, user, from, to)
       when is_integer(from) and is_integer(to) and from >= 0 and to <= 24 and from < to do
    id = :erlang.unique_integer([:positive])
    hours = Enum.to_list(from..(to - 1))

    case claim(room, hours, id) do
      :ok ->
        res = %{id: id, room: room, user: user, from: from, to: to, at: DateTime.utc_now()}
        adapter().persist_reservation(res)
        :ets.insert(:rb_reservations, {id, res})
        History.record(:book, res, :ok)
        check_occupancy(room)
        {:ok, res}

      {:error, hour} ->
        details = %{room: room, user: user, from: from, to: to}
        History.record(:book, details, {:error, :conflict})
        {:error, {:conflict, hour}}
    end
  end

  defp do_book(_room, _user, _from, _to), do: {:error, :invalid_range}

  # Reserva atomicamente cada hora; em caso de conflito, desfaz apenas o que foi inserido aqui.
  defp claim(room, hours, id) do
    result =
      Enum.reduce_while(hours, [], fn h, acc ->
        if :ets.insert_new(:rb_slots, {{room, h}, id}) do
          {:cont, [h | acc]}
        else
          Enum.each(acc, &:ets.delete_object(:rb_slots, {{room, &1}, id}))
          {:halt, {:error, h}}
        end
      end)

    case result do
      {:error, _} = err -> err
      _acumulado -> :ok
    end
  end

  defp do_cancel(id) do
    case :ets.lookup(:rb_reservations, id) do
      [{^id, res}] ->
        Enum.each(res.from..(res.to - 1), fn h ->
          :ets.delete_object(:rb_slots, {{res.room, h}, id})
        end)

        :ets.delete(:rb_reservations, id)
        adapter().delete_reservation(id)
        History.record(:cancel, res, :ok)
        :ok

      [] ->
        History.record(:cancel, %{id: id}, {:error, :not_found})
        {:error, :not_found}
    end
  end

  # Notifica quando a ocupação diária da sala atinge o limite configurado.
  defp check_occupancy(room) do
    booked = :ets.select_count(:rb_slots, [{{{room, :_}, :_}, [], [true]}])
    pct = div(booked * 100, 24)
    threshold = :ets.lookup_element(:rb_config, :occupancy_threshold, 2)

    if pct >= threshold do
      Notifier.emit({:room_occupancy_high, room, pct})
    end

    :ok
  end

  defp adapter, do: :ets.lookup_element(:rb_config, :adapter, 2)
end

defmodule RoomBooking.Tables do
  @moduledoc "Dono de todas as tabelas ETS do sistema. Se este processo cair, as tabelas são recriadas no restart."
  use GenServer

  @table_opts [:named_table, :public, read_concurrency: true, write_concurrency: true]

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  @impl true
  def init(opts) do
    # Índice de slots: {{sala, hora}, id_reserva}. Set garante unicidade via insert_new.
    :ets.new(:rb_slots, [:set | @table_opts])
    # Reservas ativas: {id, reserva}.
    :ets.new(:rb_reservations, [:set | @table_opts])
    # Histórico ordenado por sequência monotônica.
    :ets.new(:rb_history, [:ordered_set | @table_opts])
    # Assinantes de notificações: {:all, pid}.
    :ets.new(:rb_subscribers, [:bag | @table_opts])
    # Banco fictício: {id, reserva}.
    :ets.new(:rb_fake_db, [:set | @table_opts])
    # Configurações da aplicação.
    :ets.new(:rb_config, [:set | @table_opts])

    :ets.insert(:rb_config, {:occupancy_threshold, Keyword.get(opts, :occupancy_threshold, 75)})
    :ets.insert(:rb_config, {:adapter, Keyword.get(opts, :adapter, RoomBooking.FakeDB)})

    {:ok, nil}
  end
end

defmodule RoomBooking.Limiter do
  @moduledoc """
  Semáforo de contagem: no máximo `max` operações executam simultaneamente.
  Os demais chamadores aguardam em fila FIFO. Cada permissão é monitorada,
  então se o processo detentor morrer ou expirar o timeout, a permissão é liberada.
  Não é reentrante: não chame `run/2` de dentro de outro `run/2` do mesmo processo.
  """
  use GenServer

  def start_link(max) when is_integer(max) and max > 0 do
    GenServer.start_link(__MODULE__, max, name: __MODULE__)
  end

  @doc "Executa `fun` respeitando o limite de concorrência."
  def run(fun, timeout \\ 5_000) when is_function(fun, 0) do
    try do
      :ok = GenServer.call(__MODULE__, :acquire, timeout)
    catch
      :exit, reason ->
        # Remove este processo da fila caso o timeout tenha ocorrido.
        GenServer.cast(__MODULE__, {:release, self()})
        exit(reason)
    end

    try do
      fun.()
    after
      GenServer.cast(__MODULE__, {:release, self()})
    end
  end

  @impl true
  def init(max) do
    {:ok, %{max: max, refs: %{}, active: MapSet.new(), waiting: :queue.new()}}
  end

  @impl true
  def handle_call(:acquire, {pid, _} = from, state) do
    ref = Process.monitor(pid)
    state = %{state | refs: Map.put(state.refs, pid, ref)}

    if MapSet.size(state.active) < state.max do
      {:reply, :ok, %{state | active: MapSet.put(state.active, pid)}}
    else
      waiting = :queue.in({from, pid}, state.waiting)
      RoomBooking.Notifier.emit({:concurrency_limit_reached, :queue.len(waiting)})
      {:noreply, %{state | waiting: waiting}}
    end
  end

  @impl true
  def handle_cast({:release, pid}, state), do: {:noreply, drop(state, pid)}

  @impl true
  def handle_info({:DOWN, _ref, :process, pid, _reason}, state), do: {:noreply, drop(state, pid)}

  defp drop(state, pid) do
    case Map.pop(state.refs, pid) do
      {nil, _} ->
        state

      {ref, refs} ->
        Process.demonitor(ref, [:flush])

        waiting = :queue.filter(fn {_from, p} -> p != pid end, state.waiting)

        state = %{
          state
          | refs: refs,
            active: MapSet.delete(state.active, pid),
            waiting: waiting
        }

        grant(state)
    end
  end

  defp grant(state) do
    if MapSet.size(state.active) < state.max do
      case :queue.out(state.waiting) do
        {{:value, {from, pid}}, rest} ->
          GenServer.reply(from, :ok)
          grant(%{state | active: MapSet.put(state.active, pid), waiting: rest})

        {:empty, _} ->
          state
      end
    else
      state
    end
  end
end

defmodule RoomBooking.Notifier do
  @moduledoc """
  Notificações por mensagem de processo. Eventos emitidos:
    * `{:room_occupancy_high, sala, percentual}`
    * `{:concurrency_limit_reached, tamanho_da_fila}`
  """

  def subscribe(pid \\ self()) do
    :ets.insert(:rb_subscribers, {:all, pid})
    :ok
  end

  def unsubscribe(pid \\ self()) do
    :ets.delete_object(:rb_subscribers, {:all, pid})
    :ok
  end

  def emit(event) do
    payload = {:room_booking_notification, DateTime.utc_now(), event}

    for {:all, pid} <- :ets.lookup(:rb_subscribers, :all) do
      # Remove assinantes locais que já terminaram.
      if node(pid) == node() and not Process.alive?(pid) do
        :ets.delete_object(:rb_subscribers, {:all, pid})
      else
        send(pid, payload)
      end
    end

    :ok
  end
end

defmodule RoomBooking.History do
  @moduledoc "Histórico de operações armazenado em ETS (`:ordered_set`)."

  def record(op, details, result) do
    seq = :erlang.unique_integer([:monotonic, :positive])
    :ets.insert(:rb_history, {seq, DateTime.utc_now(), op, details, result})
    :ok
  end

  def list(limit \\ 50) do
    collect(:ets.last(:rb_history), limit, [])
  end

  defp collect(:"$end_of_table", _n, acc), do: acc
  defp collect(_key, 0, acc), do: acc

  defp collect(key, n, acc) do
    [{_seq, at, op, details, result}] = :ets.lookup(:rb_history, key)
    entry = %{at: at, op: op, details: details, result: result}
    collect(:ets.prev(:rb_history, key), n - 1, acc ++ [entry])
  end
end

defmodule RoomBooking.Repo do
  @moduledoc """
  Behaviour do adaptador de persistência. Pode ser implementado com Ecto
  (usando `Ecto.Migration` para criar a tabela de reservas) sem alterar o restante do sistema.
  """
  @callback persist_reservation(map()) :: :ok | {:error, term()}
  @callback delete_reservation(id :: integer()) :: :ok | {:error, term()}
  @callback all_reservations() :: [map()]
end

defmodule RoomBooking.FakeDB do
  @moduledoc "Banco fictício em memória, implementado sobre ETS."
  @behaviour RoomBooking.Repo

  @impl true
  def persist_reservation(res) do
    :ets.insert(:rb_fake_db, {res.id, res})
    :ok
  end

  @impl true
  def delete_reservation(id) do
    :ets.delete(:rb_fake_db, id)
    :ok
  end

  @impl true
  def all_reservations do
    :ets.select(:rb_fake_db, [{{:_, :"$1"}, [], [:"$1"]}])
  end
end
