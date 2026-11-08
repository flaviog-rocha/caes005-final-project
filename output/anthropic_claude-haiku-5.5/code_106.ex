# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de reservas de salas, atendendo somente aos seguintes requisitos:
#
# 1. Manter um histórico das operações realizadas.
# 2. Recuperar processos após falhas inesperadas.
# 3. Implementar expiração automática de registros.
# 4. Impedir atualizações parcialmente concluídas.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - Task
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# Gerenciamento de reservas de salas em Elixir.
# Arquivo autocontido. Executar com: elixir room_booking.exs
#
# Requisitos atendidos:
#   1. Histórico: cada operação bem-sucedida gera uma entrada no histórico persistido.
#   2. Recuperação: o Store roda sob supervisão (:permanent). Após uma falha, é reiniciado,
#      recarrega o último estado consistente do DETS e reagenda as expirações pendentes.
#   3. Expiração: reservas pendentes expiram automaticamente via Task. Cada expiração
#      é agendada por uma Task supervisionada.
#   4. Atomicidade: toda operação é calculada sobre uma cópia do estado (função pura).
#      Só após a persistência bem-sucedida do novo estado (escrita única no DETS) ele
#      substitui o estado em memória. Falhas deixam o estado anterior intacto.

defmodule RoomBooking.Store do
  @moduledoc false
  use GenServer

  @default_hold_ms 60_000
  @state_key :state
  @task_sup RoomBooking.TaskSupervisor

  # ---------------------------------------------------------------------------
  # API pública
  # ---------------------------------------------------------------------------

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))
  end

  def child_spec(opts) do
    %{
      id: Keyword.get(opts, :name, __MODULE__),
      start: {__MODULE__, :start_link, [opts]},
      restart: :permanent,
      type: :worker
    }
  end

  # Cria uma reserva pendente. Expira em `hold_ms` se não for confirmada.
  def reserve(server, room, starts_at, ends_at, user, opts \\ []) do
    hold = Keyword.get(opts, :hold_ms, @default_hold_ms)
    GenServer.call(server, {:reserve, room, starts_at, ends_at, user, hold})
  end

  # Confirma uma reserva pendente (remove a expiração).
  def confirm(server, id), do: GenServer.call(server, {:confirm, id})

  # Cancela uma reserva.
  def cancel(server, id), do: GenServer.call(server, {:cancel, id})

  # Altera o intervalo de uma reserva de forma atômica.
  def reschedule(server, id, starts_at, ends_at),
    do: GenServer.call(server, {:reschedule, id, starts_at, ends_at})

  def get(server, id), do: GenServer.call(server, {:get, id})
  def list(server), do: GenServer.call(server, :list)
  def history(server), do: GenServer.call(server, :history)

  # ---------------------------------------------------------------------------
  # Callbacks do GenServer
  # ---------------------------------------------------------------------------

  @impl true
  def init(opts) do
    table = Keyword.get(opts, :table, :room_booking_dets)
    file = opts |> Keyword.get(:file, "room_booking.dets") |> String.to_charlist()

    # O DETS é persistente em disco: sobrevive a falhas do processo.
    {:ok, ^table} = :dets.open_file(table, file: file, type: :set)

    now = now()
    {data, existed?} = load(table)

    # Se já havia estado em disco, esta inicialização é uma recuperação.
    data = if existed?, do: append_history(data, :recovered, %{}), else: data
    :ok = persist(table, data)

    # Reagenda as expirações de reservas pendentes (as Tasks anteriores morreram).
    data.reservations
    |> Map.values()
    |> Enum.each(fn r ->
      if r.status == :pending, do: schedule(r.id, r.expires_at - now)
    end)

    {:ok, %{table: table, data: data}}
  end

  @impl true
  def handle_call({:get, id}, _from, st) do
    {:reply, fetch(st.data, id), st}
  end

  def handle_call(:list, _from, st) do
    {:reply, Map.values(st.data.reservations), st}
  end

  def handle_call(:history, _from, st) do
    {:reply, Enum.reverse(st.data.history), st}
  end

  # Operações de escrita: calculadas de forma pura e persistidas em uma única escrita.
  def handle_call(cmd, _from, st) do
    case execute(cmd, st.data) do
      {:ok, new_data, reply, effects} ->
        case persist(st.table, new_data) do
          :ok ->
            # Efeitos colaterais (agendamentos) só ocorrem após a persistência.
            Enum.each(effects, fn {:expire_in, id, ms} -> schedule(id, ms) end)
            {:reply, reply, %{st | data: new_data}}

          {:error, reason} ->
            # Estado em memória permanece o anterior: nenhuma atualização parcial.
            {:reply, {:error, {:persistence_failed, reason}}, st}
        end

      {:error, reason} ->
        {:reply, {:error, reason}, st}
    end
  end

  @impl true
  def handle_cast({:expire, id}, st) do
    now = now()

    case Map.get(st.data.reservations, id) do
      %{status: :pending, expires_at: exp} when exp > now ->
        # Disparo antecipado: reagenda pelo tempo restante.
        schedule(id, exp - now)
        {:noreply, st}

      %{status: :pending} ->
        data = st.data |> remove(id) |> append_history(:expired, %{id: id})

        case persist(st.table, data) do
          :ok ->
            {:noreply, %{st | data: data}}

          {:error, _reason} ->
            # Tenta novamente depois, sem alterar o estado em memória.
            schedule(id, 1_000)
            {:noreply, st}
        end

      _ ->
        # Já confirmada, cancelada ou inexistente.
        {:noreply, st}
    end
  end

  @impl true
  def terminate(_reason, st) do
    :dets.close(st.table)
    :ok
  end

  # ---------------------------------------------------------------------------
  # Operações puras sobre o estado: {:ok, novo_estado, resposta, efeitos} | {:error, motivo}
  # ---------------------------------------------------------------------------

  defp execute({:reserve, room, s, e, user, hold}, data)
       when is_integer(s) and is_integer(e) and s < e and is_integer(hold) and hold > 0 do
    if conflict?(data, room, s, e, nil) do
      {:error, :conflict}
    else
      now = now()
      id = data.seq + 1

      r = %{
        id: id,
        room: room,
        starts_at: s,
        ends_at: e,
        user: user,
        status: :pending,
        created_at: now,
        expires_at: now + hold
      }

      data =
        %{data | seq: id}
        |> put_res(r)
        |> append_history(:reserve, %{id: id, room: room})

      {:ok, data, {:ok, r}, [{:expire_in, id, hold}]}
    end
  end

  defp execute({:reserve, _, _, _, _, _}, _data), do: {:error, :invalid_request}

  defp execute({:confirm, id}, data) do
    now = now()

    case Map.fetch(data.reservations, id) do
      {:ok, %{status: :pending, expires_at: exp} = r} when exp > now ->
        r = %{r | status: :confirmed, expires_at: nil}
        data = data |> put_res(r) |> append_history(:confirm, %{id: id})
        {:ok, data, {:ok, r}, []}

      {:ok, %{status: :pending}} ->
        {:error, :expired}

      {:ok, %{status: :confirmed}} ->
        {:error, :already_confirmed}

      :error ->
        {:error, :not_found}
    end
  end

  defp execute({:cancel, id}, data) do
    if Map.has_key?(data.reservations, id) do
      data = data |> remove(id) |> append_history(:cancel, %{id: id})
      {:ok, data, :ok, []}
    else
      {:error, :not_found}
    end
  end

  defp execute({:reschedule, id, s, e}, data) when is_integer(s) and is_integer(e) and s < e do
    case Map.fetch(data.reservations, id) do
      {:ok, r} ->
        if conflict?(data, r.room, s, e, id) do
          {:error, :conflict}
        else
          r2 = %{r | starts_at: s, ends_at: e}

          data =
            data
            |> put_res(r2)
            |> append_history(:reschedule, %{
              id: id,
              from: {r.starts_at, r.ends_at},
              to: {s, e}
            })

          {:ok, data, {:ok, r2}, []}
        end

      :error ->
        {:error, :not_found}
    end
  end

  defp execute(_cmd, _data), do: {:error, :invalid_request}

  # ---------------------------------------------------------------------------
  # Auxiliares
  # ---------------------------------------------------------------------------

  # Uma reserva é ativa se confirmada ou pendente e ainda dentro do prazo.
  defp active?(%{status: :confirmed}, _now), do: true
  defp active?(%{status: :pending, expires_at: exp}, now), do: exp > now
  defp active?(_, _), do: false

  defp conflict?(data, room, s, e, ignore_id) do
    now = now()

    Enum.any?(data.reservations, fn {id, r} ->
      id != ignore_id and r.room == room and active?(r, now) and
        r.starts_at < e and s < r.ends_at
    end)
  end

  defp fetch(data, id) do
    case Map.fetch(data.reservations, id) do
      {:ok, r} -> {:ok, r}
      :error -> {:error, :not_found}
    end
  end

  defp put_res(data, r), do: %{data | reservations: Map.put(data.reservations, r.id, r)}
  defp remove(data, id), do: %{data | reservations: Map.delete(data.reservations, id)}

  defp append_history(data, op, details) do
    entry = %{at: now(), op: op, details: details}
    %{data | history: [entry | data.history]}
  end

  # Agenda a expiração em uma Task supervisionada.
  defp schedule(id, delay) do
    pid = self()

    {:ok, _} =
      Task.Supervisor.start_child(@task_sup, fn ->
        Process.sleep(max(delay, 0))
        GenServer.cast(pid, {:expire, id})
      end)

    :ok
  end

  defp load(table) do
    case :dets.lookup(table, @state_key) do
      [{@state_key, data}] -> {data, true}
      [] -> {%{reservations: %{}, history: [], seq: 0}, false}
    end
  end

  # Escrita única do estado: ou persiste tudo, ou nada.
  defp persist(table, data) do
    with :ok <- :dets.insert(table, {@state_key, data}),
         :ok <- :dets.sync(table) do
      :ok
    end
  end

  defp now, do: System.system_time(:millisecond)
end

defmodule RoomBooking.Root do
  @moduledoc false
  # Supervisor raiz: reinicia o Store em caso de falha e mantém o TaskSupervisor.
  use Supervisor

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    children = [
      {Task.Supervisor, name: RoomBooking.TaskSupervisor},
      {RoomBooking.Store, opts}
    ]

    Supervisor.init(children, strategy: :one_for_one, max_restarts: 10, max_seconds: 30)
  end
end

defmodule RoomBooking.Demo do
  @moduledoc false
  alias RoomBooking.{Root, Store}

  def run do
    file = "room_booking_demo.dets"
    {:ok, root} = Root.start_link(file: file, table: :demo_booking)

    IO.inspect(Store.reserve(Store, "Sala A", 10, 12, "ana", hold_ms: 200), label: "reserva")
    IO.inspect(Store.reserve(Store, "Sala A", 11, 13, "bob"), label: "conflito esperado")

    # Aguarda a expiração automática da reserva pendente.
    Process.sleep(300)
    IO.inspect(Store.list(Store), label: "após expiração")

    {:ok, r2} = Store.reserve(Store, "Sala B", 14, 16, "carlos")
    {:ok, _} = Store.confirm(Store, r2.id)

    # Simula falha inesperada e verifica a recuperação.
    Process.exit(Process.whereis(Store), :kill)
    Process.sleep(100)
    IO.inspect(Store.get(Store, r2.id), label: "após recuperação")

    IO.inspect(Store.history(Store), label: "histórico")

    Supervisor.stop(root)
    File.rm(file)
  end
end

RoomBooking.Demo.run()
