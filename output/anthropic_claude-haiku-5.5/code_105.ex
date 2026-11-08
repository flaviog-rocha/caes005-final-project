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
# Arquivo autocontido, usando apenas a biblioteca padrão (Supervisor, GenServer,
# Task, :dets, System). Atende aos requisitos de histórico, recuperação de falhas,
# expiração automática e atomicidade das atualizações.

defmodule RoomBooking do
  @moduledoc """
  Ponto de entrada do sistema de reservas de salas.

  A árvore de supervisão usa a estratégia `:rest_for_one`: se o `Store`
  falhar, o supervisor o reinicia e reinicia também o `Task` de expiração,
  que depende dele. O estado é recuperado a partir do snapshot persistido em
  disco (DETS).
  """

  use Supervisor

  @doc "Inicia a árvore de supervisão. Opções: `:path`, `:hold_ttl_ms`, `:sweep_interval_ms`."
  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    children = [
      {RoomBooking.Store, opts},
      # Task supervisionado (reiniciado de forma permanente) que executa o
      # laço de expiração automática periodicamente.
      Supervisor.child_spec(
        {Task, fn -> RoomBooking.Store.sweep_loop(opts) end},
        id: :expiration_sweeper,
        restart: :permanent
      )
    ]

    Supervisor.init(children, strategy: :rest_for_one)
  end

  # ---------------------------------------------------------------------------
  # API pública
  # ---------------------------------------------------------------------------

  @doc "Cria uma reserva provisória (hold) para a sala no intervalo [from, to) em ms."
  def hold(room, owner, from, to) do
    RoomBooking.Store.execute({:hold, room, owner, from, to})
  end

  @doc "Confirma uma reserva provisória pertencente ao `owner`."
  def confirm(id, owner), do: RoomBooking.Store.execute({:confirm, id, owner})

  @doc "Cancela uma reserva pertencente ao `owner`."
  def cancel(id, owner), do: RoomBooking.Store.execute({:cancel, id, owner})

  @doc """
  Executa várias operações como uma unidade atômica (tudo ou nada).
  Recebe uma lista de tuplas de operação, por exemplo `{:hold, ...}`.
  """
  def transaction(ops) when is_list(ops) do
    RoomBooking.Store.execute({:batch, ops})
  end

  @doc "Força uma varredura de expiração imediata."
  def sweep, do: RoomBooking.Store.execute(:sweep)

  @doc "Retorna o histórico de operações em ordem cronológica."
  def history, do: RoomBooking.Store.history()

  @doc "Retorna o mapa atual de reservas."
  def reservations, do: RoomBooking.Store.reservations()
end

defmodule RoomBooking.Store do
  @moduledoc """
  Processo dono do estado do sistema.

  Garantias implementadas:

    * Atomicidade: cada operação é calculada sobre uma cópia pura do estado.
      Só há commit se a operação inteira for válida e a persistência
      concluir. Caso contrário, o estado anterior é mantido.

    * Persistência: o snapshot (reservas + histórico) é gravado em DETS
      com uma única chamada `:dets.insert/2`, seguida de `:dets.sync/1`.

    * Recuperação: em caso de falha inesperada, o processo é reiniciado pelo
      supervisor e o estado é carregado do último snapshot persistido.
      Como o commit ocorre após o cálculo, uma falha no meio de uma
      operação não deixa dados parciais.

    * Expiração: reservas provisórias expiram após `hold_ttl_ms`. Reservas
      confirmadas expiram ao término do intervalo reservado. Um Task
      periódico chama `sweep/0`.
  """

  use GenServer

  @table :room_booking_store
  @default_path "room_booking.dets"
  @default_hold_ttl_ms 300_000
  @default_sweep_interval_ms 1_000

  # ---------------------------------------------------------------------------
  # Inicialização e recuperação
  # ---------------------------------------------------------------------------

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    # Garante que `terminate/2` seja chamado em encerramentos ordenados.
    Process.flag(:trap_exit, true)

    path =
      opts
      |> Keyword.get(:path, @default_path)
      |> String.to_charlist()

    case :dets.open_file(@table, file: path, type: :set) do
      {:ok, tab} ->
        state = %{
          tab: tab,
          data: load(tab),
          hold_ttl_ms: Keyword.get(opts, :hold_ttl_ms, @default_hold_ttl_ms)
        }

        {:ok, state}

      {:error, reason} ->
        {:stop, {:dets_open_failed, reason}}
    end
  end

  @impl true
  def terminate(_reason, %{tab: tab}) do
    :dets.close(tab)
    :ok
  end

  defp load(tab) do
    case :dets.lookup(tab, :snapshot) do
      [{:snapshot, data}] -> data
      [] -> %{reservations: %{}, history: [], next_id: 1}
    end
  end

  # ---------------------------------------------------------------------------
  # API pública do Store
  # ---------------------------------------------------------------------------

  @doc "Executa uma operação de forma atômica."
  def execute(op), do: GenServer.call(__MODULE__, {:apply, op})

  @doc "Histórico de operações em ordem cronológica."
  def history, do: GenServer.call(__MODULE__, :history)

  @doc "Mapa atual de reservas."
  def reservations, do: GenServer.call(__MODULE__, :reservations)

  @doc """
  Laço executado pelo Task supervisionado. Executa `sweep` a cada intervalo.
  Se o Store cair, o supervisor reinicia este Task.
  """
  def sweep_loop(opts) do
    interval = Keyword.get(opts, :sweep_interval_ms, @default_sweep_interval_ms)
    Process.sleep(interval)
    execute(:sweep)
    sweep_loop(opts)
  end

  # ---------------------------------------------------------------------------
  # Callbacks do GenServer
  # ---------------------------------------------------------------------------

  @impl true
  def handle_call({:apply, op}, _from, s) do
    # Se `apply_op/3` lançar exceção, o processo cai antes do commit.
    # O supervisor o reinicia e o estado é recarregado do DETS.
    case apply_op(op, s.data, context(s)) do
      {:ok, data, _result} when data == s.data ->
        {:reply, {:ok, elem_result(op, s), data}, s}
        |> then(fn {:reply, _, _} = reply -> reply end)

      {:ok, data, result} ->
        commit(data, result, s)

      {:error, reason} ->
        {:reply, {:error, reason}, s}
    end
  end

  def handle_call(:history, _from, s) do
    {:reply, Enum.reverse(s.data.history), s}
  end

  def handle_call(:reservations, _from, s) do
    {:reply, s.data.reservations, s}
  end

  # ---------------------------------------------------------------------------
  # Commit atômico
  # ---------------------------------------------------------------------------

  # Persiste o novo estado. Só atualiza a memória se a persistência funcionar.
  defp commit(data, result, s) do
    case persist(s.tab, data) do
      :ok ->
        {:reply, {:ok, result}, %{s | data: data}}

      {:error, reason} ->
        {:reply, {:error, {:persist_failed, reason}}, s}
    end
  end

  # Um único `insert` do snapshot inteiro; `sync` garante gravação em disco.
  defp persist(tab, data) do
    with :ok <- :dets.insert(tab, {:snapshot, data}),
         :ok <- :dets.sync(tab) do
      :ok
    end
  end

  defp context(s) do
    %{now: System.system_time(:millisecond), hold_ttl_ms: s.hold_ttl_ms}
  end

  # Usado apenas quando a operação não alterou o estado (ex.: sweep sem expirados).
  defp elem_result(:sweep, _s), do: []
  defp elem_result(_op, _s), do: :noop

  # ---------------------------------------------------------------------------
  # Operações puras: recebem os dados e retornam {:ok, novos_dados, resultado}
  # ou {:error, motivo}. Nunca alteram o estado diretamente.
  # ---------------------------------------------------------------------------

  # Criação de reserva provisória.
  defp apply_op({:hold, room, owner, from, to}, data, ctx)
       when is_integer(from) and is_integer(to) and from < to do
    case conflicts(data, ctx, room, from, to) do
      [] ->
        id = data.next_id

        reservation = %{
          id: id,
          room: room,
          owner: owner,
          from: from,
          to: to,
          status: :held,
          expires_at: ctx.now + ctx.hold_ttl_ms
        }

        data = %{
          data
          | next_id: id + 1,
            reservations: Map.put(data.reservations, id, reservation)
        }

        {:ok, log(data, ctx, :hold, %{id: id, room: room, owner: owner}), id}

      ids ->
        {:error, {:conflict, ids}}
    end
  end

  defp apply_op({:hold, _room, _owner, _from, _to}, _data, _ctx) do
    {:error, :invalid_interval}
  end

  # Confirmação de reserva provisória.
  defp apply_op({:confirm, id, owner}, data, ctx) do
    with {:ok, r} <- fetch(data, id, owner),
         :ok <- expect(r.status == :held, :not_held),
         :ok <- expect(live?(r, ctx.now), :hold_expired) do
      r = %{r | status: :confirmed, expires_at: r.to}
      data = %{data | reservations: Map.put(data.reservations, id, r)}
      {:ok, log(data, ctx, :confirm, %{id: id}), id}
    end
  end

  # Cancelamento de reserva.
  defp apply_op({:cancel, id, owner}, data, ctx) do
    with {:ok, _r} <- fetch(data, id, owner) do
      data = %{data | reservations: Map.delete(data.reservations, id)}
      {:ok, log(data, ctx, :cancel, %{id: id}), id}
    end
  end

  # Lote atômico: aplica tudo em sequência sobre a cópia; se uma falhar,
  # o resultado é erro e nada é commitado.
  defp apply_op({:batch, ops}, data, ctx) when is_list(ops) do
    initial = {:ok, data, []}

    result =
      ops
      |> Enum.with_index()
      |> Enum.reduce_while(initial, fn {op, idx}, {:ok, acc, results} ->
        case apply_op(op, acc, ctx) do
          {:ok, acc2, res} -> {:cont, {:ok, acc2, [res | results]}}
          {:error, reason} -> {:halt, {:error, {idx, reason}}}
        end
      end)

    case result do
      {:ok, new_data, results} ->
        new_data = log(new_data, ctx, :batch, %{size: length(ops)})
        {:ok, new_data, Enum.reverse(results)}

      {:error, _} = err ->
        err
    end
  end

  # Expiração automática: remove reservas cujo prazo já passou.
  defp apply_op(:sweep, data, ctx) do
    expired = for {id, r} <- data.reservations, not live?(r, ctx.now), do: id

    data =
      Enum.reduce(expired, data, fn id, acc ->
        %{acc | reservations: Map.delete(acc.reservations, id)}
      end)

    data =
      case expired do
        [] -> data
        ids -> log(data, ctx, :expired, %{ids: ids})
      end

    {:ok, data, expired}
  end

  defp apply_op(other, _data, _ctx) do
    {:error, {:unknown_operation, other}}
  end

  # ---------------------------------------------------------------------------
  # Auxiliares
  # ---------------------------------------------------------------------------

  # Reserva ativa é aquela ainda não expirada.
  defp live?(r, now), do: r.expires_at > now

  # IDs de reservas ativas que se sobrepõem ao intervalo na mesma sala.
  defp conflicts(data, ctx, room, from, to) do
    for {id, r} <- data.reservations,
        r.room == room,
        live?(r, ctx.now),
        r.from < to,
        from < r.to,
        do: id
  end

  # Busca reserva verificando o dono.
  defp fetch(data, id, owner) do
    case Map.fetch(data.reservations, id) do
      {:ok, %{owner: ^owner} = r} -> {:ok, r}
      {:ok, _} -> {:error, :forbidden}
      :error -> {:error, :not_found}
    end
  end

  defp expect(true, _reason), do: :ok
  defp expect(false, reason), do: {:error, reason}

  # Acrescenta uma entrada ao histórico.
  defp log(data, ctx, op, details) do
    entry = %{at: ctx.now, op: op, details: details}
    %{data | history: [entry | data.history]}
  end
end
