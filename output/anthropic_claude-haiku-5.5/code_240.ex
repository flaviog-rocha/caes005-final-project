# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de reservas de salas, atendendo somente aos seguintes requisitos:
#
# 1. Impedir que uma mesma operação seja executada mais de uma vez.
# 2. Fazer uso de macros.
# 3. Persistir os dados para recuperação após reinicializações.
# 4. Permitir consultar o estado atual das operações em andamento.
# 5. Permitir limitar a quantidade de operações simultâneas.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# Gerenciador de reservas de salas com:
#   1. Idempotência: uma mesma chave nunca executa a operação mais de uma vez.
#   2. Macros: `defoperation/2` transforma uma definição em operação idempotente.
#   3. Persistência: estado gravado em DETS (biblioteca padrão), recuperado no boot.
#   4. Consulta: `in_progress/1` e `status/2` retornam o estado atual das operações.
#   5. Limite de concorrência: `max_concurrent` restringe operações simultâneas.
#
# Uso (exemplo em comentário):
#   {:ok, _} = RoomBooking.Server.start_link(path: "reservas.dets", max_concurrent: 3)
#   RoomBooking.Rooms.reservar("req-1", "Sala A", "10:00", "11:00")
#   RoomBooking.Rooms.reservar("req-1", "Sala A", "10:00", "11:00")  # mesmo resultado, sem reexecutar
#   RoomBooking.Server.in_progress()
#   RoomBooking.Server.status("req-1")

defmodule RoomBooking.Operations do
  @moduledoc """
  Macros para definir operações idempotentes.

  A macro `defoperation` gera uma função cujo primeiro argumento é a chave
  de idempotência. O corpo da operação é executado pelo servidor apenas uma vez
  por chave; chamadas repetidas recebem o mesmo resultado.
  """

  @doc false
  defmacro __using__(_opts) do
    quote do
      import RoomBooking.Operations, only: [defoperation: 2]
    end
  end

  @doc """
  Define uma operação idempotente.

      defoperation nome(arg1, arg2) do
        # corpo executado no máximo uma vez por chave
      end

  Gera `nome(idempotency_key, arg1, arg2)`.
  """
  defmacro defoperation({name, _meta, args}, do: body) when is_atom(name) do
    args = if is_list(args), do: args, else: []

    quote do
      def unquote(name)(idempotency_key, unquote_splicing(args)) do
        RoomBooking.Server.run(idempotency_key, unquote(name), fn ->
          unquote(body)
        end)
      end
    end
  end
end

defmodule RoomBooking.Server do
  @moduledoc """
  Servidor responsável por idempotência, persistência, consulta e limite de concorrência.

  Estados persistidos por chave:
    * `:running`     - operação em andamento
    * `:done`        - operação concluída (resultado armazenado e reutilizado)
    * `:interrupted` - operação estava em andamento quando o servidor caiu;
                       não é reexecutada automaticamente (evita duplicidade).
                       Pode ser liberada com `release/2` para nova tentativa.
  """
  use GenServer

  @table :room_booking_ops
  @default_path "room_booking.dets"
  @default_max_concurrent 4

  # ---------------------------------------------------------------------------
  # API pública
  # ---------------------------------------------------------------------------

  @doc "Inicia o servidor. Opções: :name, :path, :max_concurrent."
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))
  end

  @doc false
  def run(key, name, fun, server \\ __MODULE__) do
    GenServer.call(server, {:run, key, name, fun}, :infinity)
  end

  @doc "Lista as operações em andamento."
  def in_progress(server \\ __MODULE__) do
    GenServer.call(server, :in_progress)
  end

  @doc """
  Retorna o estado de uma chave:
    * `{:running, info}`
    * `{:done, registro}` / `{:interrupted, registro}`
    * `:unknown`
  """
  def status(key, server \\ __MODULE__) do
    GenServer.call(server, {:status, key})
  end

  @doc "Libera uma chave `:interrupted` para permitir nova tentativa."
  def release(key, server \\ __MODULE__) do
    GenServer.call(server, {:release, key})
  end

  # ---------------------------------------------------------------------------
  # Callbacks do GenServer
  # ---------------------------------------------------------------------------

  @impl true
  def init(opts) do
    Process.flag(:trap_exit, true)

    path = opts |> Keyword.get(:path, @default_path) |> String.to_charlist()
    {:ok, @table} = :dets.open_file(@table, file: path, type: :set)
    recover()

    {:ok,
     %{
       max: Keyword.get(opts, :max_concurrent, @default_max_concurrent),
       running: %{}
     }}
  end

  @impl true
  def handle_call({:run, key, name, fun}, from, state) do
    case Map.fetch(state.running, key) do
      {:ok, op} ->
        # Já em execução: o chamador apenas aguarda o mesmo resultado.
        op = %{op | waiters: [from | op.waiters]}
        {:noreply, %{state | running: Map.put(state.running, key, op)}}

      :error ->
        case lookup(key) do
          %{status: :done, result: result} ->
            {:reply, result, state}

          %{status: :interrupted} ->
            {:reply, {:error, :interrupted}, state}

          _ ->
            admit(key, name, fun, from, state)
        end
    end
  end

  def handle_call(:in_progress, _from, state) do
    ops =
      Enum.map(state.running, fn {key, op} ->
        %{key: key, name: op.name, started_at: op.started_at, waiters: length(op.waiters)}
      end)

    {:reply, ops, state}
  end

  def handle_call({:status, key}, _from, state) do
    reply =
      case Map.fetch(state.running, key) do
        {:ok, op} ->
          {:running, %{name: op.name, started_at: op.started_at}}

        :error ->
          case lookup(key) do
            nil -> :unknown
            %{status: status} = rec -> {status, rec}
          end
      end

    {:reply, reply, state}
  end

  def handle_call({:release, key}, _from, state) do
    case {Map.has_key?(state.running, key), lookup(key)} do
      {false, %{status: :interrupted}} ->
        :ok = :dets.delete(@table, key)
        :ok = :dets.sync(@table)
        {:reply, :ok, state}

      {false, nil} ->
        {:reply, :ok, state}

      _ ->
        {:reply, {:error, :not_releasable}, state}
    end
  end

  @impl true
  def handle_info({:op_finished, key, result}, state) do
    case Map.pop(state.running, key) do
      {nil, _} ->
        {:noreply, state}

      {op, running} ->
        Process.demonitor(op.mref, [:flush])
        finish(key, op, result, %{state | running: running})
    end
  end

  def handle_info({:DOWN, mref, :process, _pid, reason}, state) do
    case Enum.find(state.running, fn {_key, op} -> op.mref == mref end) do
      nil ->
        {:noreply, state}

      {key, op} ->
        running = Map.delete(state.running, key)
        finish(key, op, {:error, {:crashed, reason}}, %{state | running: running})
    end
  end

  def handle_info(_msg, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, state) do
    # Encerra workers ativos para não executar efeitos após o desligamento.
    # Os registros permanecem como :running e serão marcados como :interrupted no boot.
    Enum.each(state.running, fn {_key, op} -> Process.exit(op.pid, :kill) end)
    :dets.close(@table)
    :ok
  end

  # ---------------------------------------------------------------------------
  # Funções privadas
  # ---------------------------------------------------------------------------

  # Admite uma nova operação respeitando o limite de concorrência.
  defp admit(key, name, fun, from, state) do
    if map_size(state.running) >= state.max do
      {:reply, {:error, :limit_reached}, state}
    else
      parent = self()

      {pid, mref} =
        spawn_monitor(fn ->
          send(parent, {:op_finished, key, safe_run(fun)})
        end)

      started = now()

      persist(key, %{
        status: :running,
        name: name,
        started_at: started,
        finished_at: nil,
        result: nil
      })

      op = %{pid: pid, mref: mref, name: name, started_at: started, waiters: [from]}
      {:noreply, %{state | running: Map.put(state.running, key, op)}}
    end
  end

  # Conclui uma operação: persiste o resultado e responde a todos os aguardantes.
  defp finish(key, op, result, state) do
    persist(key, %{
      status: :done,
      name: op.name,
      started_at: op.started_at,
      finished_at: now(),
      result: result
    })

    Enum.each(op.waiters, &GenServer.reply(&1, result))
    {:noreply, state}
  end

  # Executa a função capturando exceções para não derrubar o worker de forma inesperada.
  defp safe_run(fun) do
    fun.()
  rescue
    e -> {:error, {:exception, Exception.message(e)}}
  catch
    kind, reason -> {:error, {kind, reason}}
  end

  # Marca operações que estavam :running no encerramento anterior como :interrupted.
  defp recover do
    stale =
      :dets.foldl(
        fn
          {key, %{status: :running} = rec}, acc ->
            [{key, %{rec | status: :interrupted, finished_at: now()}} | acc]

          _, acc ->
            acc
        end,
        [],
        @table
      )

    :ok = :dets.insert(@table, stale)
    :ok = :dets.sync(@table)
  end

  defp persist(key, rec) do
    :ok = :dets.insert(@table, {key, rec})
    :ok = :dets.sync(@table)
  end

  defp lookup(key) do
    case :dets.lookup(@table, key) do
      [{^key, rec}] -> rec
      [] -> nil
    end
  end

  defp now, do: System.system_time(:millisecond)
end

defmodule RoomBooking.Rooms do
  @moduledoc """
  Operações de reserva de salas. Cada operação recebe uma chave de idempotência
  como primeiro argumento. O resultado retornado deve conter apenas termos
  serializáveis (sem pids ou funções), pois é persistido.
  """
  use RoomBooking.Operations

  defoperation reservar(sala, inicio, fim) do
    # Simula trabalho externo (ex.: verificação de conflitos e gravação)
    Process.sleep(1_000)

    {:ok,
     %{
       id: System.unique_integer([:positive]),
       sala: sala,
       inicio: inicio,
       fim: fim
     }}
  end

  defoperation cancelar(reserva_id) do
    Process.sleep(500)
    {:ok, %{cancelada: reserva_id}}
  end
end
