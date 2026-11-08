# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de agendamento de compromissos, atendendo somente aos seguintes requisitos:
#
# 1. Impedir que uma mesma operação seja executada mais de uma vez.
# 2. Permitir comunicação entre diferentes processos.
# 3. Validar os dados recebidos antes de realizar operações.
# 4. Permitir limitar a quantidade de operações simultâneas.
# 5. Recuperar processos após falhas inesperadas.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# agenda.exs
# Execute com: elixir agenda.exs
#
# Sistema de agendamento de compromissos usando apenas a biblioteca padrão (OTP).
#
# Requisitos atendidos:
#   1. Idempotência: cada chave de operação é executada no máximo uma vez.
#   2. Comunicação entre processos: pub/sub com Registry e mensagens entre GenServers.
#   3. Validação: todos os dados são validados antes de qualquer operação.
#   4. Limite de concorrência: semáforo próprio (Agenda.Limiter).
#   5. Recuperação: supervisão com rest_for_one; o estado sobrevive à queda
#      de Store/Limiter porque as tabelas ETS pertencem ao Agenda.Vault.

defmodule Agenda do
  @moduledoc """
  API pública do sistema de agendamento.

  Árvore de supervisão (rest_for_one):
    Agenda.Vault      -> dono das tabelas ETS (dados persistem a falhas de Store/Limiter)
    Registry (pubsub) -> canal de comunicação entre processos
    Agenda.Store      -> estado e operações idempotentes
    Agenda.Limiter    -> limita operações simultâneas
  """
  use Supervisor

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    children = [
      Agenda.Vault,
      {Registry, keys: :duplicate, name: Agenda.PubSub.Registry},
      Agenda.Store,
      {Agenda.Limiter, max_concurrent: 4}
    ]

    # rest_for_one: se o Vault cair, tudo é reiniciado em ordem (tabelas são recriadas).
    # Se Store ou Limiter caírem, somente eles e os filhos posteriores são reiniciados,
    # preservando as tabelas ETS e, portanto, os agendamentos já confirmados.
    Supervisor.init(children, strategy: :rest_for_one)
  end

  @doc """
  Agenda um compromisso de forma idempotente.

  `idempotency_key` identifica a operação: repetir a mesma chave nunca
  cria um segundo agendamento; devolve o resultado original.
  """
  def schedule(idempotency_key, params) do
    with {:ok, key} <- Agenda.Validator.validate_key(idempotency_key),
         {:ok, appt} <- Agenda.Validator.validate_appointment(params) do
      run_once(key, appt)
    end
  end

  @doc "Lista todos os agendamentos confirmados, ordenados por início."
  def list, do: Agenda.Store.list_appointments()

  @doc "Inscreve o processo atual para receber eventos de agendamento."
  def subscribe, do: Agenda.PubSub.subscribe(:appointments)

  # -- Internos ---------------------------------------------------------------

  defp run_once(key, appt) do
    case Agenda.Store.begin_op(key) do
      # Operação já concluída: devolve o resultado anterior sem reexecutar.
      {:done, result} -> result
      # Mesma operação em andamento em outro processo.
      {:error, :in_progress} -> {:error, :operation_in_progress}
      :ok -> execute(key, appt)
    end
  end

  defp execute(key, appt) do
    result =
      try do
        Agenda.Limiter.run(fn -> Agenda.Store.insert_appointment(appt) end)
      catch
        kind, reason ->
          # Falha inesperada: libera a chave para que possa ser tentada novamente.
          Agenda.Store.abort_op(key)
          :erlang.raise(kind, reason, __STACKTRACE__)
      end

    case result do
      {:error, :limiter_timeout} ->
        # Não executou de fato: libera a chave para nova tentativa.
        Agenda.Store.abort_op(key)
        result

      _ ->
        Agenda.Store.finish_op(key, result)

        case result do
          {:ok, _} -> Agenda.PubSub.broadcast(:appointments, {:scheduled, appt})
          _ -> :ok
        end

        result
    end
  end
end

defmodule Agenda.Vault do
  @moduledoc """
  Dono das tabelas ETS. Não executa lógica de negócio: existe apenas para que
  os dados sobrevivam à queda de Store/Limiter (que são reiniciados).
  """
  use GenServer

  def start_link(_opts \\ []), do: GenServer.start_link(__MODULE__, :ok, name: __MODULE__)

  @impl true
  def init(:ok) do
    :ets.new(:agenda_ops, [:set, :public, :named_table, read_concurrency: true])
    :ets.new(:agenda_appointments, [:set, :public, :named_table, read_concurrency: true])
    {:ok, nil}
  end
end

defmodule Agenda.Validator do
  @moduledoc "Validação de chaves de operação e dados de agendamento."

  @max_key 128
  @max_title 200
  @max_duration 1_440

  @doc "Valida a chave de idempotência."
  def validate_key(key) when is_binary(key) do
    if String.trim(key) != "" and byte_size(key) <= @max_key,
      do: {:ok, key},
      else: {:error, [{:idempotency_key, "deve ser texto não vazio com até #{@max_key} bytes"}]}
  end

  def validate_key(_), do: {:error, [{:idempotency_key, "deve ser um texto"}]}

  @doc "Valida os dados do compromisso e monta o registro normalizado."
  def validate_appointment(params) when is_map(params) do
    title = Map.get(params, :title)
    participant = Map.get(params, :participant)
    starts_at = Map.get(params, :starts_at)
    duration = Map.get(params, :duration_minutes)

    errors =
      [
        error_unless(
          is_binary(title) and String.trim(title) != "" and String.length(title) <= @max_title,
          :title,
          "deve ser texto não vazio com até #{@max_title} caracteres"
        ),
        error_unless(
          is_binary(participant) and String.trim(participant) != "",
          :participant,
          "deve ser texto não vazio"
        ),
        error_unless(
          match?(%DateTime{}, starts_at) and
            DateTime.compare(starts_at, DateTime.utc_now()) == :gt,
          :starts_at,
          "deve ser um DateTime no futuro"
        ),
        error_unless(
          is_integer(duration) and duration >= 1 and duration <= @max_duration,
          :duration_minutes,
          "deve ser um inteiro entre 1 e #{@max_duration}"
        )
      ]
      |> Enum.reject(&is_nil/1)

    case errors do
      [] ->
        {:ok,
         %{
           id: System.unique_integer([:positive, :monotonic]),
           title: String.trim(title),
           participant: participant,
           starts_at: starts_at,
           ends_at: DateTime.add(starts_at, duration * 60, :second),
           duration_minutes: duration
         }}

      errors ->
        {:error, errors}
    end
  end

  def validate_appointment(_), do: {:error, [{:params, "deve ser um mapa"}]}

  defp error_unless(true, _field, _msg), do: nil
  defp error_unless(_, field, msg), do: {field, msg}
end

defmodule Agenda.Store do
  @moduledoc """
  Estado do sistema: agendamentos e controle de idempotência.

  - Operações são serializadas por este GenServer (evita condições de corrida
    na verificação de conflitos).
  - Chaves ficam em :agenda_ops com um de dois estados:
      {:in_progress, pid, monitor_ref} ou {:done, resultado}
  - Se o processo dono de uma operação em andamento morre, a chave é liberada
    automaticamente via monitor (permitindo nova tentativa segura).
  - Após reinício do Store, as operações em andamento são re-monitoradas;
    as de processos já mortos são liberadas.
  """
  use GenServer

  @ops :agenda_ops
  @appts :agenda_appointments

  def start_link(_opts \\ []), do: GenServer.start_link(__MODULE__, :ok, name: __MODULE__)

  def begin_op(key), do: GenServer.call(__MODULE__, {:begin_op, key, self()})
  def finish_op(key, result), do: GenServer.call(__MODULE__, {:finish_op, key, result})
  def abort_op(key), do: GenServer.call(__MODULE__, {:abort_op, key})
  def insert_appointment(appt), do: GenServer.call(__MODULE__, {:insert, appt})

  def list_appointments do
    @appts
    |> :ets.tab2list()
    |> Enum.map(fn {_id, appt} -> appt end)
    |> Enum.sort_by(& &1.starts_at, DateTime)
  end

  @impl true
  def init(:ok) do
    # Recuperação: re-monitora operações pendentes que sobreviveram ao reinício.
    # Para processos já mortos, o monitor dispara :DOWN imediatamente e a chave é liberada.
    monitors =
      for {key, {:in_progress, pid, _old_ref}} <- :ets.tab2list(@ops), into: %{} do
        ref = Process.monitor(pid)
        :ets.insert(@ops, {key, {:in_progress, pid, ref}})
        {ref, key}
      end

    {:ok, %{monitors: monitors}}
  end

  @impl true
  def handle_call({:begin_op, key, owner}, _from, state) do
    # Se o dono anterior já morreu (mensagem :DOWN ainda na fila), libera a chave.
    state = if stale?(key), do: drop_op(state, key), else: state

    case :ets.lookup(@ops, key) do
      [{^key, {:done, result}}] ->
        {:reply, {:done, result}, state}

      [{^key, {:in_progress, _pid, _ref}}] ->
        {:reply, {:error, :in_progress}, state}

      [] ->
        ref = Process.monitor(owner)
        :ets.insert(@ops, {key, {:in_progress, owner, ref}})
        {:reply, :ok, %{state | monitors: Map.put(state.monitors, ref, key)}}
    end
  end

  def handle_call({:finish_op, key, result}, _from, state) do
    state = drop_op(state, key)
    :ets.insert(@ops, {key, {:done, result}})
    {:reply, :ok, state}
  end

  def handle_call({:abort_op, key}, _from, state) do
    {:reply, :ok, drop_op(state, key)}
  end

  def handle_call({:insert, appt}, _from, state) do
    conflict? =
      @appts
      |> :ets.tab2list()
      |> Enum.any?(fn {_id, other} ->
        other.participant == appt.participant and overlap?(other, appt)
      end)

    if conflict? do
      {:reply, {:error, :conflict}, state}
    else
      :ets.insert(@appts, {appt.id, appt})
      {:reply, {:ok, appt}, state}
    end
  end

  @impl true
  def handle_info({:DOWN, ref, :process, _pid, _reason}, state) do
    case Map.pop(state.monitors, ref) do
      {nil, _} ->
        {:noreply, state}

      {key, monitors} ->
        # Dono morreu antes de concluir: libera a chave para nova tentativa.
        :ets.match_delete(@ops, {key, {:in_progress, :_, ref}})
        {:noreply, %{state | monitors: monitors}}
    end
  end

  def handle_info(_msg, state), do: {:noreply, state}

  # -- Internos ---------------------------------------------------------------

  defp stale?(key) do
    case :ets.lookup(@ops, key) do
      [{^key, {:in_progress, pid, _}}] -> not Process.alive?(pid)
      _ -> false
    end
  end

  # Remove uma operação em andamento (e seu monitor). Não altera operações concluídas.
  defp drop_op(state, key) do
    case :ets.lookup(@ops, key) do
      [{^key, {:in_progress, _pid, ref}}] ->
        Process.demonitor(ref, [:flush])
        :ets.delete(@ops, key)
        %{state | monitors: Map.delete(state.monitors, ref)}

      _ ->
        state
    end
  end

  defp overlap?(a, b) do
    DateTime.compare(a.starts_at, b.ends_at) == :lt and
      DateTime.compare(b.starts_at, a.ends_at) == :lt
  end
end

defmodule Agenda.Limiter do
  @moduledoc """
  Semáforo que limita a quantidade de operações executadas simultaneamente.

  - Quem adquire um slot é monitorado: se morrer, o slot é liberado.
  - Quem excede o limite entra em uma fila (FIFO).
  - Se o chamador desiste por timeout, sua entrada é cancelada (sem vazamento de slot).
  """
  use GenServer

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, Keyword.get(opts, :max_concurrent, 4), name: __MODULE__)
  end

  @doc "Executa `fun` respeitando o limite de concorrência."
  def run(fun, timeout \\ 5_000) when is_function(fun, 0) do
    case acquire(timeout) do
      {:ok, ref} ->
        try do
          fun.()
        after
          GenServer.cast(__MODULE__, {:release, ref})
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp acquire(timeout) do
    GenServer.call(__MODULE__, :acquire, timeout)
  catch
    :exit, _ ->
      # Ordem de mensagens garante que o cancelamento chegue após o pedido original.
      GenServer.cast(__MODULE__, {:cancel, self()})
      {:error, :limiter_timeout}
  end

  @impl true
  def init(max) do
    {:ok, %{max: max, active: %{}, waiting: :queue.new()}}
  end

  @impl true
  def handle_call(:acquire, {pid, _} = from, state) do
    ref = Process.monitor(pid)

    if map_size(state.active) < state.max do
      {:reply, {:ok, ref}, %{state | active: Map.put(state.active, ref, pid)}}
    else
      {:noreply, %{state | waiting: :queue.in({from, ref, pid}, state.waiting)}}
    end
  end

  @impl true
  def handle_cast({:release, ref}, state) do
    if Map.has_key?(state.active, ref) do
      Process.demonitor(ref, [:flush])
      {:noreply, grant_next(%{state | active: Map.delete(state.active, ref)})}
    else
      {:noreply, state}
    end
  end

  def handle_cast({:cancel, pid}, state) do
    {mine, active} = Enum.split_with(state.active, fn {_ref, p} -> p == pid end)
    Enum.each(mine, fn {ref, _} -> Process.demonitor(ref, [:flush]) end)

    {mine_waiting, rest} =
      state.waiting
      |> :queue.to_list()
      |> Enum.split_with(fn {_from, _ref, p} -> p == pid end)

    Enum.each(mine_waiting, fn {_from, ref, _} -> Process.demonitor(ref, [:flush]) end)

    state = %{state | active: Map.new(active), waiting: :queue.from_list(rest)}
    {:noreply, grant_next(state)}
  end

  @impl true
  def handle_info({:DOWN, ref, :process, _pid, _reason}, state) do
    waiting = :queue.filter(fn {_from, r, _pid} -> r != ref end, state.waiting)
    state = %{state | active: Map.delete(state.active, ref), waiting: waiting}
    {:noreply, grant_next(state)}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  defp grant_next(state) do
    if map_size(state.active) < state.max do
      case :queue.out(state.waiting) do
        {{:value, {from, ref, pid}}, rest} ->
          GenServer.reply(from, {:ok, ref})
          grant_next(%{state | active: Map.put(state.active, ref, pid), waiting: rest})

        {:empty, _} ->
          state
      end
    else
      state
    end
  end
end

defmodule Agenda.PubSub do
  @moduledoc "Comunicação entre processos por tópicos, baseada em Registry."

  @registry Agenda.PubSub.Registry

  @doc "Inscreve o processo atual no tópico."
  def subscribe(topic) do
    {:ok, _} = Registry.register(@registry, topic, nil)
    :ok
  end

  @doc "Envia `message` para todos os inscritos no tópico."
  def broadcast(topic, message) do
    Registry.dispatch(@registry, topic, fn entries ->
      for {pid, _} <- entries, do: send(pid, {:agenda_event, topic, message})
    end)

    :ok
  end
end

defmodule Agenda.Demo do
  @moduledoc "Demonstração dos requisitos."

  def run do
    {:ok, _} = Agenda.start_link()
    Agenda.subscribe()

    start = DateTime.add(DateTime.utc_now(), 3_600, :second)
    params = %{title: "Reunião", participant: "ana", starts_at: start, duration_minutes: 60}

    # 1. Idempotência
    IO.inspect(Agenda.schedule("op-1", params), label: "1ª execução")
    IO.inspect(Agenda.schedule("op-1", params), label: "repetida (mesma chave)")

    # 3. Validação
    IO.inspect(Agenda.schedule("op-3", %{params | title: ""}), label: "dados inválidos")
    IO.inspect(Agenda.schedule("", params), label: "chave inválida")

    # Conflito de horário para o mesmo participante
    IO.inspect(Agenda.schedule("op-2", params), label: "conflito de horário")

    # 4. Limite de concorrência (máx. 4 simultâneas) + 2. comunicação
    tasks =
      for i <- 1..10,
          do: Task.async(fn -> Agenda.schedule("c-#{i}", %{params | participant: "p#{i}"}) end)

    results = Enum.map(tasks, &Task.await/1)
    IO.inspect(Enum.count(results, &match?({:ok, _}, &1)), label: "concorrentes agendados")

    events = drain([])
    IO.inspect(length(events), label: "eventos recebidos via pub/sub")

    # 5. Recuperação após falha inesperada
    Process.whereis(Agenda.Store) |> Process.exit(:kill)
    Process.sleep(100)

    IO.inspect(Agenda.schedule("op-1", params), label: "após falha (idempotência mantida)")
    IO.inspect(length(Agenda.list()), label: "agendamentos preservados")
  end

  defp drain(acc) do
    receive do
      {:agenda_event, _topic, msg} -> drain([msg | acc])
    after
      200 -> Enum.reverse(acc)
    end
  end
end

Agenda.Demo.run()
