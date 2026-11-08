# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de agendamento de compromissos, atendendo somente aos seguintes requisitos:
#
# 1. Executar determinadas operações periodicamente.
# 2. Permitir a execução de operações simultâneas.
# 3. Impedir atualizações parcialmente concluídas.
# 4. Implementar novas tentativas após falhas temporárias.
# 5. Permitir que diferentes entidades sejam processadas independentemente.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - DETS
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# Agendador de compromissos com persistência em DETS.
#
# Requisitos atendidos:
#  1. Execução periódica: o tick é disparado com Process.send_after/3.
#  2. Operações simultâneas: cada compromisso elegível roda em uma Task
#     supervisionada (Task.Supervisor.async_nolink/2).
#  3. Sem atualizações parciais: cada estado de um registro é gravado como um
#     único objeto DETS (:dets.insert/2 é atômico por objeto) seguido de
#     :dets.sync/1. Um registro só vira :done depois que o handler retorna :ok.
#     Registros interrompidos por queda são reenfileirados na recuperação.
#  4. Novas tentativas: erros transitórios são reagendados com backoff
#     exponencial até :max_attempts. Erros permanentes marcam :failed.
#  5. Entidades independentes: no máximo uma execução simultânea por entidade
#     (campo :entity). Entidades diferentes rodam em paralelo, e a falha de
#     uma não afeta as demais.
#
# Contrato do handler:
#   fn appointment -> :ok | {:error, :transient | :permanent, reason} end
# Exceções e saídas anormais são tratadas como erro transitório.
#
# Uso básico:
#   {:ok, _} = Agenda.Scheduler.start_link(file: "agenda.dets", handler: &minha_funcao/1)
#   Agenda.Scheduler.schedule(%{id: 1, entity: {:medico, 10}, payload: %{...}})

defmodule Agenda.Scheduler do
  use GenServer

  @default_interval 1_000
  @default_max_attempts 5
  @default_base_backoff 500
  @max_backoff 60_000

  # ---------------------------------------------------------------------------
  # API pública
  # ---------------------------------------------------------------------------

  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc "Agenda um compromisso. Campos: :id (obrigatório), :entity (opcional), :payload (opcional)."
  def schedule(server \\ __MODULE__, appointment) do
    GenServer.call(server, {:schedule, appointment})
  end

  @doc "Lista todos os registros persistidos."
  def list(server \\ __MODULE__), do: GenServer.call(server, :list)

  @doc "Obtém um registro pelo id."
  def get(server \\ __MODULE__, id), do: GenServer.call(server, {:get, id})

  # ---------------------------------------------------------------------------
  # Callbacks do GenServer
  # ---------------------------------------------------------------------------

  @impl true
  def init(opts) do
    # Necessário para que terminate/2 feche a tabela DETS corretamente.
    Process.flag(:trap_exit, true)

    file = opts |> Keyword.get(:file, "agenda.dets") |> String.to_charlist()
    tab_name = Keyword.get(opts, :table, :agenda_appointments)

    case :dets.open_file(tab_name, file: file, type: :set) do
      {:ok, tab} ->
        {:ok, sup} = Task.Supervisor.start_link()

        # Registros que estavam :running quando o processo caiu voltam a :pending.
        recover(tab)

        interval = Keyword.get(opts, :interval, @default_interval)
        Process.send_after(self(), :tick, interval)

        {:ok,
         %{
           tab: tab,
           sup: sup,
           handler: Keyword.get(opts, :handler, &default_handler/1),
           interval: interval,
           max_attempts: Keyword.get(opts, :max_attempts, @default_max_attempts),
           base_backoff: Keyword.get(opts, :base_backoff, @default_base_backoff),
           # ref da Task => {id, entity}
           running: %{},
           # entidades com alguma execução em andamento
           busy: MapSet.new()
         }}

      {:error, reason} ->
        {:stop, reason}
    end
  end

  @impl true
  def handle_call({:schedule, %{id: id} = attrs}, _from, state) do
    rec = %{
      id: id,
      entity: Map.get(attrs, :entity, id),
      payload: Map.get(attrs, :payload),
      status: :pending,
      attempts: 0,
      next_at: now(),
      last_error: nil,
      version: 0
    }

    # insert_new é atômico: não sobrescreve um registro existente.
    reply =
      if :dets.insert_new(state.tab, {id, rec}) == true do
        :ok = :dets.sync(state.tab)
        {:ok, id}
      else
        {:error, :already_exists}
      end

    {:reply, reply, state}
  end

  def handle_call({:schedule, _invalid}, _from, state) do
    {:reply, {:error, :invalid_appointment}, state}
  end

  def handle_call(:list, _from, state) do
    {:reply, all(state.tab), state}
  end

  def handle_call({:get, id}, _from, state) do
    reply =
      case :dets.lookup(state.tab, id) do
        [{_, rec}] -> {:ok, rec}
        [] -> {:error, :not_found}
        {:error, _} = err -> err
      end

    {:reply, reply, state}
  end

  @impl true
  def handle_info(:tick, state) do
    now = now()

    state =
      state.tab
      |> all()
      |> Enum.filter(&(&1.status == :pending and &1.next_at <= now))
      |> Enum.sort_by(& &1.next_at)
      |> Enum.reduce(state, fn rec, acc ->
        # Respeita a independência por entidade: no máximo uma execução por entidade.
        if MapSet.member?(acc.busy, rec.entity), do: acc, else: dispatch(rec, acc)
      end)

    Process.send_after(self(), :tick, state.interval)
    {:noreply, state}
  end

  # Resultado de uma Task concluída.
  def handle_info({ref, result}, state) when is_reference(ref) do
    Process.demonitor(ref, [:flush])
    {:noreply, finish(ref, result, state)}
  end

  # Task terminou de forma anormal (morte externa, por exemplo).
  def handle_info({:DOWN, ref, :process, _pid, reason}, state) do
    if Map.has_key?(state.running, ref) do
      {:noreply, finish(ref, {:error, :transient, {:crashed, reason}}, state)}
    else
      {:noreply, state}
    end
  end

  def handle_info(_msg, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, state) do
    # Tarefas em andamento são encerradas pelo supervisor vinculado;
    # na próxima inicialização serão recuperadas por recover/1.
    :dets.close(state.tab)
    :ok
  end

  # ---------------------------------------------------------------------------
  # Despacho e conclusão de execuções
  # ---------------------------------------------------------------------------

  defp dispatch(rec, state) do
    running_rec = %{
      rec
      | status: :running,
        attempts: rec.attempts + 1,
        version: rec.version + 1
    }

    # Marca como em execução antes de iniciar a Task, para que uma queda
    # durante a execução seja detectada na recuperação.
    persist!(state.tab, running_rec)

    handler = state.handler
    task = Task.Supervisor.async_nolink(state.sup, fn -> execute(handler, running_rec) end)

    %{
      state
      | running: Map.put(state.running, task.ref, {rec.id, rec.entity}),
        busy: MapSet.put(state.busy, rec.entity)
    }
  end

  defp execute(handler, rec) do
    try do
      case handler.(rec) do
        :ok -> :ok
        {:error, :permanent, reason} -> {:error, :permanent, reason}
        {:error, :transient, reason} -> {:error, :transient, reason}
        other -> {:error, :transient, {:unexpected_result, other}}
      end
    catch
      kind, reason -> {:error, :transient, {kind, reason}}
    end
  end

  defp finish(ref, result, state) do
    {id, entity} = Map.fetch!(state.running, ref)
    now = now()

    current =
      case :dets.lookup(state.tab, id) do
        [{_, rec}] -> rec
      end

    new_rec =
      case result do
        :ok ->
          %{current | status: :done, next_at: nil, last_error: nil}

        {:error, :permanent, reason} ->
          %{current | status: :failed, next_at: nil, last_error: reason}

        {:error, :transient, reason} when current.attempts >= state.max_attempts ->
          %{current | status: :failed, next_at: nil, last_error: reason}

        {:error, :transient, reason} ->
          %{
            current
            | status: :pending,
              next_at: now + backoff(current.attempts, state.base_backoff),
              last_error: reason
          }
      end

    persist!(state.tab, %{new_rec | version: current.version + 1})

    %{
      state
      | running: Map.delete(state.running, ref),
        busy: MapSet.delete(state.busy, entity)
    }
  end

  # ---------------------------------------------------------------------------
  # Persistência e utilitários
  # ---------------------------------------------------------------------------

  # Cada estado é gravado como um único objeto; não há estados intermediários
  # visíveis em disco para o mesmo registro.
  defp persist!(tab, rec) do
    :ok = :dets.insert(tab, {rec.id, rec})
    :ok = :dets.sync(tab)
  end

  defp recover(tab) do
    now = now()

    tab
    |> all()
    |> Enum.filter(&(&1.status == :running))
    |> Enum.each(fn rec ->
      persist!(tab, %{
        rec
        | status: :pending,
          next_at: now,
          last_error: :interrupted,
          version: rec.version + 1
      })
    end)
  end

  defp all(tab), do: :dets.foldl(fn {_id, rec}, acc -> [rec | acc] end, [], tab)

  # Backoff exponencial: base * 2^(tentativas - 1), limitado a @max_backoff.
  defp backoff(attempts, base), do: min(base * Integer.pow(2, attempts - 1), @max_backoff)

  # Tempo de sistema (e não monotônico), pois os valores são persistidos em disco.
  defp now, do: System.system_time(:millisecond)

  defp default_handler(_rec), do: :ok
end
