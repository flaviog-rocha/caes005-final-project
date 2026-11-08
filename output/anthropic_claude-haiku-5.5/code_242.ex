# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de agendamento de compromissos, atendendo somente aos seguintes requisitos:
#
# 1. Permitir comunicação entre diferentes processos.
# 2. Implementar novas tentativas após falhas temporárias.
# 3. Gerar notificações quando determinadas condições forem atingidas.
# 4. Persistir os dados para recuperação após reinicializações.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# agenda.exs
# Sistema de agendamento de compromissos em Elixir (biblioteca padrão).
# Execute com: elixir agenda.exs

# ---------------------------------------------------------------------------
# Persistência: grava o estado em disco de forma atômica (arquivo temporário
# + rename), permitindo recuperação após reinicializações.
# ---------------------------------------------------------------------------
defmodule Agenda.Storage do
  @moduledoc "Persistência do estado em arquivo binário."

  @doc "Carrega o estado salvo. Retorna `{:ok, mapa}` ou `{:error, motivo}`."
  def load(path) do
    case File.read(path) do
      {:ok, bin} -> decode(bin)
      {:error, :enoent} -> {:ok, %{}}
      {:error, reason} -> {:error, reason}
    end
  end

  @doc "Salva o estado de forma atômica."
  def save(path, data) do
    tmp = path <> ".tmp"

    with :ok <- File.write(tmp, :erlang.term_to_binary(data)),
         :ok <- File.rename(tmp, path) do
      :ok
    end
  end

  defp decode(bin) do
    # :safe impede a criação de átomos novos a partir de dados externos.
    {:ok, :erlang.binary_to_term(bin, [:safe])}
  rescue
    ArgumentError -> {:error, :corrupted}
  end
end

# ---------------------------------------------------------------------------
# Novas tentativas: executa uma função novamente após falhas temporárias,
# com backoff exponencial.
# ---------------------------------------------------------------------------
defmodule Agenda.Retry do
  @moduledoc "Repetição de operações com backoff exponencial."

  @doc """
  Executa `fun` até que retorne `:ok` ou `{:ok, _}`.
  Opções: `:max_attempts` (padrão 3) e `:base_delay` em ms (padrão 100).
  """
  def run(fun, opts \\ []) do
    max = Keyword.get(opts, :max_attempts, 3)
    base = Keyword.get(opts, :base_delay, 100)
    do_run(fun, 1, max, base)
  end

  defp do_run(fun, attempt, max, base) do
    case fun.() do
      :ok ->
        :ok

      {:ok, _} = ok ->
        ok

      {:error, _reason} = error when attempt >= max ->
        error

      {:error, _reason} ->
        Process.sleep(base * Integer.pow(2, attempt - 1))
        do_run(fun, attempt + 1, max, base)
    end
  end
end

# ---------------------------------------------------------------------------
# Servidor principal: comunicação entre processos (call/cast/mensagens),
# notificações por condição (janela de lembrete) e persistência.
# ---------------------------------------------------------------------------
defmodule Agenda.Server do
  @moduledoc """
  GenServer que gerencia compromissos.

  * Comunicação: clientes usam `GenServer.call`; assinantes recebem
    mensagens `{:agenda_notification, tipo, compromisso}`.
  * Notificações: quando um compromisso entra na janela de lembrete
    (`reminder_seconds` antes do horário), todos os assinantes são avisados.
  * Retentativas: persistência e entrega de notificações são repetidas
    em caso de falha temporária.
  * Persistência: cada alteração é gravada em disco e o estado é
    restaurado na inicialização.
  """

  use GenServer

  @default_reminder 3600
  @default_tick 1_000

  # ----------------------------- API pública -------------------------------

  def start_link(opts) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc "Registra o processo chamador para receber notificações."
  def subscribe(server \\ __MODULE__), do: GenServer.call(server, {:subscribe, self()})

  @doc "Agenda um compromisso. Retorna `{:ok, id}` ou `{:error, motivo}`."
  def schedule(server \\ __MODULE__, title, %DateTime{} = at) do
    GenServer.call(server, {:schedule, title, at})
  end

  @doc "Cancela um compromisso pelo id."
  def cancel(server \\ __MODULE__, id), do: GenServer.call(server, {:cancel, id})

  @doc "Lista os compromissos ordenados por horário."
  def list(server \\ __MODULE__), do: GenServer.call(server, :list)

  # ------------------------------ Callbacks --------------------------------

  @impl true
  def init(opts) do
    path = Keyword.get(opts, :path, "agenda.dat")

    case Agenda.Storage.load(path) do
      {:ok, appointments} ->
        state = %{
          path: path,
          appointments: appointments,
          # pid => referência do monitor (remove assinantes que morrem)
          subscribers: %{},
          reminder: Keyword.get(opts, :reminder_seconds, @default_reminder),
          tick: Keyword.get(opts, :tick_ms, @default_tick),
          # ids com entrega em andamento (evita envios duplicados)
          notifying: MapSet.new()
        }

        schedule_tick(state.tick)
        {:ok, state}

      {:error, reason} ->
        {:stop, {:load_failed, reason}}
    end
  end

  @impl true
  def handle_call({:subscribe, pid}, _from, state) do
    subs =
      if Map.has_key?(state.subscribers, pid) do
        state.subscribers
      else
        Map.put(state.subscribers, pid, Process.monitor(pid))
      end

    {:reply, :ok, %{state | subscribers: subs}}
  end

  def handle_call({:schedule, title, at}, _from, state) do
    now = DateTime.utc_now()

    cond do
      DateTime.compare(at, now) == :lt ->
        {:reply, {:error, :in_the_past}, state}

      Enum.any?(Map.values(state.appointments), &(DateTime.compare(&1.at, at) == :eq)) ->
        {:reply, {:error, :conflict}, state}

      true ->
        id = next_id(state.appointments)
        appt = %{id: id, title: title, at: at, notified: false}
        commit(state, Map.put(state.appointments, id, appt), {:ok, id})
    end
  end

  def handle_call({:cancel, id}, _from, state) do
    if Map.has_key?(state.appointments, id) do
      commit(state, Map.delete(state.appointments, id), :ok)
    else
      {:reply, {:error, :not_found}, state}
    end
  end

  def handle_call(:list, _from, state) do
    list = Enum.sort_by(Map.values(state.appointments), & &1.at, DateTime)
    {:reply, list, state}
  end

  @impl true
  def handle_info(:tick, state) do
    schedule_tick(state.tick)
    now = DateTime.utc_now()

    due =
      state.appointments
      |> Map.values()
      |> Enum.filter(fn appt ->
        not MapSet.member?(state.notifying, appt.id) and due?(appt, now, state.reminder)
      end)

    {:noreply, Enum.reduce(due, state, &dispatch/2)}
  end

  # Entrega concluída: marca como notificado e persiste.
  def handle_info({:delivery_result, id, :ok}, state) do
    state = %{state | notifying: MapSet.delete(state.notifying, id)}

    case Map.fetch(state.appointments, id) do
      {:ok, appt} ->
        new_appts = Map.put(state.appointments, id, %{appt | notified: true})

        case persist(state.path, new_appts) do
          :ok ->
            {:noreply, %{state | appointments: new_appts}}

          # Se a persistência falhar, o lembrete pode ser reenviado após
          # reinicialização (semântica "pelo menos uma vez").
          {:error, _} ->
            {:noreply, state}
        end

      :error ->
        {:noreply, state}
    end
  end

  # Falha definitiva na entrega: libera para nova tentativa no próximo tick.
  def handle_info({:delivery_result, id, {:error, _}}, state) do
    {:noreply, %{state | notifying: MapSet.delete(state.notifying, id)}}
  end

  # Assinante terminou: remove da lista.
  def handle_info({:DOWN, _ref, :process, pid, _reason}, state) do
    {:noreply, %{state | subscribers: Map.delete(state.subscribers, pid)}}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  # ----------------------------- Funções privadas --------------------------

  defp schedule_tick(ms), do: Process.send_after(self(), :tick, ms)

  defp next_id(appts), do: appts |> Map.keys() |> Enum.max(fn -> 0 end) |> Kernel.+(1)

  # Condição de notificação: o compromisso está dentro da janela de lembrete.
  defp due?(%{notified: false, at: at}, now, reminder) do
    secs = DateTime.diff(at, now, :second)
    secs > 0 and secs <= reminder
  end

  defp due?(_appt, _now, _reminder), do: false

  # Persiste antes de aplicar a mudança; em falha, mantém o estado anterior.
  defp commit(state, new_appts, reply) do
    case persist(state.path, new_appts) do
      :ok -> {:reply, reply, %{state | appointments: new_appts}}
      {:error, reason} -> {:reply, {:error, {:persist_failed, reason}}, state}
    end
  end

  defp persist(path, appts) do
    Agenda.Retry.run(fn -> Agenda.Storage.save(path, appts) end,
      max_attempts: 3,
      base_delay: 50
    )
  end

  # Envia a notificação em um Task separado (não bloqueia o servidor),
  # com novas tentativas caso os assinantes estejam indisponíveis.
  defp dispatch(appt, state) do
    if map_size(state.subscribers) == 0 do
      state
    else
      server = self()
      targets = Map.keys(state.subscribers)
      message = {:agenda_notification, :lembrete, appt}

      Task.start(fn ->
        result =
          Agenda.Retry.run(fn -> deliver(targets, message) end,
            max_attempts: 5,
            base_delay: 100
          )

        send(server, {:delivery_result, appt.id, result})
      end)

      %{state | notifying: MapSet.put(state.notifying, appt.id)}
    end
  end

  defp deliver(targets, message) do
    case Enum.filter(targets, &Process.alive?/1) do
      [] ->
        {:error, :unavailable}

      alive ->
        Enum.each(alive, &send(&1, message))
        :ok
    end
  end
end

# ---------------------------------------------------------------------------
# Demonstração: agenda, recebe a notificação, reinicia o servidor e
# verifica que os dados foram recuperados do disco.
# ---------------------------------------------------------------------------
defmodule Agenda.Demo do
  @moduledoc "Execução de exemplo do sistema de agendamento."

  alias Agenda.Server

  def run do
    path = Path.join(System.tmp_dir!(), "agenda_demo.dat")
    File.rm(path)

    opts = [path: path, reminder_seconds: 3, tick_ms: 200]

    {:ok, pid} = Server.start_link(opts)
    :ok = Server.subscribe()

    at = DateTime.add(DateTime.utc_now(), 5, :second)
    {:ok, _id} = Server.schedule("Consulta médica", at)
    IO.inspect(Server.list(), label: "Agendados")

    receive do
      {:agenda_notification, tipo, appt} ->
        IO.puts("Notificação (#{tipo}): #{appt.title} às #{appt.at}")
    after
      10_000 -> IO.puts("Nenhuma notificação recebida")
    end

    # Simula reinicialização: o estado deve ser recuperado do disco.
    GenServer.stop(pid)
    {:ok, _} = Server.start_link(opts)
    IO.inspect(Server.list(), label: "Após reinicialização")

    {:error, :not_found} = Server.cancel(-1)
    :ok
  end
end

Agenda.Demo.run()
