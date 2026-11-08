# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de agendamento de compromissos, atendendo somente aos seguintes requisitos:
#
# 1. Permitir comunicação entre diferentes processos.
# 2. Implementar novas tentativas após falhas temporárias.
# 3. Gerar notificações quando determinadas condições forem atingidas.
# 4. Persistir os dados para recuperação após reinicializações.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# agenda.ex
# Sistema de agendamento de compromissos com:
#   1. Comunicação entre processos (GenServer + Registry + Task.Supervisor)
#   2. Novas tentativas com backoff exponencial após falhas temporárias
#   3. Notificações disparadas quando condições são atingidas (lembrete e início)
#   4. Persistência em disco com escrita atômica e recuperação na reinicialização
#
# Executar: elixir agenda.ex  (ou carregar com `c "agenda.ex"` no iex)

defmodule Agenda.Store do
  @moduledoc """
  Persistência em disco. Grava um snapshot binário escrevendo primeiro em um
  arquivo temporário e depois renomeando, o que evita arquivos corrompidos
  caso o processo caia no meio da escrita.
  """

  @doc "Carrega o snapshot salvo. Retorna `nil` se não houver arquivo."
  def load(path) do
    case File.read(path) do
      {:ok, bin} -> :erlang.binary_to_term(bin)
      {:error, :enoent} -> nil
      {:error, reason} -> raise "falha ao ler #{path}: #{inspect(reason)}"
    end
  end

  @doc "Salva o snapshot de forma atômica. Lança exceção em caso de erro."
  def save!(path, snapshot) do
    tmp = path <> ".tmp"
    :ok = File.write!(tmp, :erlang.term_to_binary(snapshot))

    case File.rename(tmp, path) do
      :ok -> :ok
      {:error, reason} -> raise "falha ao gravar #{path}: #{inspect(reason)}"
    end
  end
end

defmodule Agenda.Notifier do
  @moduledoc """
  Pub/Sub local baseado no Registry da biblioteca padrão.
  Processos interessados chamam `subscribe/1` e recebem
  mensagens `{:notification, topic, message}`.
  """

  @registry Agenda.Registry

  @doc "Registra o processo chamador como assinante do tópico."
  def subscribe(topic \\ :agenda) do
    {:ok, _} = Registry.register(@registry, topic, nil)
    :ok
  end

  @doc """
  Envia a mensagem a todos os assinantes do tópico.
  Retorna `{:error, :no_subscribers}` quando não há ninguém para receber,
  o que é tratado como falha temporária e reexecutado pelo `Agenda.Retry`.
  """
  def publish(topic \\ :agenda, message) do
    case Registry.lookup(@registry, topic) do
      [] ->
        {:error, :no_subscribers}

      subs ->
        Enum.each(subs, fn {pid, _} ->
          send(pid, {:notification, topic, message})
        end)

        :ok
    end
  end
end

defmodule Agenda.Retry do
  @moduledoc """
  Executa uma função novamente após falhas temporárias, com backoff
  exponencial e jitter. A função deve retornar `:ok`, `{:ok, valor}`
  ou `{:error, motivo}`. Exceções também são tratadas como falha.
  """

  @doc """
  Opções:
    * `:max_attempts` - número máximo de tentativas (padrão 5)
    * `:base_delay_ms` - atraso base do backoff em ms (padrão 100)
  """
  def run(fun, opts \\ []) do
    max = Keyword.get(opts, :max_attempts, 5)
    base = Keyword.get(opts, :base_delay_ms, 100)
    attempt(fun, 1, max, base)
  end

  defp attempt(fun, n, max, base) do
    result =
      try do
        fun.()
      rescue
        e -> {:error, {:exception, Exception.message(e)}}
      catch
        :exit, reason -> {:error, {:exit, reason}}
      end

    case result do
      :ok ->
        :ok

      {:ok, _} = ok ->
        ok

      {:error, _} when n < max ->
        # backoff exponencial: base * 2^(n-1) + jitter aleatório
        delay = base * Integer.pow(2, n - 1) + :rand.uniform(base)
        Process.sleep(delay)
        attempt(fun, n + 1, max, base)

      {:error, motivo} ->
        {:error, {:esgotado, n, motivo}}
    end
  end
end

defmodule Agenda.Scheduler do
  @moduledoc """
  Processo central que mantém os compromissos e a caixa de saída (outbox)
  de notificações. A cada tick verifica as condições:

    * lembrete: faltam `lead_seconds` ou menos para o compromisso
    * início: o horário do compromisso foi atingido

  Eventos pendentes ficam na outbox persistida. Entregas são feitas em
  tarefas supervisionadas com retentativas. Um evento só sai da outbox
  após entrega confirmada, então nada se perde em uma reinicialização.
  """

  use GenServer

  @name __MODULE__

  # ---------------------------------------------------------------------------
  # API pública
  # ---------------------------------------------------------------------------

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, @name))
  end

  @doc "Agenda um compromisso e retorna `{:ok, id}`."
  def schedule(server \\ @name, title, %DateTime{} = at) do
    GenServer.call(server, {:schedule, title, at})
  end

  @doc "Cancela um compromisso pelo id."
  def cancel(server \\ @name, id) do
    GenServer.call(server, {:cancel, id})
  end

  @doc "Lista os compromissos ordenados por horário."
  def list(server \\ @name) do
    GenServer.call(server, :list)
  end

  # ---------------------------------------------------------------------------
  # Callbacks
  # ---------------------------------------------------------------------------

  @impl true
  def init(opts) do
    path = Keyword.fetch!(opts, :path)
    lead = Keyword.get(opts, :lead_seconds, 60)
    tick = Keyword.get(opts, :tick_ms, 1_000)
    task_sup = Keyword.get(opts, :task_sup, Agenda.TaskSup)

    # Recuperação: carrega o snapshot anterior, se existir
    snap =
      case Agenda.Store.load(path) do
        nil -> %{appointments: %{}, outbox: %{}, next_id: 1}
        s -> s
      end

    state =
      Map.merge(snap, %{
        path: path,
        lead: lead,
        tick_ms: tick,
        task_sup: task_sup,
        # eventos já em entrega nesta execução (não persistido: após restart,
        # tudo que está na outbox é reenviado)
        inflight: MapSet.new()
      })

    Process.send_after(self(), :tick, tick)
    {:ok, state}
  end

  @impl true
  def handle_call({:schedule, title, at}, _from, s) do
    id = s.next_id

    appt = %{
      id: id,
      title: title,
      at: at,
      reminded: false,
      started: false
    }

    s =
      %{s | appointments: Map.put(s.appointments, id, appt), next_id: id + 1}
      |> persist!()

    {:reply, {:ok, id}, s}
  end

  def handle_call({:cancel, id}, _from, s) do
    case Map.pop(s.appointments, id) do
      {nil, _} ->
        {:reply, {:error, :not_found}, s}

      {_, rest} ->
        outbox = Map.reject(s.outbox, fn {{aid, _}, _} -> aid == id end)
        s = %{s | appointments: rest, outbox: outbox} |> persist!()
        {:reply, :ok, s}
    end
  end

  def handle_call(:list, _from, s) do
    lista =
      s.appointments
      |> Map.values()
      |> Enum.sort_by(& &1.at, DateTime)

    {:reply, lista, s}
  end

  @impl true
  def handle_info(:tick, s) do
    now = DateTime.utc_now()

    s =
      s
      |> enqueue_due(now)
      |> dispatch_pending()

    Process.send_after(self(), :tick, s.tick_ms)
    {:noreply, s}
  end

  # Resultado de uma tentativa de entrega vinda da tarefa
  def handle_info({:entrega, key, :ok}, s) do
    s = %{
      s
      | outbox: Map.delete(s.outbox, key),
        inflight: MapSet.delete(s.inflight, key)
    }

    {:noreply, persist!(s)}
  end

  def handle_info({:entrega, key, {:error, _motivo}}, s) do
    # Falha definitiva para esta rodada: o evento continua na outbox
    # e será reenviado no próximo tick.
    {:noreply, %{s | inflight: MapSet.delete(s.inflight, key)}}
  end

  # ---------------------------------------------------------------------------
  # Internos
  # ---------------------------------------------------------------------------

  # Verifica as condições de notificação e coloca os eventos na outbox
  defp enqueue_due(s, now) do
    novos =
      Enum.flat_map(s.appointments, fn {id, a} ->
        lembrete =
          if not a.reminded and DateTime.diff(a.at, now) <= s.lead,
            do: [{{id, :lembrete}, {:lembrete, id, a.title, a.at}}],
            else: []

        inicio =
          if not a.started and DateTime.compare(now, a.at) != :lt,
            do: [{{id, :inicio}, {:inicio, id, a.title}}],
            else: []

        lembrete ++ inicio
      end)

    case novos do
      [] ->
        s

      _ ->
        appts =
          Enum.reduce(novos, s.appointments, fn {{id, tipo}, _}, acc ->
            Map.update!(acc, id, fn a ->
              case tipo do
                :lembrete -> %{a | reminded: true}
                :inicio -> %{a | started: true}
              end
            end)
          end)

        outbox = Enum.reduce(novos, s.outbox, fn {k, msg}, acc -> Map.put(acc, k, msg) end)

        persist!(%{s | appointments: appts, outbox: outbox})
    end
  end

  # Dispara entregas assíncronas para eventos que não estão em voo
  defp dispatch_pending(s) do
    me = self()

    pendentes =
      Enum.reject(s.outbox, fn {k, _} -> MapSet.member?(s.inflight, k) end)

    Enum.reduce(pendentes, s, fn {key, msg}, acc ->
      Task.Supervisor.start_child(acc.task_sup, fn ->
        resultado = Agenda.Retry.run(fn -> Agenda.Notifier.publish(msg) end)
        send(me, {:entrega, key, resultado})
      end)

      %{acc | inflight: MapSet.put(acc.inflight, key)}
    end)
  end

  defp persist!(s) do
    :ok = Agenda.Store.save!(s.path, Map.take(s, [:appointments, :outbox, :next_id]))
    s
  end
end

defmodule Agenda do
  @moduledoc """
  Supervisor raiz. Inicia o Registry de assinantes, o Task.Supervisor para
  entregas e o Scheduler.

  Exemplo de uso (em comentário):

      {:ok, _} = Agenda.start_link(path: "agenda.dat", lead_seconds: 60)

      # um processo qualquer assina as notificações
      parent = self()
      spawn(fn ->
        Agenda.Notifier.subscribe()
        loop = fn loop ->
          receive do
            {:notification, _topic, msg} ->
              IO.inspect(msg, label: "notificacao")
              loop.(loop)
          end
        end
        loop.(loop)
      end)

      at = DateTime.add(DateTime.utc_now(), 30, :second)
      {:ok, id} = Agenda.Scheduler.schedule("Reunião", at)
      Agenda.Scheduler.list()
      Agenda.Scheduler.cancel(id)
  """

  use Supervisor

  def start_link(opts) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    children = [
      {Registry, keys: :duplicate, name: Agenda.Registry},
      {Task.Supervisor, name: Agenda.TaskSup},
      {Agenda.Scheduler, opts}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end
