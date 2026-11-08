# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de uma fila de tarefas, atendendo somente aos seguintes requisitos:
#
# 1. Fazer uso de macros.
# 2. Permitir comunicação entre diferentes processos.
# 3. Garantir a consistência dos dados durante operações concorrentes.
# 4. Implementar novas tentativas após falhas temporárias.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# Arquivo único: sistema de fila de tarefas com macros, comunicação entre processos,
# consistência sob concorrência e novas tentativas após falhas temporárias.
# Usa apenas a biblioteca padrão do Elixir (GenServer, Process, :queue, MapSet etc.).

# ---------------------------------------------------------------------------
# Macros: permitem declarar jobs de forma declarativa e fornecem atalhos
# para sinalizar falhas transitórias (retry!) ou permanentes (abort!).
# ---------------------------------------------------------------------------
defmodule TaskQueue.Macros do
  @moduledoc """
  Macros para declarar jobs executáveis pela fila.

  Uso:

      defmodule Mailer do
        use TaskQueue.Macros

        defjob :send_welcome, max_attempts: 5 do
          if payload.flaky?, do: retry!(:smtp_busy)
          {:sent, payload.email}
        end
      end

  - `defjob/3` define uma função `name/1` cujo argumento é o payload.
  - Exceções não tratadas viram falhas transitórias (serão reexecutadas).
  - `retry!/1` força uma falha transitória.
  - `abort!/1` força uma falha permanente (sem novas tentativas).
  """

  # Importa as macros e registra o acumulador de metadados dos jobs.
  defmacro __using__(_opts) do
    quote do
      import TaskQueue.Macros
      Module.register_attribute(__MODULE__, :job_registry, accumulate: true)
      @before_compile TaskQueue.Macros
    end
  end

  # Gera a função __jobs__/0 com as opções de cada job declarado no módulo.
  defmacro __before_compile__(env) do
    jobs = env.module |> Module.get_attribute(:job_registry) |> Enum.reverse()

    quote do
      @doc false
      def __jobs__, do: unquote(Macro.escape(jobs))
    end
  end

  # Declara um job. Valida as opções em tempo de compilação e envolve o corpo
  # em try/rescue/catch para normalizar o retorno para o protocolo da fila.
  defmacro defjob(name, opts \\ [], do: block) do
    opts = Keyword.validate!(opts, max_attempts: 3, timeout: 5_000)

    quote do
      @job_registry {unquote(name), unquote(Macro.escape(opts))}

      def unquote(name)(var!(payload)) do
        try do
          {:ok, unquote(block)}
        rescue
          exception -> {:error, :transient, exception}
        catch
          :throw, {:task_queue, kind, reason} -> {:error, kind, reason}
        end
      end
    end
  end

  # Falha temporária: a fila tentará novamente com backoff exponencial.
  defmacro retry!(reason) do
    quote do
      throw({:task_queue, :transient, unquote(reason)})
    end
  end

  # Falha permanente: o job vai para a fila de mortos sem novas tentativas.
  defmacro abort!(reason) do
    quote do
      throw({:task_queue, :permanent, unquote(reason)})
    end
  end
end

# ---------------------------------------------------------------------------
# Fila de tarefas.
#
# Consistência: todo o estado é mantido por um único GenServer. Mudanças de
# estado são serializadas pela mailbox, então não há condição de corrida sobre
# `jobs`, `pending` ou `workers`. Cada job tem no máximo um worker ativo por vez.
#
# Comunicação: clientes usam chamadas síncronas (GenServer.call); workers
# reportam resultados por mensagens; assinantes recebem eventos
# `{:task_queue, job_id, evento}`.
#
# Novas tentativas: falhas transitórias (retry!, exceções, crash do worker,
# timeout) são reagendadas com backoff exponencial e jitter, até max_attempts.
# ---------------------------------------------------------------------------
defmodule TaskQueue do
  use GenServer

  defmodule Job do
    @moduledoc false
    defstruct [
      :id,
      :mod,
      :fun,
      :payload,
      :max_attempts,
      :timeout,
      :result,
      :last_error,
      attempts: 0,
      status: :pending
    ]
  end

  # -------------------------------------------------------------------------
  # API pública
  # -------------------------------------------------------------------------

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc "Enfileira um job. Opções declaradas via defjob servem como padrão."
  def enqueue(mod, fun, payload, opts \\ []) do
    opts = Keyword.merge(declared_opts(mod, fun), opts)
    GenServer.call(__MODULE__, {:enqueue, mod, fun, payload, opts})
  end

  @doc "Consulta o status atual de um job."
  def status(id), do: GenServer.call(__MODULE__, {:status, id})

  @doc "Retorna a contagem de jobs por status."
  def stats, do: GenServer.call(__MODULE__, :stats)

  @doc "Inscreve o processo chamador para receber eventos da fila."
  def subscribe, do: GenServer.call(__MODULE__, {:subscribe, self()})

  # Lê as opções declaradas com defjob (se o módulo as expuser).
  defp declared_opts(mod, fun) do
    declared =
      if Code.ensure_loaded?(mod) and function_exported?(mod, :__jobs__, 0),
        do: mod.__jobs__(),
        else: []

    Keyword.get(declared, fun, [])
  end

  # -------------------------------------------------------------------------
  # Callbacks do GenServer
  # -------------------------------------------------------------------------

  @impl true
  def init(opts) do
    state = %{
      jobs: %{},
      pending: :queue.new(),
      # pid do worker => {job_id, monitor_ref}
      workers: %{},
      subscribers: MapSet.new(),
      max_concurrency: Keyword.get(opts, :max_concurrency, System.schedulers_online()),
      base_backoff: Keyword.get(opts, :base_backoff, 100),
      max_backoff: Keyword.get(opts, :max_backoff, 10_000)
    }

    {:ok, state}
  end

  @impl true
  def handle_call({:enqueue, mod, fun, payload, opts}, _from, s) do
    id = System.unique_integer([:positive, :monotonic])

    job = %Job{
      id: id,
      mod: mod,
      fun: fun,
      payload: payload,
      status: :pending,
      max_attempts: Keyword.get(opts, :max_attempts, 3),
      timeout: Keyword.get(opts, :timeout, 5_000)
    }

    s =
      s
      |> put_job(job)
      |> Map.update!(:pending, &:queue.in(id, &1))
      |> notify(id, :enqueued)
      |> dispatch()

    {:reply, {:ok, id}, s}
  end

  def handle_call({:status, id}, _from, s) do
    reply =
      case Map.fetch(s.jobs, id) do
        {:ok, job} -> {:ok, job.status}
        :error -> {:error, :not_found}
      end

    {:reply, reply, s}
  end

  def handle_call(:stats, _from, s) do
    counts = s.jobs |> Map.values() |> Enum.frequencies_by(& &1.status)
    {:reply, {:ok, counts}, s}
  end

  def handle_call({:subscribe, pid}, _from, s) do
    if MapSet.member?(s.subscribers, pid) do
      {:reply, :ok, s}
    else
      Process.monitor(pid)
      {:reply, :ok, %{s | subscribers: MapSet.put(s.subscribers, pid)}}
    end
  end

  @impl true
  # Worker terminou e reportou o resultado.
  def handle_info({:job_done, pid, result}, s) do
    case Map.pop(s.workers, pid) do
      {{id, ref}, workers} ->
        Process.demonitor(ref, [:flush])

        s =
          %{s | workers: workers}
          |> finish(id, result)
          |> dispatch()

        {:noreply, s}

      {nil, _} ->
        {:noreply, s}
    end
  end

  # Worker excedeu o timeout: é encerrado e a falha é tratada como transitória.
  def handle_info({:job_timeout, pid}, s) do
    case Map.pop(s.workers, pid) do
      {{id, ref}, workers} ->
        Process.demonitor(ref, [:flush])
        Process.exit(pid, :kill)

        s =
          %{s | workers: workers}
          |> fail(id, :timeout)
          |> dispatch()

        {:noreply, s}

      {nil, _} ->
        {:noreply, s}
    end
  end

  # Monitor disparou: pode ser crash de worker ou saída de assinante.
  def handle_info({:DOWN, _ref, :process, pid, reason}, s) do
    case Map.pop(s.workers, pid) do
      {{id, _ref}, workers} ->
        s =
          %{s | workers: workers}
          |> fail(id, {:crashed, reason})
          |> dispatch()

        {:noreply, s}

      {nil, _} ->
        {:noreply, %{s | subscribers: MapSet.delete(s.subscribers, pid)}}
    end
  end

  # Tempo de backoff expirou: recoloca o job na fila.
  def handle_info({:retry, id}, s) do
    s =
      case Map.fetch(s.jobs, id) do
        {:ok, %{status: :waiting} = job} ->
          s
          |> put_job(%{job | status: :pending})
          |> Map.update!(:pending, &:queue.in(id, &1))
          |> notify(id, :requeued)

        _ ->
          s
      end

    {:noreply, dispatch(s)}
  end

  # -------------------------------------------------------------------------
  # Lógica interna (sempre executada dentro do GenServer)
  # -------------------------------------------------------------------------

  # Inicia workers enquanto houver capacidade e jobs pendentes.
  defp dispatch(s) do
    if map_size(s.workers) < s.max_concurrency and not :queue.is_empty(s.pending) do
      {{:value, id}, pending} = :queue.out(s.pending)
      s = %{s | pending: pending}

      job = %{Map.fetch!(s.jobs, id) | attempts: Map.fetch!(s.jobs, id).attempts + 1, status: :running}
      server = self()

      {pid, ref} =
        spawn_monitor(fn ->
          send(server, {:job_done, self(), execute(job)})
        end)

      Process.send_after(server, {:job_timeout, pid}, job.timeout)

      s =
        %{s | workers: Map.put(s.workers, pid, {id, ref})}
        |> put_job(job)
        |> notify(id, {:started, job.attempts})

      dispatch(s)
    else
      s
    end
  end

  # Executa o job no processo worker, normalizando o retorno.
  defp execute(%Job{mod: mod, fun: fun, payload: payload}) do
    Code.ensure_loaded(mod)

    if function_exported?(mod, fun, 1) do
      normalize(apply(mod, fun, [payload]))
    else
      {:error, :permanent, {:undefined_job, mod, fun}}
    end
  rescue
    exception -> {:error, :transient, exception}
  end

  defp normalize({:ok, _} = ok), do: ok
  defp normalize({:error, kind, _} = err) when kind in [:transient, :permanent], do: err
  defp normalize({:error, reason}), do: {:error, :transient, reason}
  defp normalize(other), do: {:error, :permanent, {:invalid_return, other}}

  defp finish(s, id, {:ok, value}) do
    job = Map.fetch!(s.jobs, id)

    s
    |> put_job(%{job | status: :completed, result: value})
    |> notify(id, {:completed, value})
  end

  defp finish(s, id, {:error, :permanent, reason}) do
    job = Map.fetch!(s.jobs, id)
    dead(s, %{job | last_error: reason})
  end

  defp finish(s, id, {:error, :transient, reason}), do: fail(s, id, reason)

  # Decide entre nova tentativa (com backoff) ou envio para a fila de mortos.
  defp fail(s, id, reason) do
    job = %{Map.fetch!(s.jobs, id) | last_error: reason}

    if job.attempts < job.max_attempts do
      delay = backoff(s, job.attempts)
      Process.send_after(self(), {:retry, id}, delay)

      s
      |> put_job(%{job | status: :waiting})
      |> notify(id, {:retrying, job.attempts, delay})
    else
      dead(s, job)
    end
  end

  defp dead(s, job) do
    s
    |> put_job(%{job | status: :dead})
    |> notify(job.id, {:dead, job.last_error})
  end

  # Backoff exponencial limitado, com jitter para evitar rajadas sincronizadas.
  defp backoff(s, attempt) do
    base = s.base_backoff * Integer.pow(2, attempt - 1)
    min(base, s.max_backoff) + :rand.uniform(max(s.base_backoff, 1))
  end

  defp put_job(s, %Job{id: id} = job), do: %{s | jobs: Map.put(s.jobs, id, job)}

  # Comunicação entre processos: envia o evento a todos os assinantes.
  defp notify(s, id, event) do
    Enum.each(s.subscribers, &send(&1, {:task_queue, id, event}))
    s
  end
end

# ---------------------------------------------------------------------------
# Exemplo de módulo de jobs usando as macros.
# ---------------------------------------------------------------------------
defmodule TaskQueue.Examples.Mailer do
  use TaskQueue.Macros

  # Falha transitória simulada: será reexecutado com backoff.
  defjob :send_welcome, max_attempts: 5 do
    if :rand.uniform() < 0.5, do: retry!(:smtp_busy)
    {:sent, payload}
  end

  # Falha permanente: não há novas tentativas.
  defjob :validate, timeout: 1_000 do
    unless is_binary(payload), do: abort!(:invalid_email)
    {:valid, payload}
  end
end
