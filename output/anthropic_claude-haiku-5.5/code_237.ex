# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de uma fila de tarefas, atendendo somente aos seguintes requisitos:
#
# 1. Fazer uso de macros.
# 2. Permitir comunicação entre diferentes processos.
# 3. Garantir a consistência dos dados durante operações concorrentes.
# 4. Implementar novas tentativas após falhas temporárias.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# Arquivo autocontido: fila de tarefas com macros, comunicação entre processos,
# consistência sob concorrência e novas tentativas com backoff exponencial.
# Executar com: elixir task_queue.exs

defmodule TaskQueue.DSL do
  @moduledoc """
  Macros para declarar jobs de forma declarativa.

      defmodule MeusJobs do
        use TaskQueue.DSL

        defjob :enviar, max_attempts: 5 do
          if payload[:ok], do: :ok, else: {:error, :instavel}
        end
      end

  Valores de retorno do corpo do job:
    * `:ok` ou `{:ok, valor}`  -> sucesso
    * `{:error, motivo}`       -> falha temporária (será re-tentada)
    * `{:fatal, motivo}`       -> falha permanente (vai para a dead-letter)
    * exceções, throws ou exits -> tratados como falha temporária
  """

  # Injeta os imports e o acumulador de jobs no módulo que usa a DSL.
  defmacro __using__(_opts) do
    quote do
      import TaskQueue.DSL, only: [defjob: 2, defjob: 3]
      Module.register_attribute(__MODULE__, :__tq_jobs__, accumulate: true)
      @before_compile TaskQueue.DSL
    end
  end

  # Forma sem opções: defjob :nome do ... end
  defmacro defjob(name, do: block), do: build(name, [], block)

  # Forma com opções: defjob :nome, max_attempts: 3 do ... end
  defmacro defjob(name, opts, do: block), do: build(name, opts, block)

  # Valida em tempo de compilação e gera a função do job.
  # A variável `payload` é exposta ao corpo via var!/1 (higiene explícita).
  defp build(name, opts, block) do
    unless is_atom(name), do: raise(ArgumentError, "o nome do job deve ser um átomo")

    unless Keyword.keyword?(opts),
      do: raise(ArgumentError, "as opções do job devem ser uma keyword list")

    quote do
      @__tq_jobs__ {unquote(name), unquote(opts)}
      def unquote(name)(var!(payload)), do: unquote(block)
    end
  end

  # Gera o catálogo de jobs do módulo, consultado pelo servidor em tempo de execução.
  defmacro __before_compile__(env) do
    jobs =
      env.module
      |> Module.get_attribute(:__tq_jobs__)
      |> Enum.reverse()
      |> Map.new()

    quote do
      @doc false
      def __jobs__, do: unquote(Macro.escape(jobs))
    end
  end
end

defmodule TaskQueue.Server do
  @moduledoc false
  # Processo único que detém todo o estado da fila. Todas as transições
  # (enfileirar, despachar, concluir, re-tentar, descartar) acontecem aqui,
  # serializadas pela mailbox do GenServer, o que garante consistência
  # mesmo com muitos produtores e workers concorrentes.

  use GenServer

  @defaults %{max_attempts: 3, timeout: 5_000, backoff: 100}
  @keys [:max_attempts, :timeout, :backoff]
  @max_backoff 60_000

  defstruct mod: nil,
            max_concurrency: 4,
            pending: :queue.new(),
            jobs: %{},
            running: %{},
            subscribers: %{}

  ## API pública (executada nos processos clientes)

  def start_link(opts) do
    {name, opts} = Keyword.pop(opts, :name)
    gen_opts = if name, do: [name: name], else: []
    GenServer.start_link(__MODULE__, opts, gen_opts)
  end

  def enqueue(server, job_name, payload, opts \\ [])
      when is_atom(job_name) and is_list(opts),
      do: GenServer.call(server, {:enqueue, job_name, payload, opts})

  def status(server, id), do: GenServer.call(server, {:status, id})

  def stats(server), do: GenServer.call(server, :stats)

  # Assinantes recebem {:tq_event, evento} na mailbox (comunicação assíncrona).
  def subscribe(server), do: GenServer.call(server, {:subscribe, self()})

  def unsubscribe(server), do: GenServer.call(server, {:unsubscribe, self()})

  # Bloqueia até o job concluir ou ser descartado (ou até o timeout).
  def await(server, id, timeout \\ 5_000) do
    :ok = subscribe(server)

    try do
      case status(server, id) do
        {:ok, %{status: :done, result: value}} -> {:ok, value}
        {:ok, %{status: :dead, error: reason}} -> {:error, reason}
        {:ok, _} -> wait_for(id, timeout)
        {:error, _} = error -> error
      end
    after
      :ok = unsubscribe(server)
    end
  end

  defp wait_for(id, timeout) do
    receive do
      {:tq_event, {:completed, ^id, value}} -> {:ok, value}
      {:tq_event, {:dead, ^id, reason}} -> {:error, reason}
    after
      timeout -> {:error, :timeout}
    end
  end

  ## Callbacks do GenServer

  @impl true
  def init(opts) do
    mod = Keyword.fetch!(opts, :jobs)
    concurrency = Keyword.get(opts, :max_concurrency, System.schedulers_online())
    {:ok, %__MODULE__{mod: mod, max_concurrency: concurrency}}
  end

  @impl true
  def handle_call({:enqueue, job_name, payload, opts}, _from, state) do
    case Map.fetch(state.mod.__jobs__(), job_name) do
      :error ->
        {:reply, {:error, :unknown_job}, state}

      {:ok, declared} ->
        # Idempotência: um :id explícito não pode ser enfileirado duas vezes.
        id = Keyword.get_lazy(opts, :id, fn -> System.unique_integer([:positive, :monotonic]) end)

        if Map.has_key?(state.jobs, id) do
          {:reply, {:error, :duplicate}, state}
        else
          overrides = Keyword.merge(Keyword.take(declared, @keys), Keyword.take(opts, @keys))
          config = Map.merge(@defaults, Map.new(overrides))

          job = %{
            id: id,
            name: job_name,
            payload: payload,
            config: config,
            status: :pending,
            attempts: 0,
            result: nil,
            error: nil
          }

          state = %{
            state
            | jobs: Map.put(state.jobs, id, job),
              pending: :queue.in(id, state.pending)
          }

          {:reply, {:ok, id}, dispatch(state)}
        end
    end
  end

  def handle_call({:status, id}, _from, state) do
    reply =
      case Map.fetch(state.jobs, id) do
        {:ok, job} -> {:ok, job}
        :error -> {:error, :not_found}
      end

    {:reply, reply, state}
  end

  def handle_call(:stats, _from, state) do
    counts = Enum.frequencies_by(Map.values(state.jobs), & &1.status)
    {:reply, Map.put(counts, :running_workers, map_size(state.running)), state}
  end

  def handle_call({:subscribe, pid}, _from, state) do
    subs = Map.put_new_lazy(state.subscribers, pid, fn -> Process.monitor(pid) end)
    {:reply, :ok, %{state | subscribers: subs}}
  end

  def handle_call({:unsubscribe, pid}, _from, state) do
    case Map.pop(state.subscribers, pid) do
      {nil, _} ->
        {:reply, :ok, state}

      {ref, subs} ->
        Process.demonitor(ref, [:flush])
        {:reply, :ok, %{state | subscribers: subs}}
    end
  end

  # Resultado reportado pelo worker. Como o worker envia a mensagem antes de
  # encerrar, ela sempre chega antes do :DOWN correspondente.
  @impl true
  def handle_info({:tq_result, pid, result}, state) do
    case Map.pop(state.running, pid) do
      {{id, ref}, running} ->
        Process.demonitor(ref, [:flush])

        state =
          %{state | running: running}
          |> settle(id, result)

        {:noreply, dispatch(state)}

      {nil, _} ->
        {:noreply, state}
    end
  end

  # Worker morreu sem reportar resultado: conta como falha temporária.
  def handle_info({:DOWN, _ref, :process, pid, reason}, state) do
    case Map.pop(state.running, pid) do
      {{id, _ref}, running} ->
        state =
          %{state | running: running}
          |> settle(id, {:retry, {:worker_crashed, reason}})

        {:noreply, dispatch(state)}

      {nil, _} ->
        # Pode ser um assinante que caiu.
        {:noreply, %{state | subscribers: Map.delete(state.subscribers, pid)}}
    end
  end

  # Timer de re-tentativa disparado: devolve o job para a fila de pendentes.
  def handle_info({:tq_retry, id}, state) do
    case Map.fetch(state.jobs, id) do
      {:ok, %{status: :retrying} = job} ->
        jobs = Map.put(state.jobs, id, %{job | status: :pending})
        state = %{state | jobs: jobs, pending: :queue.in(id, state.pending)}
        {:noreply, dispatch(state)}

      _ ->
        {:noreply, state}
    end
  end

  ## Despacho e transições de estado

  # Respeita o limite de concorrência.
  defp dispatch(%{running: running, max_concurrency: max} = state)
       when map_size(running) >= max,
       do: state

  defp dispatch(state) do
    case :queue.out(state.pending) do
      {:empty, _} ->
        state

      {{:value, id}, pending} ->
        job =
          state.jobs
          |> Map.fetch!(id)
          |> Map.update!(:attempts, &(&1 + 1))
          |> Map.put(:status, :running)

        server = self()
        mod = state.mod

        # O worker é monitorado; o resultado volta como mensagem para o servidor.
        {pid, ref} =
          spawn_monitor(fn ->
            send(server, {:tq_result, self(), run(mod, job)})
          end)

        state = %{
          state
          | pending: pending,
            jobs: Map.put(state.jobs, id, job),
            running: Map.put(state.running, pid, {id, ref})
        }

        dispatch(state)
    end
  end

  # Executa o job com timeout, isolando exceções e exits.
  defp run(mod, %{name: name, payload: payload, config: %{timeout: timeout}}) do
    task =
      Task.async(fn ->
        try do
          normalize(apply(mod, name, [payload]))
        rescue
          e -> {:retry, Exception.message(e)}
        catch
          kind, reason -> {:retry, {kind, reason}}
        end
      end)

    case Task.yield(task, timeout) || Task.shutdown(task, :brutal_kill) do
      {:ok, result} -> result
      {:exit, reason} -> {:retry, {:exit, reason}}
      nil -> {:retry, :timeout}
    end
  end

  # Normaliza o contrato de retorno do job.
  defp normalize(:ok), do: {:ok, :ok}
  defp normalize({:ok, value}), do: {:ok, value}
  defp normalize({:error, reason}), do: {:retry, reason}
  defp normalize({:fatal, reason}), do: {:fatal, reason}
  defp normalize(other), do: {:fatal, {:invalid_return, other}}

  defp settle(state, id, {:ok, value}) do
    transition(state, id, &%{&1 | status: :done, result: value}, {:completed, id, value})
  end

  defp settle(state, id, {:fatal, reason}), do: dead(state, id, reason)

  defp settle(state, id, {:retry, reason}) do
    job = Map.fetch!(state.jobs, id)

    if job.attempts < job.config.max_attempts do
      delay = backoff(job)
      Process.send_after(self(), {:tq_retry, id}, delay)

      transition(
        state,
        id,
        &%{&1 | status: :retrying, error: reason},
        {:retrying, id, reason, delay}
      )
    else
      dead(state, id, reason)
    end
  end

  defp dead(state, id, reason) do
    transition(state, id, &%{&1 | status: :dead, error: reason}, {:dead, id, reason})
  end

  # Atualiza o job e notifica os assinantes.
  defp transition(state, id, fun, event) do
    state = %{state | jobs: Map.update!(state.jobs, id, fun)}
    Enum.each(Map.keys(state.subscribers), &send(&1, {:tq_event, event}))
    state
  end

  # Backoff exponencial com jitter, limitado a @max_backoff.
  defp backoff(%{attempts: n, config: %{backoff: base}}) do
    base = max(base, 1)
    delay = trunc(base * :math.pow(2, n - 1)) + :rand.uniform(base)
    min(delay, @max_backoff)
  end
end

defmodule TaskQueue do
  @moduledoc """
  Interface pública da fila de tarefas.

      {:ok, q} = TaskQueue.start_link(jobs: TaskQueue.Jobs, max_concurrency: 4)
      {:ok, id} = TaskQueue.enqueue(q, :send_email, %{to: "a@b.c"})
      TaskQueue.await(q, id, 10_000)
  """

  alias TaskQueue.Server

  defdelegate start_link(opts), to: Server
  defdelegate enqueue(server, job, payload, opts \\ []), to: Server
  defdelegate status(server, id), to: Server
  defdelegate stats(server), to: Server
  defdelegate subscribe(server), to: Server
  defdelegate unsubscribe(server), to: Server
  defdelegate await(server, id, timeout \\ 5_000), to: Server
end

defmodule TaskQueue.Jobs do
  @moduledoc false
  # Exemplo de declaração de jobs usando as macros da DSL.

  use TaskQueue.DSL

  defjob :send_email, max_attempts: 5 do
    case payload do
      %{to: to} when is_binary(to) -> {:ok, "enviado para #{to}"}
      _ -> {:fatal, :invalid_payload}
    end
  end

  defjob :flaky_io, backoff: 50 do
    # Simula falha temporária em aproximadamente metade das chamadas.
    if :rand.uniform() < 0.5, do: {:error, :timeout}, else: {:ok, payload}
  end

  defjob :sum do
    Enum.sum(payload)
  end
end
