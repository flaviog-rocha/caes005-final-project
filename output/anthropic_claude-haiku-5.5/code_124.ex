# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de votação online, atendendo somente aos seguintes requisitos:
#
# 1. Gerar notificações quando determinadas condições forem atingidas.
# 2. Permitir limitar a quantidade de operações simultâneas.
# 3. Garantir a consistência dos dados durante operações concorrentes.
# 4. Implementar novas tentativas após falhas temporárias.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - Task
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# Sistema de votação online em Elixir (arquivo único autocontido).
#
# Execute com: elixir voting_system.exs
#
# Requisitos atendidos:
# 1. Notificações: eventos (quórum atingido e maioria atingida) são publicados
#    para os processos inscritos via Registry, em Tasks assíncronas.
# 2. Limite de operações simultâneas: VotingSystem.Limiter é um semáforo
#    baseado em GenServer que limita quantas Tasks executam votos ao mesmo tempo.
# 3. Consistência: VotingSystem.Ballot é um GenServer que serializa todas as
#    alterações de estado. Validação, verificação de voto duplicado e
#    atualização da contagem ocorrem de forma atômica.
# 4. Novas tentativas: VotingSystem.Retry reexecuta operações que falharam
#    temporariamente, com backoff exponencial e jitter.
# Recurso do Elixir utilizado: Task (Task.Supervisor, Task.async_stream, Task.await).

defmodule VotingSystem do
  @moduledoc """
  Ponto de entrada do sistema de votação.

  Inicia uma árvore de supervisão com:
    * Registry: inscrição de processos para notificações;
    * Task.Supervisor: executa as Tasks de votação e de notificação;
    * Limiter: semáforo que limita operações simultâneas;
    * Ballot: estado consistente das votações.
  """

  use Supervisor

  @registry VotingSystem.Registry
  @task_sup VotingSystem.TaskSup
  @vote_timeout 15_000

  # ---------------------------------------------------------------------------
  # Inicialização
  # ---------------------------------------------------------------------------

  @doc """
  Inicia o sistema.

  Opções:
    * `:max_concurrency` - número máximo de votos processados simultaneamente (padrão: 4)
    * `:fail_rate` - probabilidade (0.0 a 1.0) de falha temporária simulada (padrão: 0.0)
  """
  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    children = [
      {Registry, keys: :duplicate, name: @registry},
      {Task.Supervisor, name: @task_sup},
      {VotingSystem.Limiter, max: Keyword.get(opts, :max_concurrency, 4)},
      {VotingSystem.Ballot, fail_rate: Keyword.get(opts, :fail_rate, 0.0)}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  # ---------------------------------------------------------------------------
  # API pública
  # ---------------------------------------------------------------------------

  @doc "Cria uma votação com opções e quórum (número de votos que dispara a notificação)."
  def create_poll(id, options, quorum) do
    VotingSystem.Ballot.create_poll(id, options, quorum)
  end

  @doc "Encerra uma votação, impedindo novos votos."
  def close_poll(id), do: VotingSystem.Ballot.close_poll(id)

  @doc "Retorna a contagem atual da votação."
  def results(id), do: VotingSystem.Ballot.results(id)

  @doc "Inscreve o processo chamador para receber notificações da votação."
  def subscribe(id) do
    Registry.register(@registry, {:poll, id}, nil)
  end

  @doc """
  Registra um voto.

  A operação é executada em uma Task, respeita o limite de concorrência
  do Limiter e é repetida automaticamente em caso de falha temporária.
  """
  def vote(id, voter, option) do
    task =
      Task.Supervisor.async_nolink(@task_sup, fn ->
        VotingSystem.Limiter.run(fn ->
          VotingSystem.Retry.run(fn ->
            VotingSystem.Ballot.register_vote(id, voter, option)
          end)
        end)
      end)

    case Task.yield(task, @vote_timeout) || Task.shutdown(task, :brutal_kill) do
      {:ok, result} -> result
      {:exit, reason} -> {:error, reason}
      nil -> {:error, :timeout}
    end
  end

  @doc """
  Processa uma lista de votos `{id, eleitor, opção}` em paralelo,
  com no máximo `max_concurrency` Tasks ativas.
  """
  def cast_votes(votes, max_concurrency \\ 8) do
    votes
    |> Task.async_stream(
      fn {id, voter, option} -> vote(id, voter, option) end,
      max_concurrency: max_concurrency,
      timeout: 30_000,
      ordered: true
    )
    |> Enum.map(fn
      {:ok, result} -> result
      {:exit, reason} -> {:error, reason}
    end)
  end
end

defmodule VotingSystem.Limiter do
  @moduledoc """
  Semáforo baseado em GenServer que limita o número de operações simultâneas.

  Monitora os processos que adquirem vagas: se um deles morrer sem liberar
  a vaga, ela é recuperada automaticamente.
  """

  use GenServer

  def start_link(opts) do
    GenServer.start_link(__MODULE__, Keyword.fetch!(opts, :max), name: __MODULE__)
  end

  @doc "Executa `fun` ocupando uma vaga do semáforo. Bloqueia enquanto não houver vaga."
  def run(fun, timeout \\ :infinity) do
    ref = GenServer.call(__MODULE__, :acquire, timeout)

    try do
      fun.()
    after
      GenServer.cast(__MODULE__, {:release, ref})
    end
  end

  @impl true
  def init(max) do
    {:ok, %{max: max, holders: %{}, waiting: :queue.new()}}
  end

  @impl true
  def handle_call(:acquire, {pid, _tag} = from, state) do
    ref = Process.monitor(pid)

    if map_size(state.holders) < state.max do
      {:reply, ref, %{state | holders: Map.put(state.holders, ref, pid)}}
    else
      # Sem vaga: a resposta será enviada depois, com GenServer.reply/2.
      {:noreply, %{state | waiting: :queue.in({from, ref}, state.waiting)}}
    end
  end

  @impl true
  def handle_cast({:release, ref}, state) do
    Process.demonitor(ref, [:flush])
    state = %{state | holders: Map.delete(state.holders, ref)}
    {:noreply, grant_next(state)}
  end

  @impl true
  def handle_info({:DOWN, ref, :process, _pid, _reason}, state) do
    state =
      if Map.has_key?(state.holders, ref) do
        %{state | holders: Map.delete(state.holders, ref)}
      else
        # Processo morreu enquanto esperava na fila.
        %{state | waiting: :queue.filter(fn {_from, r} -> r != ref end, state.waiting)}
      end

    {:noreply, grant_next(state)}
  end

  # Concede vagas livres aos próximos da fila.
  defp grant_next(%{holders: holders, max: max} = state) when map_size(holders) < max do
    case :queue.out(state.waiting) do
      {{:value, {{pid, _tag} = from, ref}}, rest} ->
        GenServer.reply(from, ref)
        grant_next(%{state | waiting: rest, holders: Map.put(holders, ref, pid)})

      {:empty, _} ->
        state
    end
  end

  defp grant_next(state), do: state
end

defmodule VotingSystem.Ballot do
  @moduledoc """
  Estado das votações, mantido por um único GenServer.

  Todas as leituras e escritas são serializadas pelo processo, o que garante
  consistência: um eleitor não consegue votar duas vezes na mesma votação,
  mesmo com chamadas concorrentes, e a contagem nunca fica inconsistente.

  Falhas temporárias são simuladas antes de qualquer alteração de estado,
  então uma nova tentativa nunca aplica o mesmo voto duas vezes.
  """

  use GenServer

  # ---------------------------------------------------------------------------
  # API
  # ---------------------------------------------------------------------------

  def start_link(opts) do
    GenServer.start_link(__MODULE__, Keyword.get(opts, :fail_rate, 0.0), name: __MODULE__)
  end

  def create_poll(id, options, quorum),
    do: GenServer.call(__MODULE__, {:create_poll, id, options, quorum})

  def close_poll(id), do: GenServer.call(__MODULE__, {:close_poll, id})

  def results(id), do: GenServer.call(__MODULE__, {:results, id})

  def register_vote(id, voter, option),
    do: GenServer.call(__MODULE__, {:vote, id, voter, option})

  # ---------------------------------------------------------------------------
  # Callbacks
  # ---------------------------------------------------------------------------

  @impl true
  def init(fail_rate), do: {:ok, %{polls: %{}, fail_rate: fail_rate}}

  @impl true
  def handle_call({:create_poll, id, options, quorum}, _from, state) do
    if Map.has_key?(state.polls, id) do
      {:reply, {:error, :already_exists}, state}
    else
      poll = %{
        options: Map.new(options, &{&1, 0}),
        quorum: quorum,
        total: 0,
        voters: MapSet.new(),
        open?: true,
        # Conjunto de condições já notificadas, para notificar apenas uma vez.
        notified: MapSet.new()
      }

      {:reply, :ok, put_poll(state, id, poll)}
    end
  end

  def handle_call({:close_poll, id}, _from, state) do
    case Map.fetch(state.polls, id) do
      {:ok, poll} -> {:reply, :ok, put_poll(state, id, %{poll | open?: false})}
      :error -> {:reply, {:error, :unknown_poll}, state}
    end
  end

  def handle_call({:results, id}, _from, state) do
    case Map.fetch(state.polls, id) do
      {:ok, poll} ->
        {:reply, {:ok, %{tally: poll.options, total: poll.total, open?: poll.open?}}, state}

      :error ->
        {:reply, {:error, :unknown_poll}, state}
    end
  end

  def handle_call({:vote, id, voter, option}, _from, state) do
    if simulated_failure?(state) do
      # Falha temporária antes de qualquer mudança: é seguro tentar novamente.
      {:reply, {:error, :temporary_failure}, state}
    else
      case validate_and_apply(state, id, voter, option) do
        {:ok, new_state} -> {:reply, :ok, new_state}
        {:error, reason} -> {:reply, {:error, reason}, state}
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Funções privadas
  # ---------------------------------------------------------------------------

  defp validate_and_apply(state, id, voter, option) do
    with {:ok, poll} <- fetch(state, id),
         :ok <- ensure_open(poll),
         :ok <- ensure_option(poll, option),
         :ok <- ensure_not_voted(poll, voter) do
      poll = %{
        poll
        | voters: MapSet.put(poll.voters, voter),
          options: Map.update!(poll.options, option, &(&1 + 1)),
          total: poll.total + 1
      }

      {poll, events} = evaluate_conditions(id, poll, option)
      Enum.each(events, &notify(id, &1))

      {:ok, put_poll(state, id, poll)}
    end
  end

  # Condições monitoradas:
  #  * :quorum_reached   - total de votos atingiu o quórum;
  #  * :majority_reached - uma opção ultrapassou metade do quórum.
  defp evaluate_conditions(id, poll, option) do
    candidates = [
      {poll.total >= poll.quorum, :quorum,
       {:quorum_reached, id, poll.options}},
      {poll.options[option] * 2 > poll.quorum, {:majority, option},
       {:majority_reached, id, option, poll.options[option]}}
    ]

    Enum.reduce(candidates, {poll, []}, fn {met?, key, event}, {acc, events} ->
      if met? and not MapSet.member?(acc.notified, key) do
        {%{acc | notified: MapSet.put(acc.notified, key)}, [event | events]}
      else
        {acc, events}
      end
    end)
  end

  # Publica o evento em uma Task separada, sem bloquear o GenServer.
  defp notify(id, event) do
    Task.Supervisor.start_child(VotingSystem.TaskSup, fn ->
      Registry.dispatch(VotingSystem.Registry, {:poll, id}, fn subscribers ->
        for {pid, _} <- subscribers, do: send(pid, {:voting_event, event})
      end)
    end)
  end

  defp fetch(state, id) do
    case Map.fetch(state.polls, id) do
      {:ok, poll} -> {:ok, poll}
      :error -> {:error, :unknown_poll}
    end
  end

  defp ensure_open(%{open?: true}), do: :ok
  defp ensure_open(_poll), do: {:error, :closed}

  defp ensure_option(poll, option) do
    if Map.has_key?(poll.options, option), do: :ok, else: {:error, :invalid_option}
  end

  defp ensure_not_voted(poll, voter) do
    if MapSet.member?(poll.voters, voter), do: {:error, :already_voted}, else: :ok
  end

  defp simulated_failure?(%{fail_rate: rate}) when rate > 0, do: :rand.uniform() < rate
  defp simulated_failure?(_state), do: false

  defp put_poll(state, id, poll), do: %{state | polls: Map.put(state.polls, id, poll)}
end

defmodule VotingSystem.Retry do
  @moduledoc """
  Reexecuta operações que falharam temporariamente, com backoff exponencial
  e jitter. Apenas o retorno `{:error, :temporary_failure}` é repetido.
  """

  @default_attempts 4
  @base_delay_ms 20

  def run(fun, opts \\ []) do
    attempts = Keyword.get(opts, :attempts, @default_attempts)
    base = Keyword.get(opts, :base_delay_ms, @base_delay_ms)

    do_run(fun, 1, attempts, base)
  end

  defp do_run(fun, attempt, max_attempts, base) do
    case fun.() do
      {:error, :temporary_failure} when attempt < max_attempts ->
        backoff = base * Integer.pow(2, attempt - 1) + :rand.uniform(base)
        Process.sleep(backoff)
        do_run(fun, attempt + 1, max_attempts, base)

      result ->
        result
    end
  end
end

defmodule VotingSystem.Demo do
  @moduledoc "Demonstração executável do sistema."

  def run do
    {:ok, _pid} = VotingSystem.start_link(max_concurrency: 3, fail_rate: 0.3)

    :ok = VotingSystem.create_poll("eleicao", ["A", "B", "C"], 10)
    {:ok, _} = VotingSystem.subscribe("eleicao")

    votes =
      for i <- 1..12 do
        {"eleicao", "eleitor_#{i}", Enum.random(["A", "B", "C"])}
      end

    results = VotingSystem.cast_votes(votes)
    IO.inspect(Enum.frequencies(results), label: "Resultados das operações")

    IO.inspect(VotingSystem.vote("eleicao", "eleitor_1", "A"), label: "Voto duplicado")
    IO.inspect(VotingSystem.vote("eleicao", "eleitor_99", "X"), label: "Opção inválida")

    IO.inspect(VotingSystem.results("eleicao"), label: "Contagem final")

    collect_notifications()
  end

  defp collect_notifications do
    receive do
      {:voting_event, event} ->
        IO.inspect(event, label: "Notificação")
        collect_notifications()
    after
      500 -> :ok
    end
  end
end

VotingSystem.Demo.run()
