# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de votação online, atendendo somente aos seguintes requisitos:
#
# 1. Recuperar processos após falhas inesperadas.
# 2. Persistir os dados para recuperação após reinicializações.
# 3. Permitir que diferentes entidades sejam processadas independentemente.
# 4. Permitir a execução de operações simultâneas.
# 5. Diferenciar erros de validação de falhas operacionais.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# ============================================================================
# Sistema de votação online tolerante a falhas (arquivo único, stdlib pura)
#
# Arquitetura:
#   * Cada enquete é um processo GenServer isolado (VotingSystem.Poll),
#     registrado via Registry e supervisionado por um DynamicSupervisor.
#     Falhas em uma enquete não afetam as demais.
#   * Cada voto é persistido em DETS (stdlib) ANTES de ser respondido.
#     Se a persistência falhar, o estado em memória não é alterado.
#   * Após falha ou reinicialização, o estado é recarregado do DETS.
#   * Votos em enquetes diferentes são processados em paralelo; votos na
#     mesma enquete são serializados pelo seu próprio processo.
#   * Erros são classificados em:
#       {:error, {:validation, motivo}}   -> entrada inválida do usuário
#       {:error, {:operational, motivo}}  -> falha de infraestrutura/sistema
# ============================================================================

defmodule VotingSystem do
  @moduledoc """
  API pública do sistema de votação.

  Retornos de sucesso: `:ok`, `{:ok, valor}`.
  Retornos de erro:    `{:error, {:validation, motivo}}` ou `{:error, {:operational, motivo}}`.
  """

  alias VotingSystem.{Poll, Store}

  @registry VotingSystem.Registry
  @supervisor VotingSystem.PollSupervisor
  @call_timeout 5_000

  @doc "Cria uma nova enquete com ao menos duas opções distintas."
  def create_poll(id, title, options) do
    with :ok <- validate_spec(id, title, options),
         :ok <- ensure_absent(id),
         :ok <- Store.put(id, %Poll{id: id, title: title, options: init_options(options)}),
         {:ok, _pid} <- start_poll(id) do
      {:ok, id}
    end
  end

  @doc "Registra um voto de `voter` na opção `option` da enquete `poll_id`."
  def vote(poll_id, voter, option), do: call(poll_id, {:vote, voter, option})

  @doc "Retorna a apuração parcial da enquete."
  def results(poll_id), do: call(poll_id, :results)

  @doc "Encerra a votação de uma enquete."
  def close_poll(poll_id), do: call(poll_id, :close)

  @doc false
  # Inicia (ou localiza) o processo de uma enquete já existente no armazenamento.
  def start_poll(id) do
    case DynamicSupervisor.start_child(@supervisor, {Poll, id}) do
      {:ok, pid} -> {:ok, pid}
      {:error, {:already_started, pid}} -> {:ok, pid}
      {:error, reason} -> {:error, {:operational, {:start_failed, reason}}}
    end
  end

  # ---------------------------------------------------------------------------
  # Internos
  # ---------------------------------------------------------------------------

  defp call(poll_id, request) do
    case Registry.lookup(@registry, poll_id) do
      [{pid, _}] ->
        try do
          GenServer.call(pid, request, @call_timeout)
        catch
          # Timeout, processo morto durante a chamada, etc. são falhas operacionais.
          :exit, reason -> {:error, {:operational, {:poll_unavailable, reason}}}
        end

      [] ->
        # Enquete não está em execução: distingue inexistente de não recuperada.
        case Store.get(poll_id) do
          :not_found -> {:error, {:validation, :unknown_poll}}
          {:ok, _} -> {:error, {:operational, :poll_not_running}}
          {:error, _} = error -> error
        end
    end
  end

  defp validate_spec(id, title, options)
       when is_binary(id) and byte_size(id) > 0 and
              is_binary(title) and byte_size(title) > 0 and
              is_list(options) do
    opts = Enum.uniq(options)

    cond do
      length(opts) < 2 -> {:error, {:validation, :not_enough_options}}
      not Enum.all?(opts, &(is_binary(&1) and byte_size(&1) > 0)) -> {:error, {:validation, :invalid_options}}
      true -> :ok
    end
  end

  defp validate_spec(_, _, _), do: {:error, {:validation, :invalid_poll}}

  defp ensure_absent(id) do
    case Store.get(id) do
      :not_found -> :ok
      {:ok, _} -> {:error, {:validation, :already_exists}}
      {:error, _} = error -> error
    end
  end

  defp init_options(options) do
    options |> Enum.uniq() |> Map.new(&{&1, 0})
  end
end

# ============================================================================
# Processo de uma enquete: estado isolado, persistido a cada mutação.
# ============================================================================
defmodule VotingSystem.Poll do
  @moduledoc false
  use GenServer, restart: :permanent

  alias VotingSystem.Store

  defstruct [:id, :title, options: %{}, voters: MapSet.new(), status: :open]

  def start_link(id), do: GenServer.start_link(__MODULE__, id, name: via(id))

  defp via(id), do: {:via, Registry, {VotingSystem.Registry, id}}

  @impl true
  def init(id) do
    # Recuperação: o estado é sempre lido do armazenamento durável.
    case Store.get(id) do
      {:ok, poll} -> {:ok, poll}
      :not_found -> {:stop, :unknown_poll}
      {:error, reason} -> {:stop, reason}
    end
  end

  @impl true
  def handle_call({:vote, voter, option}, _from, poll) do
    with :ok <- check_voter(voter),
         :ok <- check_open(poll),
         :ok <- check_option(poll, option),
         :ok <- check_not_voted(poll, voter) do
      next = %{
        poll
        | options: Map.update!(poll.options, option, &(&1 + 1)),
          voters: MapSet.put(poll.voters, voter)
      }

      commit(poll, next, :ok)
    else
      {:error, _} = error -> {:reply, error, poll}
    end
  end

  def handle_call(:close, _from, %{status: :closed} = poll) do
    {:reply, {:error, {:validation, :already_closed}}, poll}
  end

  def handle_call(:close, _from, poll) do
    commit(poll, %{poll | status: :closed}, :ok)
  end

  def handle_call(:results, _from, poll) do
    summary = %{
      id: poll.id,
      title: poll.title,
      status: poll.status,
      tally: poll.options,
      total_votes: MapSet.size(poll.voters)
    }

    {:reply, {:ok, summary}, poll}
  end

  # Persiste ANTES de responder. Se a persistência falhar, o estado anterior
  # é mantido e o erro operacional é devolvido ao chamador.
  defp commit(current, next, reply) do
    case Store.put(next.id, next) do
      :ok -> {:reply, reply, next}
      {:error, _} = error -> {:reply, error, current}
    end
  end

  # Validações de entrada (erros de validação)
  defp check_voter(voter) when is_binary(voter) and byte_size(voter) > 0, do: :ok
  defp check_voter(_), do: {:error, {:validation, :invalid_voter}}

  defp check_open(%{status: :open}), do: :ok
  defp check_open(_), do: {:error, {:validation, :poll_closed}}

  defp check_option(%{options: options}, option) do
    if Map.has_key?(options, option),
      do: :ok,
      else: {:error, {:validation, :unknown_option}}
  end

  defp check_not_voted(%{voters: voters}, voter) do
    if MapSet.member?(voters, voter),
      do: {:error, {:validation, :already_voted}},
      else: :ok
  end
end

# ============================================================================
# Persistência durável com DETS (stdlib). O processo mantém a tabela aberta;
# se ele cair, a tabela é fechada e reaberta pelo supervisor.
# ============================================================================
defmodule VotingSystem.Store do
  @moduledoc false
  use GenServer

  @table :voting_polls

  def start_link(_), do: GenServer.start_link(__MODULE__, :ok, name: __MODULE__)

  @impl true
  def init(:ok) do
    path = Application.get_env(:voting_system, :data_file, "data/voting_polls.dets")
    File.mkdir_p!(Path.dirname(path))

    case :dets.open_file(@table, file: String.to_charlist(path), type: :set) do
      {:ok, _} -> {:ok, :no_state}
      {:error, reason} -> {:stop, {:dets_open_failed, reason}}
    end
  end

  @doc "Grava `value` sob `id` e força sincronização em disco."
  def put(id, value) do
    with {:ok, true} <- dets(fn -> :dets.insert(@table, {id, value}) end),
         {:ok, :ok} <- dets(fn -> :dets.sync(@table) end) do
      :ok
    end
  end

  @doc "Recupera o valor associado a `id`."
  def get(id) do
    case dets(fn -> :dets.lookup(@table, id) end) do
      {:ok, [{^id, value}]} -> {:ok, value}
      {:ok, []} -> :not_found
      {:error, _} = error -> error
    end
  end

  @doc "Lista todos os identificadores persistidos."
  def ids do
    dets(fn -> :dets.foldl(fn {id, _}, acc -> [id | acc] end, [], @table) end)
  end

  defp dets(fun) do
    case fun.() do
      {:error, reason} -> {:error, {:operational, reason}}
      result -> {:ok, result}
    end
  rescue
    e -> {:error, {:operational, {:store_unavailable, e}}}
  catch
    :exit, reason -> {:error, {:operational, {:store_unavailable, reason}}}
  end
end

# ============================================================================
# Árvore de supervisão e recuperação na inicialização.
# rest_for_one: se o Store cair, tudo depois dele é reiniciado e a
# recuperação é executada novamente.
# ============================================================================
defmodule VotingSystem.Application do
  @moduledoc false
  use Application

  @impl true
  def start(_type, _args) do
    children = [
      VotingSystem.Store,
      {Registry, keys: :unique, name: VotingSystem.Registry},
      {DynamicSupervisor, strategy: :one_for_one, name: VotingSystem.PollSupervisor},
      %{
        id: :recovery,
        start: {Task, :start_link, [&recover/0]},
        restart: :transient
      }
    ]

    Supervisor.start_link(children, strategy: :rest_for_one, name: VotingSystem.Supervisor)
  end

  @doc false
  # Reinicia todas as enquetes persistidas. Falhas levantam exceção para que o
  # supervisor tente novamente (falha operacional recuperável).
  def recover do
    {:ok, ids} = VotingSystem.Store.ids()
    Enum.each(ids, fn id -> {:ok, _pid} = VotingSystem.start_poll(id) end)
  end
end

# ============================================================================
# Demonstração: concorrência, validação, falha simulada e recuperação.
# Execução: VotingSystem.Demo.run()
# ============================================================================
defmodule VotingSystem.Demo do
  @moduledoc false

  def run do
    {:ok, _} = VotingSystem.Application.start(:normal, [])

    {:ok, _} = VotingSystem.create_poll("eleicao-1", "Eleição", ["Sim", "Não", "Abstenção"])
    {:ok, _} = VotingSystem.create_poll("eleicao-2", "Pesquisa", ["A", "B"])

    options = ["Sim", "Não", "Abstenção"]

    # Votos simultâneos na mesma enquete (serializados pelo processo da enquete)
    # e em enquetes diferentes (processados em paralelo).
    1..500
    |> Task.async_stream(
      fn i ->
        VotingSystem.vote("eleicao-1", "eleitor-#{i}", Enum.random(options))
        VotingSystem.vote("eleicao-2", "eleitor-#{i}", Enum.random(["A", "B"]))
      end,
      max_concurrency: 50
    )
    |> Enum.to_list()

    IO.inspect(VotingSystem.results("eleicao-1"), label: "apuração eleicao-1")

    # Erros de validação
    IO.inspect(VotingSystem.vote("eleicao-1", "eleitor-1", "Sim"), label: "voto duplicado")
    IO.inspect(VotingSystem.vote("eleicao-1", "eleitor-x", "Talvez"), label: "opção inválida")
    IO.inspect(VotingSystem.vote("inexistente", "eleitor-x", "Sim"), label: "enquete inexistente")

    # Falha inesperada: mata o processo da enquete; o supervisor o recria
    # a partir do DETS, sem perda de votos confirmados.
    [{pid, _}] = Registry.lookup(VotingSystem.Registry, "eleicao-1")
    Process.exit(pid, :kill)
    Process.sleep(100)

    IO.inspect(VotingSystem.results("eleicao-1"), label: "após recuperação")
    IO.inspect(VotingSystem.vote("eleicao-1", "eleitor-new", "Não"), label: "voto após falha")

    :ok
  end
end
