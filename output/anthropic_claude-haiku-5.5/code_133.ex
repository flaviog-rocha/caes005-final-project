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

# sistema_votacao.exs
# Execução: elixir sistema_votacao.exs
#
# Arquitetura:
#   - VotingSystem (Supervisor): árvore de supervisão one_for_one.
#   - VotingSystem.Store: dono da tabela DETS (persistência em disco, stdlib :dets).
#   - VotingSystem.Registry: nomeia um processo por enquete (entidades independentes).
#   - DynamicSupervisor: supervisiona um GenServer por enquete (recuperação de falhas).
#   - Restore: ao iniciar, reidrata todas as enquetes persistidas.
#
# Erros são classificados em:
#   - {:error, {:validation, motivo}}   -> entrada inválida (culpa do chamador, sem efeito colateral)
#   - {:error, {:operational, motivo}}  -> falha de infraestrutura/processo (recuperável/retentável)

defmodule VotingSystem do
  @moduledoc false
  use Supervisor

  alias VotingSystem.{Poll, Store}

  @registry VotingSystem.Registry
  @poll_sup VotingSystem.PollSupervisor
  @call_timeout 5_000

  # ---------------------------------------------------------------------------
  # Supervisão
  # ---------------------------------------------------------------------------

  # Inicia o sistema. `path` é o arquivo DETS onde os dados são persistidos.
  def start_link(opts) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    path = Keyword.fetch!(opts, :path)

    children = [
      # Registro de processos por nome (uma entrada por enquete).
      {Registry, keys: :unique, name: @registry},
      # Dono da persistência. Se cair, é reiniciado e reabre o arquivo.
      {Store, path},
      # Supervisor dinâmico: cada enquete é um processo isolado.
      {DynamicSupervisor, strategy: :one_for_one, name: @poll_sup},
      # Reidratação das enquetes após reinicialização.
      %{
        id: :restore,
        start: {Task, :start_link, [__MODULE__, :restore, []]},
        restart: :transient
      }
    ]

    # :one_for_one: uma falha isolada não derruba outras partes do sistema.
    Supervisor.init(children, strategy: :one_for_one)
  end

  # Reinicia (a partir da persistência) todas as enquetes salvas.
  @doc false
  def restore do
    case Store.poll_ids() do
      {:ok, ids} ->
        Enum.each(ids, fn id -> start_poll(id) end)
        :ok

      {:error, reason} ->
        exit({:operational, reason})
    end
  end

  # ---------------------------------------------------------------------------
  # API pública
  # ---------------------------------------------------------------------------

  # Cria uma enquete. Retorna :ok ou um erro classificado.
  def create_poll(id, question, options) do
    with :ok <- validate_create(id, question, options) do
      state = Poll.new(id, question, options)

      case Store.insert_new({:poll, id}, state) do
        :ok ->
          # Persistida com sucesso; se o processo não subir, será restaurado no próximo boot.
          case start_poll(id) do
            {:ok, _pid} -> :ok
            {:error, reason} -> {:error, reason}
          end

        :exists ->
          {:error, {:validation, :already_exists}}

        {:error, reason} ->
          {:error, {:operational, reason}}
      end
    end
  end

  # Registra um voto. Cada enquete é processada por seu próprio processo.
  def vote(poll_id, voter, option)
      when is_binary(poll_id) and is_binary(voter) and is_binary(option) do
    with {:ok, pid} <- ensure_running(poll_id) do
      call_poll(pid, {:vote, voter, option})
    end
  end

  def vote(_poll_id, _voter, _option),
    do: {:error, {:validation, :invalid_arguments}}

  # Retorna o estado de apuração da enquete.
  def tally(poll_id) when is_binary(poll_id) do
    with {:ok, pid} <- ensure_running(poll_id) do
      call_poll(pid, :tally)
    end
  end

  def tally(_poll_id), do: {:error, {:validation, :invalid_arguments}}

  # ---------------------------------------------------------------------------
  # Funções privadas
  # ---------------------------------------------------------------------------

  # Garante que o processo da enquete esteja ativo; se não estiver, reidrata-o
  # a partir da persistência (recuperação sob demanda).
  defp ensure_running(id) do
    case Registry.lookup(@registry, id) do
      [{pid, _}] ->
        {:ok, pid}

      [] ->
        case Store.get({:poll, id}) do
          {:ok, _state} -> start_poll(id)
          :not_found -> {:error, {:validation, :poll_not_found}}
          {:error, reason} -> {:error, {:operational, reason}}
        end
    end
  end

  defp start_poll(id) do
    case DynamicSupervisor.start_child(@poll_sup, {Poll, %{id: id}}) do
      {:ok, pid} -> {:ok, pid}
      {:error, {:already_started, pid}} -> {:ok, pid}
      {:error, reason} -> {:error, {:operational, reason}}
    end
  end

  # Converte saídas abruptas do processo (crash, timeout) em erro operacional.
  defp call_poll(pid, msg) do
    GenServer.call(pid, msg, @call_timeout)
  catch
    :exit, reason -> {:error, {:operational, reason}}
  end

  defp validate_create(id, question, options) do
    cond do
      not (is_binary(id) and id != "") ->
        {:error, {:validation, :invalid_id}}

      not (is_binary(question) and String.trim(question) != "") ->
        {:error, {:validation, :invalid_question}}

      not valid_options?(options) ->
        {:error, {:validation, :invalid_options}}

      true ->
        :ok
    end
  end

  # Pelo menos duas opções, todas strings não vazias e distintas.
  defp valid_options?(options) do
    is_list(options) and
      length(options) >= 2 and
      Enum.all?(options, &(is_binary(&1) and &1 != "")) and
      length(Enum.uniq(options)) == length(options)
  end
end

defmodule VotingSystem.Store do
  @moduledoc false
  # Persistência em DETS (stdlib). Um único dono da tabela, serializando escritas.
  use GenServer

  @table :voting_store

  def start_link(path), do: GenServer.start_link(__MODULE__, path, name: __MODULE__)

  def put(key, value), do: call({:put, key, value})
  def insert_new(key, value), do: call({:insert_new, key, value})
  def get(key), do: call({:get, key})
  def poll_ids, do: call(:poll_ids)

  # Falha de comunicação com o Store vira erro operacional, nunca exceção.
  defp call(msg) do
    GenServer.call(__MODULE__, msg)
  catch
    :exit, reason -> {:error, {:store_unavailable, reason}}
  end

  @impl true
  def init(path) do
    # Garante que terminate/2 seja chamado em desligamentos ordenados.
    Process.flag(:trap_exit, true)

    case :dets.open_file(@table, file: String.to_charlist(path), type: :set) do
      {:ok, _} -> {:ok, path}
      {:error, reason} -> {:stop, reason}
    end
  end

  @impl true
  def handle_call({:put, key, value}, _from, state) do
    reply =
      safe(fn ->
        case :dets.insert(@table, {key, value}) do
          true -> sync()
          {:error, reason} -> {:error, reason}
        end
      end)

    {:reply, reply, state}
  end

  def handle_call({:insert_new, key, value}, _from, state) do
    reply =
      safe(fn ->
        case :dets.insert_new(@table, {key, value}) do
          true -> sync()
          false -> :exists
          {:error, reason} -> {:error, reason}
        end
      end)

    {:reply, reply, state}
  end

  def handle_call({:get, key}, _from, state) do
    reply =
      safe(fn ->
        case :dets.lookup(@table, key) do
          [{^key, value}] -> {:ok, value}
          [] -> :not_found
          {:error, reason} -> {:error, reason}
        end
      end)

    {:reply, reply, state}
  end

  def handle_call(:poll_ids, _from, state) do
    reply =
      safe(fn ->
        case :dets.match(@table, {{:poll, :"$1"}, :_}) do
          ids when is_list(ids) -> {:ok, List.flatten(ids)}
          {:error, reason} -> {:error, reason}
        end
      end)

    {:reply, reply, state}
  end

  @impl true
  def terminate(_reason, _state) do
    :dets.close(@table)
    :ok
  end

  defp sync do
    case :dets.sync(@table) do
      :ok -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  # Captura falhas de E/S do DETS e as devolve como erro operacional.
  defp safe(fun) do
    fun.()
  rescue
    e -> {:error, e}
  catch
    kind, reason -> {:error, {kind, reason}}
  end
end

defmodule VotingSystem.Poll do
  @moduledoc false
  # Processo de uma enquete. Cada enquete é independente e isolada.
  # O estado é persistido a cada alteração bem-sucedida (write-through).
  use GenServer

  alias VotingSystem.{Registry, Store}

  def new(id, question, options) do
    %{
      id: id,
      question: question,
      options: options,
      counts: Map.new(options, &{&1, 0}),
      voters: MapSet.new()
    }
  end

  def start_link(%{id: id}) do
    GenServer.start_link(__MODULE__, id, name: via(id))
  end

  defp via(id), do: {:via, Elixir.Registry, {VotingSystem.Registry, id}}

  @impl true
  def init(id) do
    # Após uma falha, o processo reidrata seu estado a partir da persistência.
    case Store.get({:poll, id}) do
      {:ok, state} -> {:ok, state}
      :not_found -> {:stop, :not_found}
      {:error, reason} -> {:stop, {:store, reason}}
    end
  end

  @impl true
  def handle_call({:vote, voter, option}, _from, state) do
    cond do
      not Enum.member?(state.options, option) ->
        # Erro de validação: nenhum estado é alterado.
        {:reply, {:error, {:validation, :invalid_option}}, state}

      MapSet.member?(state.voters, voter) ->
        {:reply, {:error, {:validation, :already_voted}}, state}

      true ->
        new_state = %{
          state
          | counts: Map.update!(state.counts, option, &(&1 + 1)),
            voters: MapSet.put(state.voters, voter)
        }

        # Persiste antes de confirmar; falha de persistência não altera o estado.
        case Store.put({:poll, state.id}, new_state) do
          :ok ->
            {:reply, {:ok, new_state.counts}, new_state}

          {:error, reason} ->
            {:reply, {:error, {:operational, reason}}, state}
        end
    end
  end

  def handle_call(:tally, _from, state) do
    {:reply, {:ok, %{question: state.question, counts: state.counts}}, state}
  end
end

defmodule VotingSystem.Demo do
  @moduledoc false

  def run do
    path = Path.join(System.tmp_dir!(), "voting_demo.dets")
    File.rm(path)

    {:ok, _} = VotingSystem.start_link(path: path)

    IO.inspect(VotingSystem.create_poll("p1", "Linguagem favorita?", ["elixir", "erlang"]),
      label: "criar p1"
    )

    IO.inspect(VotingSystem.create_poll("p1", "Outra", ["a", "b"]), label: "criar p1 duplicada")
    IO.inspect(VotingSystem.create_poll("p2", "", ["a"]), label: "criar p2 inválida")

    # Votos concorrentes de eleitores distintos.
    results =
      1..500
      |> Task.async_stream(
        fn i ->
          option = if rem(i, 3) == 0, do: "erlang", else: "elixir"
          VotingSystem.vote("p1", "eleitor#{i}", option)
        end,
        max_concurrency: 50
      )
      |> Enum.map(fn {:ok, r} -> r end)

    IO.inspect(Enum.frequencies_by(results, &classify/1), label: "resultados concorrentes")

    IO.inspect(VotingSystem.vote("p1", "eleitor1", "elixir"), label: "voto duplicado")
    IO.inspect(VotingSystem.vote("p1", "eleitor600", "go"), label: "opção inválida")
    IO.inspect(VotingSystem.vote("inexistente", "x", "y"), label: "enquete inexistente")

    IO.inspect(VotingSystem.tally("p1"), label: "apuração antes da falha")

    # Falha inesperada simulada: o processo da enquete é morto.
    [{pid, _}] = Registry.lookup(VotingSystem.Registry, "p1")
    Process.exit(pid, :kill)
    Process.sleep(50)

    IO.inspect(VotingSystem.tally("p1"), label: "apuração após recuperação")

    # Reinicialização completa do sistema: os dados vêm do DETS.
    Supervisor.stop(VotingSystem)
    {:ok, _} = VotingSystem.start_link(path: path)

    IO.inspect(VotingSystem.tally("p1"), label: "apuração após reinicialização")
    IO.inspect(VotingSystem.vote("p1", "eleitor1", "elixir"), label: "voto persistido")

    Supervisor.stop(VotingSystem)
    File.rm(path)
    :ok
  end

  defp classify({:ok, _}), do: :ok
  defp classify({:error, {kind, _}}), do: kind
end

VotingSystem.Demo.run()
