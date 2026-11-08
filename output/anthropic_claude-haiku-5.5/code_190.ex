# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de votação online, atendendo somente aos seguintes requisitos:
#
# 1. Persistir os dados para recuperação após reinicializações.
# 2. Impedir que uma mesma operação seja executada mais de uma vez.
# 3. Processar grandes volumes de dados sem carregá-los integralmente na memória.
# 4. Fazer uso de macros.
# 5. Recuperar processos após falhas inesperadas.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# voting.exs
# Sistema de votação online com persistência (DETS), idempotência, streaming,
# macros e recuperação por supervisão. Somente biblioteca padrão.
# Execução: elixir voting.exs

defmodule Voting.Macros do
  # Macros e utilitários de idempotência e retentativa.

  @doc """
  Executa o bloco no máximo uma vez para um `op_id` persistido na tabela DETS.
  Se a operação já foi concluída com sucesso, devolve `{:duplicate, resultado_anterior}`
  sem reexecutar o bloco. Apenas resultados `{:ok, _}` são registrados, para que
  falhas (`{:error, _}`) possam ser reenviadas.
  """
  defmacro idempotent(table, op_id, do: block) do
    quote do
      key = {:op, unquote(op_id)}

      case :dets.lookup(unquote(table), key) do
        [{^key, previous}] ->
          {:duplicate, previous}

        [] ->
          case unquote(block) do
            {:ok, _} = ok ->
              # Registro da operação após sucesso. Se o processo cair entre
              # a escrita do efeito e este registro, a regra de unicidade do
              # eleitor (ballot) ainda impede voto duplicado.
              :ok = :dets.insert(unquote(table), {key, ok})
              ok

            other ->
              other
          end
      end
    end
  end

  @doc """
  Reexecuta a expressão em caso de saída (`exit`) do processo alvo,
  típico durante a reinicialização de um GenServer supervisionado.
  """
  defmacro with_retry(attempts, do: block) do
    quote do
      Voting.Macros.__retry__(fn -> unquote(block) end, unquote(attempts))
    end
  end

  @doc false
  def __retry__(fun, attempts_left) do
    fun.()
  catch
    :exit, _reason when attempts_left > 1 ->
      Process.sleep(100)
      __retry__(fun, attempts_left - 1)
  end
end

defmodule Voting.Store do
  # Processo dono da tabela DETS. Serializa as escritas (evita condições de corrida),
  # garante idempotência e persiste os dados em disco.

  use GenServer
  import Voting.Macros, only: [idempotent: 3]

  @table :voting_store
  @batch 500

  # ---------------------------------------------------------------------------
  # API pública
  # ---------------------------------------------------------------------------

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def create_election(op_id, name, options) do
    GenServer.call(__MODULE__, {:create_election, op_id, name, options})
  end

  def vote(op_id, election, voter, option) do
    GenServer.call(__MODULE__, {:vote, op_id, election, voter, option})
  end

  @doc """
  Apuração com leitura preguiçosa: os votos são lidos do disco em lotes e
  agregados um a um, sem materializar a eleição inteira na memória.
  """
  def tally(election) do
    election
    |> stream_ballots()
    |> Enum.reduce(%{}, fn {_voter, option}, acc ->
      Map.update(acc, option, 1, &(&1 + 1))
    end)
  end

  @doc "Stream preguiçosa de `{voter, option}` de uma eleição, lida em lotes do DETS."
  def stream_ballots(election) do
    spec = [{{{:ballot, election, :"$1"}, :"$2"}, [], [{{:"$1", :"$2"}}]}]

    Stream.resource(
      fn -> {:first, spec} end,
      fn
        {:first, s} -> emit(:dets.select(@table, s, @batch))
        {:next, cont} -> emit(:dets.select(cont))
      end,
      fn _ -> :ok end
    )
  end

  defp emit(:"$end_of_table"), do: {:halt, nil}
  defp emit({objs, cont}), do: {objs, {:next, cont}}

  # ---------------------------------------------------------------------------
  # Callbacks do GenServer
  # ---------------------------------------------------------------------------

  @impl true
  def init(opts) do
    # Com trap_exit, o terminate/2 é chamado em desligamentos ordenados da supervisão.
    Process.flag(:trap_exit, true)

    path = opts |> Keyword.get(:path, "voting.dets") |> String.to_charlist()

    # Se o processo anterior morreu sem fechar a tabela, o DETS faz reparo
    # automático na reabertura: é o mecanismo de recuperação dos dados.
    case :dets.open_file(@table, file: path, type: :set, auto_save: 1_000) do
      {:ok, tab} -> {:ok, %{tab: tab}}
      {:error, reason} -> {:stop, reason}
    end
  end

  @impl true
  def handle_call({:create_election, op_id, name, options}, _from, state) do
    reply = idempotent(@table, op_id, do: create(name, options))
    {:reply, reply, state}
  end

  def handle_call({:vote, op_id, election, voter, option}, _from, state) do
    reply = idempotent(@table, op_id, do: cast(election, voter, option))
    {:reply, reply, state}
  end

  @impl true
  def terminate(_reason, %{tab: tab}) do
    :dets.close(tab)
  end

  # ---------------------------------------------------------------------------
  # Regras de negócio
  # ---------------------------------------------------------------------------

  defp create(name, options) do
    case :dets.lookup(@table, {:election, name}) do
      [_] ->
        {:error, :election_exists}

      [] ->
        :ok = :dets.insert(@table, {{:election, name}, options})
        {:ok, :created}
    end
  end

  defp cast(election, voter, option) do
    case :dets.lookup(@table, {:election, election}) do
      [] ->
        {:error, :unknown_election}

      [{_, options}] ->
        cond do
          option not in options ->
            {:error, :invalid_option}

          :dets.member(@table, {:ballot, election, voter}) ->
            {:error, :already_voted}

          true ->
            :ok = :dets.insert(@table, {{:ballot, election, voter}, option})
            {:ok, :voted}
        end
    end
  end
end

defmodule Voting.Supervisor do
  # Supervisor que reinicia o Store após falhas inesperadas.

  use Supervisor
  def start_link(opts), do: Supervisor.start_link(__MODULE__, opts)

  @impl true
  def init(opts) do
    children = [{Voting.Store, opts}]
    Supervisor.init(children, strategy: :one_for_one, max_restarts: 10, max_seconds: 30)
  end
end

defmodule Voting.Importer do
  # Importação de grandes arquivos CSV (op_id,election,voter,option) em streaming.

  import Voting.Macros, only: [with_retry: 2]

  @doc """
  Processa o arquivo linha a linha. Como o pipeline é preguiçoso, a memória
  utilizada não cresce com o tamanho do arquivo. Reimportar o mesmo arquivo
  não gera votos duplicados, pois cada linha possui um `op_id` único.
  """
  def import_file(path) do
    path
    |> File.stream!()
    |> Stream.map(&String.trim/1)
    |> Stream.reject(&(&1 == ""))
    |> Stream.map(&String.split(&1, ","))
    |> Enum.reduce(%{ok: 0, duplicate: 0, error: 0}, fn [op, election, voter, option], acc ->
      result =
        with_retry 5 do
          Voting.Store.vote(op, election, voter, option)
        end

      case result do
        {:ok, _} -> Map.update!(acc, :ok, &(&1 + 1))
        {:duplicate, _} -> Map.update!(acc, :duplicate, &(&1 + 1))
        _ -> Map.update!(acc, :error, &(&1 + 1))
      end
    end)
  end
end

defmodule Voting.Demo do
  # Demonstração de todos os requisitos.

  import Voting.Macros, only: [with_retry: 2]

  def run do
    db = "voting_demo.dets"
    csv = "votes_demo.csv"

    File.rm(db)
    File.rm(csv)

    # 1) Inicialização
    {:ok, sup} = Voting.Supervisor.start_link(path: db)

    IO.inspect(Voting.Store.create_election("op-create-1", "eleicao", ["sim", "nao"]),
      label: "criar eleicao"
    )

    # Geração de um arquivo grande (10.000 linhas), escrito em streaming
    1..10_000
    |> Stream.map(fn i ->
      option = if rem(i, 3) == 0, do: "nao", else: "sim"
      "op-#{i},eleicao,eleitor-#{i},#{option}\n"
    end)
    |> Stream.into(File.stream!(csv))
    |> Stream.run()

    # 2) Importação em streaming
    IO.inspect(Voting.Importer.import_file(csv), label: "importacao 1")

    # 3) Idempotência: reimportar o mesmo arquivo não duplica operações
    IO.inspect(Voting.Importer.import_file(csv), label: "importacao 2 (reenvio)")

    # 4) Apuração lendo do disco em lotes
    IO.inspect(Voting.Store.tally("eleicao"), label: "apuracao antes da falha")

    # 5) Falha inesperada: o processo é encerrado abruptamente
    Process.exit(Process.whereis(Voting.Store), :kill)

    IO.inspect(
      with_retry 5 do
        Voting.Store.vote("op-after-crash", "eleicao", "eleitor-novo", "sim")
      end,
      label: "voto apos recuperacao"
    )

    # 6) Persistência: a supervisão é encerrada e iniciada novamente
    Supervisor.stop(sup)
    {:ok, _sup} = Voting.Supervisor.start_link(path: db)

    IO.inspect(Voting.Store.tally("eleicao"), label: "apuracao apos reinicializacao")

    IO.inspect(
      Voting.Store.vote("op-after-crash", "eleicao", "eleitor-novo", "sim"),
      label: "reenvio da mesma operacao"
    )
  end
end

Voting.Demo.run()
