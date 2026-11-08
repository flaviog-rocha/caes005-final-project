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

# Sistema de votação online: persistência em DETS, idempotência, processamento
# em streaming, macros e recuperação por supervisão.
# Execução: elixir voting.exs

# ---------------------------------------------------------------------------
# Macros: envolvem uma operação para garantir execução única por op_id.
# ---------------------------------------------------------------------------
defmodule Voting.Macros do
  @moduledoc """
  Macros de uso geral. `idempotent/2` garante que a operação identificada
  por `op_id` seja executada no máximo uma vez: reenvios retornam o
  resultado já persistido, sem reexecutar o bloco.
  """

  defmacro idempotent(op_id, do: block) do
    quote do
      Voting.Store.once(unquote(op_id), fn -> unquote(block) end)
    end
  end
end

# ---------------------------------------------------------------------------
# Store: único dono da tabela DETS. Serializa escritas via GenServer, o que
# torna a verificação de idempotência livre de condições de corrida.
# ---------------------------------------------------------------------------
defmodule Voting.Store do
  use GenServer

  @table :voting_store
  @stream_chunk 500

  # ----- API pública -----

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Executa `fun` no máximo uma vez para `op_id`. Se a operação já foi
  concluída, devolve `{:duplicate, resultado_original}`. Se `fun` lançar
  exceção, nada é registrado e o erro é devolvido, permitindo nova tentativa.
  """
  def once(op_id, fun) when is_function(fun, 0) do
    GenServer.call(__MODULE__, {:once, op_id, fun}, :infinity)
  end

  @doc """
  Lê os votos de uma enquete em lotes (`:dets.select/3` com continuação),
  sem materializar a tabela inteira em memória.
  """
  def stream_votes(poll) do
    ms = [{{{:vote, :"$2", :_}, :"$1"}, [{:"=:=", :"$2", {:const, poll}}], [:"$1"]}]

    Stream.resource(
      fn -> :dets.select(@table, ms, @stream_chunk) end,
      fn
        :"$end_of_table" -> {:halt, nil}
        {:error, reason} -> raise "erro na leitura DETS: #{inspect(reason)}"
        {items, cont} -> {items, :dets.select(cont)}
      end,
      fn _ -> :ok end
    )
  end

  @doc false
  # Operação de escrita primitiva. Deve ser chamada apenas dentro de `once/2`,
  # ou seja, no processo Store. `insert_new` garante um voto por eleitor.
  def __insert_vote__(poll, voter, choice) do
    if :dets.insert_new(@table, {{:vote, poll, voter}, choice}) do
      {:ok, :registered}
    else
      {:error, :already_voted}
    end
  end

  # ----- GenServer -----

  @impl true
  def init(opts) do
    # Trap de exits para garantir terminate/2 (fechamento limpo do DETS).
    Process.flag(:trap_exit, true)
    path = opts |> Keyword.get(:path, "voting.dets") |> String.to_charlist()

    case :dets.open_file(@table, file: path, type: :set) do
      {:ok, _ref} -> {:ok, %{}}
      {:error, reason} -> {:stop, reason}
    end
  end

  @impl true
  def handle_call({:once, op_id, fun}, _from, state) do
    key = {:op, op_id}

    reply =
      case :dets.lookup(@table, key) do
        [{^key, result}] ->
          {:duplicate, result}

        [] ->
          try do
            result = fun.()
            # Registro da operação só é persistido após a execução bem-sucedida.
            :ok = :dets.insert(@table, {key, result})
            :ok = :dets.sync(@table)
            {:ok, result}
          catch
            kind, reason -> {:error, {kind, reason}}
          end
      end

    {:reply, reply, state}
  end

  @impl true
  def terminate(_reason, _state) do
    :dets.close(@table)
    :ok
  end
end

# ---------------------------------------------------------------------------
# API de domínio: votação e apuração.
# ---------------------------------------------------------------------------
defmodule Voting do
  import Voting.Macros

  @doc "Registra um voto. `op_id` identifica a requisição para evitar reexecução."
  def vote(op_id, poll, voter, choice) do
    idempotent(op_id) do
      Voting.Store.__insert_vote__(poll, voter, choice)
    end
  end

  @doc "Apuração em streaming: memória proporcional ao número de opções, não de votos."
  def tally(poll) do
    poll
    |> Voting.Store.stream_votes()
    |> Enum.reduce(%{}, fn choice, acc -> Map.update(acc, choice, 1, &(&1 + 1)) end)
  end
end

# ---------------------------------------------------------------------------
# Importação de grandes volumes: lê o arquivo linha a linha, em lotes.
# Cada lote é uma operação idempotente identificada por {arquivo, índice}.
# Após uma falha, reexecutar a importação pula lotes já concluídos.
# ---------------------------------------------------------------------------
defmodule Voting.Import do
  import Voting.Macros

  @batch 1_000

  @doc "Formato de cada linha: poll,eleitor,escolha"
  def run(path) do
    path
    |> File.stream!()
    |> Stream.map(&String.trim/1)
    |> Stream.reject(&(&1 == ""))
    |> Stream.chunk_every(@batch)
    |> Stream.with_index()
    |> Stream.map(fn {lines, idx} -> process_batch(path, idx, lines) end)
    |> Enum.reduce(%{}, fn {tag, _}, acc -> Map.update(acc, tag, 1, &(&1 + 1)) end)
  end

  defp process_batch(path, idx, lines) do
    idempotent({path, idx}) do
      Enum.frequencies(Enum.map(lines, &parse_and_insert/1))
    end
  end

  defp parse_and_insert(line) do
    case String.split(line, ",", parts: 3) do
      [poll, voter, choice] -> Voting.Store.__insert_vote__(poll, voter, choice)
      _ -> {:error, :malformed}
    end
  end
end

# ---------------------------------------------------------------------------
# Supervisão: reinicia o Store (que reabre o DETS com os dados persistidos)
# em caso de falha inesperada.
# ---------------------------------------------------------------------------
defmodule Voting.Sup do
  use Supervisor

  def start_link(opts) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    children = [
      {Voting.Store, path: Keyword.get(opts, :path, "voting.dets")}
    ]

    Supervisor.init(children, strategy: :one_for_one, max_restarts: 5, max_seconds: 10)
  end
end

# ---------------------------------------------------------------------------
# Demonstração
# ---------------------------------------------------------------------------
{:ok, _sup} = Voting.Sup.start_link(path: "voting.dets")

IO.inspect(Voting.vote("op-1", "poll-A", "eleitor-1", "sim"), label: "voto 1")
IO.inspect(Voting.vote("op-1", "poll-A", "eleitor-1", "sim"), label: "reenvio mesmo op_id")
IO.inspect(Voting.vote("op-2", "poll-A", "eleitor-1", "nao"), label: "segundo voto do eleitor")
IO.inspect(Voting.vote("op-3", "poll-A", "eleitor-2", "nao"), label: "voto 2")

File.write!(
  "votos.csv",
  Enum.map_join(1..5_000, "", fn i ->
    "poll-B,eleitor-#{i},#{Enum.random(["sim", "nao"])}\n"
  end)
)

IO.inspect(Voting.Import.run("votos.csv"), label: "importação em lotes")
IO.inspect(Voting.tally("poll-A"), label: "apuração poll-A")
IO.inspect(Voting.tally("poll-B"), label: "apuração poll-B")
