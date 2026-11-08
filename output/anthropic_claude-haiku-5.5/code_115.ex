# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de votação online, atendendo somente aos seguintes requisitos:
#
# 1. Impedir que uma mesma operação seja executada mais de uma vez.
# 2. Processar grandes volumes de dados sem carregá-los integralmente na memória.
# 3. Persistir os dados para recuperação após reinicializações.
# 4. Validar os dados recebidos antes de realizar operações.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - Agent
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# voting_system.ex
#
# Sistema de votação online com Agent.
#
# Garantias:
#   1. Idempotência: cada operação (op_id) é executada no máximo uma vez, e
#      cada eleitor vota no máximo uma vez.
#   2. Processamento de grandes volumes: entradas são consumidas via Stream em
#      lotes (chunk_every); a recuperação do log é feita linha a linha com
#      File.stream!. Só o estado agregado (conjuntos e contagens) fica em memória.
#   3. Persistência: log append-only (write-ahead log) com fsync por lote. Na
#      inicialização o estado é reconstruído a partir do log.
#   4. Validação: todos os dados são validados antes de qualquer alteração de estado.
#
# Uso (exemplo):
#
#   {:ok, _pid} =
#     VotingSystem.start_link(
#       name: :eleicao,
#       data_file: "data/votos.log",
#       candidates: ["Alice", "Bob"]
#     )
#
#   VotingSystem.cast_vote(:eleicao, %{
#     "op_id" => "3f2b8a1e-6c4d-4e8f-9a1b-2c3d4e5f6a7b",
#     "voter_id" => "eleitor_001",
#     "candidate" => "Alice"
#   })
#   #=> :ok
#
#   VotingSystem.tally(:eleicao)
#   #=> %{"Alice" => 1, "Bob" => 0}
#
#   # Processamento em lote de grande volume (ex.: CSV/TSV via stream):
#   File.stream!("votos.tsv")
#   |> Stream.map(&parse_linha/1)   # cada linha -> mapa com as chaves string
#   |> then(&VotingSystem.submit_stream(:eleicao, &1, chunk_size: 1_000))
#   #=> %{accepted: 950, rejected: %{duplicate_operation: 10, already_voted: 40}}

defmodule VotingSystem.Validator do
  @moduledoc """
  Validação pura dos dados recebidos. Nenhuma função aqui altera estado.
  """

  @doc """
  Valida um mapa de voto com chaves string: "op_id", "voter_id" e "candidate".
  Retorna `{:ok, voto}` com o voto normalizado ou `{:error, motivo}`.
  """
  def vote(%{} = params, candidates) do
    with {:ok, op_id} <- fetch_string(params, "op_id"),
         :ok <- check(uuid?(op_id), :invalid_op_id),
         {:ok, voter_id} <- fetch_string(params, "voter_id"),
         :ok <- check(voter_id?(voter_id), :invalid_voter_id),
         {:ok, candidate} <- fetch_string(params, "candidate"),
         :ok <- check(candidate in candidates, :unknown_candidate) do
      {:ok, %{op_id: op_id, voter_id: voter_id, candidate: candidate}}
    end
  end

  def vote(_params, _candidates), do: {:error, :invalid_payload}

  # UUID v4-like (formato 8-4-4-4-12 hexadecimal)
  defp uuid?(value) do
    Regex.match?(~r/\A[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\z/, value)
  end

  # Identificador de eleitor: 1 a 64 caracteres seguros (sem tabulação/quebra de linha)
  defp voter_id?(value) do
    Regex.match?(~r/\A[A-Za-z0-9_.\-]{1,64}\z/, value)
  end

  defp fetch_string(params, key) do
    case Map.get(params, key) do
      value when is_binary(value) ->
        case String.trim(value) do
          "" -> {:error, {:missing, key}}
          trimmed -> {:ok, trimmed}
        end

      _ ->
        {:error, {:missing, key}}
    end
  end

  defp check(true, _reason), do: :ok
  defp check(false, reason), do: {:error, reason}
end

defmodule VotingSystem do
  @moduledoc """
  Sistema de votação baseado em Agent com persistência em log append-only.

  O estado do Agent possui:
    * `ops`        - conjunto de op_ids já executados (idempotência)
    * `voters`     - conjunto de eleitores que já votaram
    * `tally`      - contagem de votos por candidato
    * `candidates` - lista de candidatos válidos
    * `io`         - dispositivo de escrita do log

  Toda alteração segue: decidir (puro) -> persistir com fsync -> confirmar no estado.
  Se a persistência falhar, o Agent cai sem alterar o estado em memória e a
  recuperação ocorre na próxima inicialização a partir do log.
  """

  alias VotingSystem.Validator

  @default_chunk_size 500

  # ---------------------------------------------------------------------------
  # API pública
  # ---------------------------------------------------------------------------

  @doc """
  Inicia o sistema.

  Opções:
    * `:name`        - nome do Agent (padrão: `#{inspect(__MODULE__)}`)
    * `:data_file`   - caminho do arquivo de log (obrigatório)
    * `:candidates`  - lista de candidatos válidos (obrigatório)
  """
  def start_link(opts) do
    name = Keyword.get(opts, :name, __MODULE__)
    path = Keyword.fetch!(opts, :data_file)
    candidates = Keyword.fetch!(opts, :candidates)

    validate_candidates!(candidates)

    Agent.start_link(fn -> init_state(path, candidates) end, name: name)
  end

  @doc false
  def child_spec(opts) do
    %{
      id: Keyword.get(opts, :name, __MODULE__),
      start: {__MODULE__, :start_link, [opts]},
      type: :worker
    }
  end

  @doc """
  Registra um único voto. Retorna `:ok` ou `{:error, motivo}`.
  A validação ocorre antes de qualquer alteração de estado.
  """
  def cast_vote(name, params) do
    candidates = Agent.get(name, & &1.candidates)

    case Validator.vote(params, candidates) do
      {:ok, vote} ->
        [result] = Agent.get_and_update(name, &process_batch(&1, [vote]))
        result

      {:error, _reason} = error ->
        error
    end
  end

  @doc """
  Processa um enumerável (possivelmente um Stream de arquivo) em lotes.
  Apenas o lote corrente e o sumário são mantidos em memória.

  Opções:
    * `:chunk_size` - tamanho de cada lote (padrão: #{@default_chunk_size})

  Retorna `%{accepted: integer, rejected: %{motivo => quantidade}}`.
  """
  def submit_stream(name, enumerable, opts \\ []) do
    chunk_size = Keyword.get(opts, :chunk_size, @default_chunk_size)
    candidates = Agent.get(name, & &1.candidates)

    enumerable
    |> Stream.map(&Validator.vote(&1, candidates))
    |> Stream.chunk_every(chunk_size)
    |> Enum.reduce(%{accepted: 0, rejected: %{}}, fn chunk, summary ->
      # Separa válidos de inválidos antes de tocar no Agent
      {oks, errs} = Enum.split_with(chunk, &match?({:ok, _}, &1))
      votes = Enum.map(oks, fn {:ok, vote} -> vote end)

      results =
        case votes do
          [] -> []
          _ -> Agent.get_and_update(name, &process_batch(&1, votes))
        end

      summary
      |> add_results(results)
      |> add_results(errs)
    end)
  end

  @doc "Retorna a contagem de votos por candidato."
  def tally(name), do: Agent.get(name, & &1.tally)

  @doc "Indica se o eleitor já votou."
  def voted?(name, voter_id), do: Agent.get(name, &MapSet.member?(&1.voters, voter_id))

  # ---------------------------------------------------------------------------
  # Inicialização e recuperação
  # ---------------------------------------------------------------------------

  defp validate_candidates!(candidates) do
    valid? =
      is_list(candidates) and candidates != [] and
        Enum.all?(candidates, fn c ->
          is_binary(c) and c != "" and not String.contains?(c, ["\t", "\n"])
        end)

    unless valid? do
      raise ArgumentError,
            "candidates deve ser uma lista não vazia de strings sem tabulação ou quebra de linha"
    end
  end

  defp init_state(path, candidates) do
    path |> Path.dirname() |> File.mkdir_p!()

    base = %{
      ops: MapSet.new(),
      voters: MapSet.new(),
      tally: Map.new(candidates, &{&1, 0}),
      candidates: candidates,
      io: nil
    }

    state = if File.exists?(path), do: replay(path, base), else: base

    %{state | io: File.open!(path, [:append, :binary])}
  end

  # Lê o log linha a linha (memória constante) e reconstrói o estado.
  # Registros incompletos no fim do arquivo (queda durante escrita) são descartados
  # e o arquivo é truncado até o último registro íntegro.
  defp replay(path, state) do
    {state, valid_bytes} =
      path
      |> File.stream!()
      |> Enum.reduce({state, 0}, fn raw, {st, valid} ->
        if String.ends_with?(raw, "\n") do
          {replay_line(st, String.trim_trailing(raw, "\n")), valid + byte_size(raw)}
        else
          IO.warn("registro incompleto descartado na recuperação do log")
          {st, valid}
        end
      end)

    truncate_to(path, valid_bytes)
    state
  end

  defp replay_line(st, line) do
    case parse_line(line, st.candidates) do
      {:ok, vote} ->
        {_result, new_st} = decide(st, vote)
        new_st

      {:error, reason} ->
        IO.warn("registro ignorado na recuperação: #{inspect(reason)}")
        st
    end
  end

  defp truncate_to(path, size) do
    if File.stat!(path).size > size do
      {:ok, io} = File.open(path, [:read, :write, :binary])
      {:ok, _} = :file.position(io, size)
      :ok = :file.truncate(io)
      :ok = File.close(io)
    end

    :ok
  end

  # Formato do log: "op_id\tvoter_id\tcandidate\n"
  defp parse_line(line, candidates) do
    case String.split(line, "\t") do
      [op_id, voter_id, candidate] ->
        Validator.vote(
          %{"op_id" => op_id, "voter_id" => voter_id, "candidate" => candidate},
          candidates
        )

      _ ->
        {:error, :malformed_line}
    end
  end

  # ---------------------------------------------------------------------------
  # Lógica de negócio (funções puras sobre o estado)
  # ---------------------------------------------------------------------------

  # Decide se um voto pode ser aplicado. Retorna {:ok, novo_estado} ou
  # {{:error, motivo}, estado_inalterado}.
  defp decide(st, vote) do
    cond do
      MapSet.member?(st.ops, vote.op_id) ->
        {{:error, :duplicate_operation}, st}

      MapSet.member?(st.voters, vote.voter_id) ->
        {{:error, :already_voted}, st}

      true ->
        {:ok, commit(st, vote)}
    end
  end

  defp commit(st, vote) do
    %{
      st
      | ops: MapSet.put(st.ops, vote.op_id),
        voters: MapSet.put(st.voters, vote.voter_id),
        tally: Map.update(st.tally, vote.candidate, 1, &(&1 + 1))
    }
  end

  # Processa um lote: decide todos em memória, persiste os aceitos com fsync
  # e só então devolve o novo estado ao Agent (write-ahead).
  defp process_batch(state, votes) do
    {results, new_state, accepted} =
      Enum.reduce(votes, {[], state, []}, fn vote, {res, st, acc} ->
        case decide(st, vote) do
          {:ok, st2} -> {[:ok | res], st2, [vote | acc]}
          {{:error, _} = err, st2} -> {[err | res], st2, acc}
        end
      end)

    :ok = persist(state.io, Enum.reverse(accepted))

    {Enum.reverse(results), new_state}
  end

  defp persist(_io, []), do: :ok

  defp persist(io, votes) do
    lines =
      Enum.map(votes, fn v ->
        [v.op_id, ?\t, v.voter_id, ?\t, v.candidate, ?\n]
      end)

    :ok = IO.binwrite(io, lines)
    :ok = :file.sync(io)
  end

  # ---------------------------------------------------------------------------
  # Utilitários
  # ---------------------------------------------------------------------------

  defp add_results(summary, results) do
    Enum.reduce(results, summary, fn
      :ok, acc ->
        %{acc | accepted: acc.accepted + 1}

      {:error, reason}, acc ->
        %{acc | rejected: Map.update(acc.rejected, reason, 1, &(&1 + 1))}
    end)
  end
end
