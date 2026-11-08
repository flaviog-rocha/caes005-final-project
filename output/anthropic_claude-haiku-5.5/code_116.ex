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

defmodule VotingSystem do
  @moduledoc """
  Sistema de votação online com:

    * Idempotência: cada operação (`op_id`) e cada eleitor (`voter_id`) são
      aceitos no máximo uma vez.
    * Processamento em streaming: lotes de votos e o log de recuperação são
      lidos linha a linha com `File.stream!/1`, sem carregar o arquivo inteiro.
    * Persistência: cada voto aceito é gravado em um log (append + fsync)
      antes de ser confirmado. Na inicialização o log é reproduzido para
      recuperar o estado.
    * Validação: todos os campos são verificados antes de qualquer operação.

  O estado vivo fica em um `Agent`. A memória cresce apenas com os
  identificadores (op_id e voter_id) e com os contadores por candidato,
  nunca com o conteúdo bruto dos votos.

  Formato de cada linha do log e dos arquivos de importação:

      <op_id>\t<voter_id>\t<candidate>\n
  """

  @separator "\t"

  # ---------------------------------------------------------------------------
  # API pública
  # ---------------------------------------------------------------------------

  @doc """
  Especificação para uso em supervisores.
  Aceita as mesmas opções de `start_link/1`.
  """
  def child_spec(opts) do
    %{
      id: Keyword.get(opts, :name, __MODULE__),
      start: {__MODULE__, :start_link, [opts]},
      type: :worker
    }
  end

  @doc """
  Inicia o sistema de votação.

  Opções:
    * `:path` - caminho do arquivo de log (obrigatório)
    * `:candidates` - lista de candidatos válidos (obrigatório)
    * `:name` - nome registrado do Agent (padrão: `VotingSystem`)
  """
  def start_link(opts) do
    path = Keyword.fetch!(opts, :path)
    name = Keyword.get(opts, :name, __MODULE__)
    candidates = opts |> Keyword.fetch!(:candidates) |> Enum.map(&to_string/1)

    # Validação da configuração: candidatos devem ser identificadores válidos.
    if candidates == [] or not Enum.all?(candidates, &valid_id?/1) do
      raise ArgumentError, "lista de candidatos inválida: #{inspect(candidates)}"
    end

    Agent.start_link(fn -> init_state(path, MapSet.new(candidates)) end, name: name)
  end

  @doc """
  Registra um voto.

  Retorna `:ok` ou `{:error, motivo}`, onde o motivo pode ser:
    * `:invalid_operation_id`
    * `:invalid_voter_id`
    * `:invalid_candidate`
    * `:duplicate_operation` - a operação já foi executada
    * `:already_voted` - o eleitor já votou
    * `{:persistence_failed, reason}` - falha ao gravar no log
  """
  def vote(op_id, voter_id, candidate, name \\ __MODULE__) do
    do_vote(%{op_id: op_id, voter_id: voter_id, candidate: candidate}, name)
  end

  @doc """
  Importa votos de um arquivo em streaming. Cada linha é processada de forma
  independente e o arquivo nunca é carregado integralmente na memória.

  Retorna um resumo: `%{accepted: integer, rejected: %{motivo => quantidade}}`.
  Linhas malformadas são contabilizadas como `:malformed_record`.
  """
  def import_file(path, name \\ __MODULE__) do
    path
    |> File.stream!()
    |> Enum.reduce(%{accepted: 0, rejected: %{}}, fn line, summary ->
      result =
        case parse_record(line) do
          {:ok, record} -> do_vote(record, name)
          :error -> {:error, :malformed_record}
        end

      case result do
        :ok ->
          %{summary | accepted: summary.accepted + 1}

        {:error, reason} ->
          %{summary | rejected: Map.update(summary.rejected, reason, 1, &(&1 + 1))}
      end
    end)
  end

  @doc "Retorna a contagem atual de votos por candidato."
  def results(name \\ __MODULE__) do
    Agent.get(name, & &1.counts)
  end

  # ---------------------------------------------------------------------------
  # Execução de operações (ponto único de escrita, serializado pelo Agent)
  # ---------------------------------------------------------------------------

  defp do_vote(record, name) do
    Agent.get_and_update(name, fn state ->
      case check_vote(record, state) do
        :ok -> persist_and_apply(record, state)
        {:error, reason} -> {{:error, reason}, state}
      end
    end)
  end

  # Grava primeiro no log (write-ahead) e só depois altera o estado em memória.
  defp persist_and_apply(record, state) do
    line = Enum.join([record.op_id, record.voter_id, record.candidate], @separator) <> "\n"

    # Observação: em caso de falha parcial de escrita, uma linha incompleta pode
    # permanecer no log; ao reproduzir, ela será descartada pela validação.
    with :ok <- :file.write(state.device, line),
         :ok <- :file.sync(state.device) do
      {:ok, apply_vote(record, state)}
    else
      {:error, reason} -> {{:error, {:persistence_failed, reason}}, state}
    end
  end

  # ---------------------------------------------------------------------------
  # Validação
  # ---------------------------------------------------------------------------

  defp check_vote(%{op_id: op_id, voter_id: voter_id, candidate: candidate}, state) do
    cond do
      not valid_id?(op_id) -> {:error, :invalid_operation_id}
      not valid_id?(voter_id) -> {:error, :invalid_voter_id}
      not MapSet.member?(state.candidates, candidate) -> {:error, :invalid_candidate}
      MapSet.member?(state.op_ids, op_id) -> {:error, :duplicate_operation}
      MapSet.member?(state.voters, voter_id) -> {:error, :already_voted}
      true -> :ok
    end
  end

  # Identificadores: 1 a 64 caracteres alfanuméricos, '_' ou '-'.
  # Isso impede tabs/quebras de linha que quebrariam o formato do log.
  defp valid_id?(value) when is_binary(value) do
    Regex.match?(~r/\A[A-Za-z0-9_-]{1,64}\z/, value)
  end

  defp valid_id?(_), do: false

  defp parse_record(line) do
    case line |> String.trim_trailing() |> String.split(@separator) do
      [op_id, voter_id, candidate] ->
        {:ok, %{op_id: op_id, voter_id: voter_id, candidate: candidate}}

      _ ->
        :error
    end
  end

  # ---------------------------------------------------------------------------
  # Estado e recuperação
  # ---------------------------------------------------------------------------

  defp init_state(path, candidates) do
    base = %{
      path: path,
      candidates: candidates,
      counts: Map.new(candidates, &{&1, 0}),
      op_ids: MapSet.new(),
      voters: MapSet.new(),
      device: nil
    }

    state = if File.exists?(path), do: replay(path, base), else: base

    # Arquivo aberto em modo raw e pertencente ao processo do Agent.
    {:ok, device} = File.open(path, [:append, :binary, :raw])
    %{state | device: device}
  end

  # Reproduz o log em streaming, aplicando apenas registros válidos.
  defp replay(path, state) do
    path
    |> File.stream!()
    |> Enum.reduce(state, fn line, acc ->
      with {:ok, record} <- parse_record(line),
           :ok <- check_vote(record, acc) do
        apply_vote(record, acc)
      else
        _invalid -> acc
      end
    end)
  end

  defp apply_vote(%{op_id: op_id, voter_id: voter_id, candidate: candidate}, state) do
    %{
      state
      | counts: Map.update!(state.counts, candidate, &(&1 + 1)),
        op_ids: MapSet.put(state.op_ids, op_id),
        voters: MapSet.put(state.voters, voter_id)
    }
  end
end

# Exemplo de uso (em um iex ou script):
#
#   {:ok, _pid} =
#     VotingSystem.start_link(
#       path: "votos.log",
#       candidates: ["alice", "bob"],
#       name: VotingSystem
#     )
#
#   :ok = VotingSystem.vote("op-1", "eleitor-1", "alice")
#   {:error, :duplicate_operation} = VotingSystem.vote("op-1", "eleitor-2", "bob")
#   {:error, :already_voted} = VotingSystem.vote("op-2", "eleitor-1", "bob")
#   {:error, :invalid_candidate} = VotingSystem.vote("op-3", "eleitor-3", "carol")
#
#   VotingSystem.import_file("lote.txt")
#   VotingSystem.results()
