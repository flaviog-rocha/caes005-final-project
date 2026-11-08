# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o processamento de arquivos CSV, atendendo somente aos seguintes requisitos:
#
# 1. Impedir atualizações parcialmente concluídas.
# 2. Implementar novas tentativas após falhas temporárias.
# 3. Permitir limitar a quantidade de operações simultâneas.
# 4. Quando fizer sentido para o domínio, considerar a comunicação com um banco de dados fictício e, opcionalmente, o uso de Ecto.Migration, sem tornar isso obrigatório.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - Logger
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

require Logger

defmodule CSVImporter do
  @moduledoc """
  Importação de arquivos CSV com:

  1. Atomicidade: todas as linhas válidas são gravadas em um único commit.
     Se qualquer linha for inválida ou o commit falhar de forma definitiva,
     nada é gravado.
  2. Retentativas com backoff exponencial e jitter para falhas temporárias.
     O commit é idempotente por `import_id`, então reenviar após uma falha
     ambígua não duplica dados.
  3. Limite de concorrência no processamento dos lotes via `Task.async_stream/3`
     (`:max_concurrency`).
  """

  # Erros considerados temporários (podem ser reexecutados com segurança).
  @transient_errors [:timeout, :busy, :connection_closed]

  defmodule FakeDB do
    @moduledoc """
    Banco de dados fictício em memória, implementado com `Agent`.
    Simula latência de rede e falhas temporárias de forma configurável.

    Em um projeto real, `commit/2` seria substituído por uma transação
    (por exemplo, `Repo.transaction/1` com `Ecto.Multi`). As migrações
    com `Ecto.Migration` não são necessárias para este exemplo.
    """
    use Agent

    @doc "Inicia o banco fictício. `:fail_rate` é a probabilidade (0.0 a 1.0) de falha temporária."
    def start_link(opts \\ []) do
      fail_rate = Keyword.get(opts, :fail_rate, 0.0)

      initial = %{table: %{}, applied: MapSet.new(), fail_rate: fail_rate}
      Agent.start_link(fn -> initial end, name: __MODULE__)
    end

    @doc "Retorna a quantidade de registros persistidos."
    def count, do: Agent.get(__MODULE__, &map_size(&1.table))

    @doc """
    Aplica todas as linhas em uma única operação atômica (tudo ou nada).
    A verificação de falha temporária ocorre antes de qualquer escrita,
    então uma falha nunca deixa o estado parcialmente atualizado.
    """
    def commit(import_id, rows) do
      Agent.get_and_update(__MODULE__, fn state ->
        cond do
          # Idempotência: este import já foi aplicado em uma tentativa anterior.
          MapSet.member?(state.applied, import_id) ->
            {{:ok, :already_applied}, state}

          # Falha temporária simulada: nada é escrito.
          :rand.uniform() < state.fail_rate ->
            {{:error, :timeout}, state}

          true ->
            table =
              Enum.reduce(rows, state.table, fn row, acc -> Map.put(acc, row.id, row) end)

            new_state = %{
              state
              | table: table,
                applied: MapSet.put(state.applied, import_id)
            }

            {{:ok, length(rows)}, new_state}
        end
      end)
    end

    @doc "Altera a taxa de falhas temporárias em tempo de execução."
    def set_fail_rate(rate), do: Agent.update(__MODULE__, &%{&1 | fail_rate: rate})
  end

  @doc """
  Importa o arquivo CSV indicado por `path`.

  Opções:
    * `:import_id` - identificador da importação (usado na idempotência)
    * `:chunk_size` - linhas por lote processado (padrão: 500)
    * `:max_concurrency` - máximo de lotes processados simultaneamente
    * `:max_retries` - número máximo de retentativas após falhas temporárias (padrão: 5)
    * `:base_backoff_ms` - base do backoff exponencial em milissegundos (padrão: 100)
  """
  def import(path, opts \\ []) do
    import_id =
      Keyword.get_lazy(opts, :import_id, fn ->
        "#{Path.basename(path)}-#{System.unique_integer([:positive])}"
      end)

    config = %{
      chunk_size: Keyword.get(opts, :chunk_size, 500),
      max_concurrency: Keyword.get(opts, :max_concurrency, System.schedulers_online()),
      max_retries: Keyword.get(opts, :max_retries, 5),
      base_backoff: Keyword.get(opts, :base_backoff_ms, 100)
    }

    Logger.info("Iniciando importação #{import_id} de #{path}")

    with {:ok, content} <- read_file(path),
         {:ok, columns, body} <- split_header(content),
         {:ok, rows} <- process_rows(body, columns, config),
         {:ok, result} <- commit_with_retry(import_id, rows, config) do
      Logger.info(
        "Importação #{import_id} concluída: #{length(rows)} linhas, resultado #{inspect(result)}"
      )

      {:ok, %{import_id: import_id, rows: length(rows), result: result}}
    else
      {:error, reason} = error ->
        Logger.error("Importação #{import_id} falhou (nada foi gravado): #{inspect(reason)}")
        error
    end
  end

  # Leitura do arquivo

  defp read_file(path) do
    case File.read(path) do
      {:ok, content} -> {:ok, content}
      {:error, reason} -> {:error, {:file, reason}}
    end
  end

  # Separa o cabeçalho e valida as colunas obrigatórias.
  defp split_header(content) do
    case String.split(content, ["\r\n", "\n"], trim: true) do
      [] ->
        {:error, :empty_file}

      [header | body] ->
        columns = split_csv(header)

        if Enum.all?(~w(id amount), &(&1 in columns)) do
          {:ok, columns, body}
        else
          {:error, {:invalid_header, columns}}
        end
    end
  end

  # Processamento concorrente dos lotes com limite de concorrência.
  # Qualquer linha inválida interrompe o processamento antes do commit.
  defp process_rows(body, columns, config) do
    body
    # Numeração começa em 2, pois a linha 1 é o cabeçalho.
    |> Enum.with_index(2)
    |> Enum.chunk_every(config.chunk_size)
    |> Task.async_stream(&parse_chunk(&1, columns),
      max_concurrency: config.max_concurrency,
      timeout: :infinity,
      ordered: true
    )
    |> Enum.reduce_while({:ok, []}, fn
      {:ok, {:ok, rows}}, {:ok, acc} ->
        {:cont, {:ok, [rows | acc]}}

      {:ok, {:error, _} = error}, _acc ->
        {:halt, error}
    end)
    |> case do
      {:ok, chunks} -> {:ok, chunks |> Enum.reverse() |> Enum.concat()}
      error -> error
    end
  end

  defp parse_chunk(indexed_lines, columns) do
    indexed_lines
    |> Enum.reduce_while({:ok, []}, fn {line, lineno}, {:ok, acc} ->
      case parse_row(line, columns, lineno) do
        {:ok, row} -> {:cont, {:ok, [row | acc]}}
        {:error, _} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, rows} -> {:ok, Enum.reverse(rows)}
      error -> error
    end
  end

  defp parse_row(line, columns, lineno) do
    values = split_csv(line)

    if length(values) != length(columns) do
      {:error, {:invalid_row, lineno, :column_count}}
    else
      row = columns |> Enum.zip(values) |> Map.new()

      with {:ok, id} <- fetch_id(row["id"], lineno),
           {:ok, amount} <- parse_amount(row["amount"], lineno) do
        {:ok, %{id: id, name: Map.get(row, "name", ""), amount: amount}}
      end
    end
  end

  defp fetch_id(id, _lineno) when is_binary(id) and id != "", do: {:ok, id}
  defp fetch_id(_id, lineno), do: {:error, {:invalid_row, lineno, :id}}

  defp parse_amount(raw, lineno) do
    case Float.parse(raw || "") do
      {value, ""} when value >= 0 -> {:ok, value}
      _ -> {:error, {:invalid_row, lineno, :amount}}
    end
  end

  # Parser simples de CSV: separador vírgula, sem suporte a campos entre aspas.
  defp split_csv(line) do
    line
    |> String.split(",")
    |> Enum.map(&String.trim/1)
  end

  # Commit atômico com retentativas para falhas temporárias.
  defp commit_with_retry(import_id, rows, config, attempt \\ 1) do
    case CSVImporter.FakeDB.commit(import_id, rows) do
      {:ok, _} = ok ->
        ok

      {:error, reason} when reason in @transient_errors and attempt <= config.max_retries ->
        # Backoff exponencial com jitter.
        delay =
          config.base_backoff * Integer.pow(2, attempt - 1) +
            :rand.uniform(max(config.base_backoff, 1))

        Logger.warning(
          "Falha temporária (#{inspect(reason)}) na tentativa #{attempt}; " <>
            "nova tentativa em #{delay} ms"
        )

        Process.sleep(delay)
        commit_with_retry(import_id, rows, config, attempt + 1)

      {:error, reason} when reason in @transient_errors ->
        {:error, {:retries_exhausted, reason}}

      {:error, reason} ->
        {:error, reason}
    end
  end
end

# Demonstração de uso

{:ok, _} = CSVImporter.FakeDB.start_link(fail_rate: 0.3)

path = Path.join(System.tmp_dir!(), "clientes.csv")
linhas = Enum.map(1..1_200, fn i -> "#{i},cliente#{i},#{i * 1.5}" end)
File.write!(path, Enum.join(["id,name,amount" | linhas], "\n"))

CSVImporter.import(path, chunk_size: 100, max_concurrency: 4, max_retries: 8)
|> IO.inspect(label: "resultado válido")

IO.inspect(CSVImporter.FakeDB.count(), label: "registros no banco")

# Arquivo com linha inválida: nenhuma linha deve ser gravada (atomicidade).
bad = Path.join(System.tmp_dir!(), "invalido.csv")
File.write!(bad, "id,name,amount\n5000,a,10\n5001,b,-5\n")

CSVImporter.import(bad)
|> IO.inspect(label: "resultado inválido")

IO.inspect(CSVImporter.FakeDB.count(), label: "registros no banco após falha")
