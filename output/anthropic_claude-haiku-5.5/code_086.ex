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

# Processador de CSV com atomicidade, novas tentativas e limite de concorrência.
# Execução: elixir csv_processor.exs

defmodule CsvProcessor do
  @moduledoc """
  Processa arquivos CSV garantindo:
    1. Atomicidade: cada arquivo é validado por completo e gravado em uma única
       transação. Se qualquer etapa falhar, nenhuma alteração é persistida.
    2. Novas tentativas: falhas temporárias (`{:transient, _}`) são repetidas com
       backoff exponencial e jitter. Falhas permanentes não são repetidas.
    3. Concorrência limitada: `Task.async_stream/3` com `max_concurrency`.
  """

  require Logger

  @header ~w(id name amount)
  @default_max_attempts 4
  @default_base_delay_ms 200

  defmodule Store do
    @moduledoc """
    Banco de dados fictício em memória (Agent).

    Cada escrita de lote ocorre dentro de uma única chamada `Agent.get_and_update/2`,
    o que torna a operação atômica: ou o estado inteiro é substituído pelo novo,
    ou permanece o anterior. Falhas transitórias são simuladas ANTES da aplicação
    das mudanças, então nunca deixam dados parcialmente gravados.

    Em um sistema real, este módulo seria substituído por um repositório Ecto
    usando `Repo.transaction/1`. Um esquema equivalente seria definido por um
    `Ecto.Migration` com uma tabela `records` (id :integer PK, name :string,
    amount :integer), mas a migração não é obrigatória para este exemplo.
    """

    @doc "Inicia o armazenamento. `failure_rate` (0.0..1.0) simula falhas transitórias."
    def start_link(opts \\ []) do
      failure_rate = Keyword.get(opts, :failure_rate, 0.0)

      Agent.start_link(
        fn -> %{records: %{}, failure_rate: failure_rate, commits: 0} end,
        name: __MODULE__
      )
    end

    @doc "Aplica atomicamente um lote de registros (upsert). Retorna `{:ok, total}` ou `{:error, reason}`."
    def commit_batch(records) when is_map(records) do
      Agent.get_and_update(__MODULE__, fn state ->
        if :rand.uniform() < state.failure_rate do
          # Falha antes de qualquer escrita: nada é aplicado.
          {{:error, {:transient, :connection_timeout}}, state}
        else
          new_state = %{
            state
            | records: Map.merge(state.records, records),
              commits: state.commits + 1
          }

          {{:ok, map_size(records)}, new_state}
        end
      end)
    end

    @doc "Retorna todos os registros persistidos."
    def all, do: Agent.get(__MODULE__, & &1.records)
  end

  # ---------------------------------------------------------------------------
  # API pública
  # ---------------------------------------------------------------------------

  @doc """
  Processa um único arquivo de forma atômica, com novas tentativas para
  falhas temporárias.

  Opções:
    * `:max_attempts`   - número máximo de tentativas (padrão: #{@default_max_attempts})
    * `:base_delay_ms`  - atraso base do backoff em ms (padrão: #{@default_base_delay_ms})
  """
  def process_file(path, opts \\ []) do
    max_attempts = Keyword.get(opts, :max_attempts, @default_max_attempts)
    base_delay = Keyword.get(opts, :base_delay_ms, @default_base_delay_ms)

    Logger.info("Iniciando processamento de #{path}")

    with {:ok, content} <- read_file(path),
         {:ok, records} <- parse_csv(content),
         {:ok, count} <-
           with_retry(fn -> Store.commit_batch(records) end, max_attempts, base_delay, path, 1) do
      Logger.info("Arquivo #{Path.basename(path)} concluído: #{count} registro(s) gravado(s)")
      {:ok, count}
    else
      {:error, reason} = error ->
        Logger.error("Falha ao processar #{Path.basename(path)}: #{inspect(reason)}. Nada foi gravado.")
        error
    end
  end

  @doc """
  Processa vários arquivos com no máximo `:max_concurrency` operações simultâneas.
  Retorna uma lista `[{path, resultado}]` na mesma ordem de entrada.

  Opções:
    * `:max_concurrency` - limite de arquivos processados ao mesmo tempo
      (padrão: `System.schedulers_online/0`)
    * demais opções são repassadas para `process_file/2`
  """
  def process_files(paths, opts \\ []) do
    max_concurrency = Keyword.get(opts, :max_concurrency, System.schedulers_online())

    paths
    |> Task.async_stream(&process_file(&1, opts),
      max_concurrency: max_concurrency,
      timeout: :infinity,
      ordered: true
    )
    |> Enum.zip(paths)
    |> Enum.map(fn {{:ok, result}, path} -> {path, result} end)
  end

  # ---------------------------------------------------------------------------
  # Leitura e validação
  # ---------------------------------------------------------------------------

  defp read_file(path) do
    case File.read(path) do
      {:ok, content} -> {:ok, content}
      {:error, reason} -> {:error, {:file, reason}}
    end
  end

  @doc false
  # Converte o conteúdo CSV em um mapa %{id => %{name:, amount:}}.
  # Qualquer linha inválida invalida o arquivo inteiro (validação completa antes da escrita).
  # Observação: o parser é simplificado e não trata campos entre aspas com vírgulas.
  def parse_csv(content) do
    case String.split(content, ["\r\n", "\n"], trim: true) do
      [] ->
        {:error, :empty_file}

      [header | lines] ->
        columns = header |> String.split(",") |> Enum.map(&String.trim/1)

        if columns == @header do
          parse_lines(lines)
        else
          {:error, {:invalid_header, columns}}
        end
    end
  end

  defp parse_lines(lines) do
    lines
    |> Enum.with_index(2)
    |> Enum.reduce_while({:ok, %{}}, fn {line, lineno}, {:ok, acc} ->
      case parse_line(line) do
        {:ok, {id, row}} ->
          if Map.has_key?(acc, id) do
            {:halt, {:error, {:duplicate_id, id, lineno}}}
          else
            {:cont, {:ok, Map.put(acc, id, row)}}
          end

        {:error, reason} ->
          {:halt, {:error, {:invalid_line, lineno, reason}}}
      end
    end)
  end

  defp parse_line(line) do
    case line |> String.split(",") |> Enum.map(&String.trim/1) do
      [id, name, amount] ->
        with {id_int, ""} <- Integer.parse(id),
             {amount_int, ""} <- Integer.parse(amount),
             :ok <- validate_name(name) do
          {:ok, {id_int, %{name: name, amount: amount_int}}}
        else
          :error -> {:error, :invalid_number}
          {_, _} -> {:error, :invalid_number}
          {:error, reason} -> {:error, reason}
        end

      fields ->
        {:error, {:wrong_field_count, length(fields)}}
    end
  end

  defp validate_name(""), do: {:error, :empty_name}
  defp validate_name(_name), do: :ok

  # ---------------------------------------------------------------------------
  # Novas tentativas
  # ---------------------------------------------------------------------------

  # Repete `fun` apenas quando ele retorna `{:error, {:transient, _}}`.
  # Como cada tentativa é atômica, repetir não causa gravações duplicadas parciais.
  defp with_retry(fun, max_attempts, base_delay, label, attempt) do
    case fun.() do
      {:error, {:transient, reason}} when attempt < max_attempts ->
        delay = backoff(base_delay, attempt)

        Logger.warning(
          "[#{Path.basename(label)}] falha temporária (#{inspect(reason)}) " <>
            "na tentativa #{attempt}/#{max_attempts}; nova tentativa em #{delay} ms"
        )

        Process.sleep(delay)
        with_retry(fun, max_attempts, base_delay, label, attempt + 1)

      {:error, {:transient, reason}} ->
        {:error, {:retries_exhausted, reason}}

      other ->
        other
    end
  end

  # Backoff exponencial com jitter: base * 2^(tentativa-1) + jitter aleatório.
  defp backoff(base_delay, attempt) do
    base_delay * Integer.pow(2, attempt - 1) + :rand.uniform(base_delay)
  end

  # ---------------------------------------------------------------------------
  # Demonstração
  # ---------------------------------------------------------------------------

  @doc false
  def main do
    {:ok, _} = Application.ensure_all_started(:logger)
    {:ok, _} = Store.start_link(failure_rate: 0.3)

    dir = System.tmp_dir!()

    paths = [
      write_sample(dir, "clientes_1.csv", "id,name,amount\n1,Ana,100\n2,Bruno,250\n"),
      write_sample(dir, "clientes_2.csv", "id,name,amount\n3,Carla,300\n4,Diego,40\n"),
      # Linha 3 inválida: o arquivo inteiro deve ser rejeitado (sem gravação parcial).
      write_sample(dir, "clientes_invalido.csv", "id,name,amount\n5,Eva,10\n6,,20\n")
    ]

    results =
      process_files(paths,
        max_concurrency: 2,
        max_attempts: 4,
        base_delay_ms: 100
      )

    Enum.each(results, fn {path, result} ->
      Logger.info("Resultado #{Path.basename(path)}: #{inspect(result)}")
    end)

    Logger.info("Estado final do banco: #{inspect(Store.all())}")
    :ok
  end

  defp write_sample(dir, name, content) do
    path = Path.join(dir, name)
    File.write!(path, content)
    path
  end
end

CsvProcessor.main()
