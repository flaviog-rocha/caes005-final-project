# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o processamento de arquivos CSV, atendendo somente aos seguintes requisitos:
#
# 1. Persistir os dados para recuperação após reinicializações.
# 2. Manter um histórico das operações realizadas.
# 3. Processar grandes volumes de dados sem carregá-los integralmente na memória.
# 4. Permitir cadastrar, consultar, atualizar e remover registros.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule CsvProcessor do
  @moduledoc """
  Processador de CSV de alta performance usando Streams para memória constante
  e DETS (Disk Erlang Term Storage) para persistência em disco.
  """

  @db_file 'records.dets'
  @log_file 'operations.log'

  def init do
    {:ok, _} = :dets.open_file(:records, file: @db_file)
    :ok
  end

  # Requisito 3: Processamento por Stream (memória constante)
  def process_csv(file_path) do
    file_path
    |> File.stream!()
    |> Stream.drop(1) # Ignora cabeçalho
    |> Stream.map(&String.trim/1)
    |> Stream.map(&String.split(&1, ","))
    |> Stream.each(fn [id | data] -> save(id, data) end)
    |> Stream.run()
  end

  # Requisito 1 & 4: CRUD com Persistência
  def save(id, data) do
    :dets.insert(:records, {id, data})
    log_operation("SAVE", id)
  end

  def get(id) do
    case :dets.lookup(:records, id) do
      [{^id, data}] -> {:ok, data}
      [] -> {:error, :not_found}
    end
  end

  def update(id, new_data) do
    :dets.insert(:records, {id, new_data})
    log_operation("UPDATE", id)
  end

  def delete(id) do
    :dets.delete(:records, id)
    log_operation("DELETE", id)
  end

  # Requisito 2: Histórico de operações
  defp log_operation(action, id) do
    timestamp = DateTime.utc_now() |> DateTime.to_string()
    log_entry = "#{timestamp} | #{action} | ID: #{id}\n"
    File.write!(@log_file, log_entry, [:append])
  end

  def close do
    :dets.close(:records)
  end
end

# Exemplo de uso:
# CsvProcessor.init()
# CsvProcessor.process_csv("dados.csv")
# CsvProcessor.get("123")
# CsvProcessor.update("123", ["novo_dado"])
# CsvProcessor.delete("123")
# CsvProcessor.close()
