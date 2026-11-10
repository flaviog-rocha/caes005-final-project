# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o processamento de arquivos CSV, atendendo somente aos seguintes requisitos:
#
# 1. Persistir os dados para recuperação após reinicializações.
# 2. Manter um histórico das operações realizadas.
# 3. Processar grandes volumes de dados sem carregá-los integralmente na memória.
# 4. Permitir cadastrar, consultar, atualizar e remover registros.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# Para rodar este código, certifique-se de ter o Elixir instalado.
# O sistema utiliza o módulo :ets para persistência em disco (via file-based dump)
# e Stream para processamento de arquivos sem carregar tudo na memória.

defmodule DataProcessor do
  @table_name :records_table
  @db_file "storage.ets"
  @log_file "operations.log"

  def init do
    # Cria a tabela ETS persistente
    :ets.new(@table_name, [:set, :public, :named_table, {:read_concurrency, true}])
    load_from_disk()
  end

  # Requisito 3: Processamento via Stream (lazy)
  def process_csv(path) do
    File.stream!(path)
    |> Stream.drop(1) # Ignorar header
    |> Stream.map(&String.trim/1)
    |> Stream.map(&String.split(&1, ","))
    |> Stream.each(fn [id | data] -> insert(id, data) end)
    |> Stream.run()
  end

  # Requisito 4: CRUD
  def insert(id, data) do
    :ets.insert(@table_name, {id, data})
    log_operation(:insert, id)
    save_to_disk()
  end

  def get(id), do: :ets.lookup(@table_name, id)

  def update(id, data) do
    if :ets.member(@table_name, id) do
      :ets.insert(@table_name, {id, data})
      log_operation(:update, id)
      save_to_disk()
    end
  end

  def delete(id) do
    :ets.delete(@table_name, id)
    log_operation(:delete, id)
    save_to_disk()
  end

  # Requisito 1: Persistência
  defp save_to_disk do
    :ets.tab2file(@table_name, String.to_charlist(@db_file))
  end

  defp load_from_disk do
    if File.exists?(@db_file) do
      :ets.file2tab(String.to_charlist(@db_file))
    end
  end

  # Requisito 2: Histórico de operações
  defp log_operation(op, id) do
    timestamp = DateTime.utc_now() |> DateTime.to_string()
    log_entry = "#{timestamp} | OP: #{op} | ID: #{id}\n"
    File.write(@log_file, log_entry, [:append])
  end
end

# Exemplo de uso:
# DataProcessor.init()
# DataProcessor.process_csv("data.csv")
# DataProcessor.get("1")
# DataProcessor.update("1", ["Novo Valor"])
# DataProcessor.delete("1")
