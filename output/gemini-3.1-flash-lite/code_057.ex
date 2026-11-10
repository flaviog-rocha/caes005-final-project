# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de transações financeiras, atendendo somente aos seguintes requisitos:
#
# 1. Persistir os dados para recuperação após reinicializações.
# 2. Impedir que uma mesma operação seja executada mais de uma vez.
# 3. Permitir consultar o estado atual das operações em andamento.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - ETS
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule TransactionManager do
  @moduledoc """
  Gerenciador de transações idempotentes utilizando ETS.
  """
  use GenServer

  @table_name :transaction_store
  @file_path "transactions.tab"

  # API Pública

  def start_link(_opts) do
    GenServer.start_link(__MODULE__, nil, name: __MODULE__)
  end

  @doc "Registra ou recupera o estado de uma transação. Impede duplicidade."
  def process(transaction_id, payload) do
    GenServer.call(__MODULE__, {:process, transaction_id, payload})
  end

  @doc "Consulta todas as transações em andamento."
  def list_transactions do
    :ets.tab2list(@table_name)
  end

  # Callbacks do GenServer

  @impl true
  def init(_) do
    # Cria a tabela. Se o arquivo existir, carrega os dados.
    table = :ets.new(@table_name, [:named_table, :set, :public, read_concurrency: true])
    
    if File.exists?(@file_path) do
      :ets.file2tab(@table_name, String.to_charlist(@file_path))
    end

    {:ok, table}
  end

  @impl true
  def handle_call({:process, id, payload}, _from, state) do
    case :ets.lookup(@table_name, id) do
      [{^id, status, _data}] ->
        {:reply, {:error, :already_processed, status}, state}

      [] ->
        # Persistência atômica: insere e salva no disco
        :ets.insert(@table_name, {id, :processing, payload})
        :ets.tab2file(@table_name, String.to_charlist(@file_path))
        
        {:reply, {:ok, :started}, state}
    end
  end

  @impl true
  def terminate(_reason, _state) do
    :ets.tab2file(@table_name, String.to_charlist(@file_path))
  end
end
