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
  Gerenciador de transações idempotente persistente usando ETS.
  """
  use GenServer

  @table :transaction_store
  @dump_file "transactions.tab"

  # API Pública

  def start_link(_opts) do
    GenServer.start_link(__MODULE__, nil, name: __MODULE__)
  end

  @doc "Registra ou recupera uma transação. Garante idempotência pelo transaction_id."
  def process(transaction_id, payload) do
    GenServer.call(__MODULE__, {:process, transaction_id, payload})
  end

  @doc "Consulta o estado de uma transação."
  def get_status(transaction_id) do
    case :ets.lookup(@table, transaction_id) do
      [{^transaction_id, status, _payload}] -> {:ok, status}
      [] -> {:error, :not_found}
    end
  end

  # Callbacks do GenServer

  @impl true
  def init(_) do
    # Cria tabela ETS com proteção pública para leitura
    :ets.new(@table, [:set, :named_table, :public, read_concurrency: true])

    # Recupera estado do arquivo se existir
    if File.exists?(@dump_file) do
      :ets.tab2file(:ets.new(:tmp, []), @dump_file) # Só para carregar
      :ets.file2tab(@table, @dump_file)
    end

    {:ok, nil}
  end

  @impl true
  def handle_call({:process, id, payload}, _from, state) do
    case :ets.lookup(@table, id) do
      [{^id, status, _}] ->
        # Idempotência: Se já existe, retorna o estado sem reprocessar
        {:reply, {:already_processed, status}, state}

      [] ->
        # Persistência de novo registro
        :ets.insert(@table, {id, :processed, payload})
        save_to_disk()
        {:reply, {:ok, :processed}, state}
    end
  end

  # Funções Privadas

  defp save_to_disk do
    :ets.tab2file(@table, @dump_file, [{:extended_info, [:md5sum]}, :halt])
  end
end
