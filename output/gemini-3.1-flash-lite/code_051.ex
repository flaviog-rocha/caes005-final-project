# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de transações financeiras, atendendo somente aos seguintes requisitos:
#
# 1. Permitir cadastrar, consultar, atualizar e remover registros.
# 2. Permitir consultar o estado atual das operações em andamento.
# 3. Executar determinadas operações periodicamente.
# 4. Processar grandes volumes de dados sem carregá-los integralmente na memória.
# 5. Permitir a execução de operações simultâneas.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - Registry
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule FinancialSystem do
  @moduledoc """
  Sistema de gerenciamento de transações utilizando GenServers, Registry 
  para rastreamento dinâmico e Streams para processamento de grandes volumes.
  """

  # 1. Cadastro, Consulta, Atualização e Remoção (via GenServer dinâmico)
  defmodule TransactionManager do
    use GenServer

    def start_link(id) do
      GenServer.start_link(__MODULE__, %{}, name: {:via, Registry, {TransactionRegistry, id}})
    end

    def init(state), do: {:ok, state}

    def handle_call({:update, data}, _from, state), do: {:reply, :ok, Map.merge(state, data)}
    def handle_call(:get, _from, state), do: {:reply, state, state}
    def handle_call(:delete, _from, _state), do: {:stop, :normal, :ok, %{}}

    # 2. Consultar estado atual das operações
    def list_all_active do
      Registry.select(TransactionRegistry, [{{:_, :_, :_}, [], [{{:key, :"$1"}}]}])
      |> Enum.map(fn %{key: id} -> {id, GenServer.call({:via, Registry, {TransactionRegistry, id}}, :get)} end)
    end
  end

  # Setup do Registry
  def start_registry do
    Registry.start_link(keys: :unique, name: TransactionRegistry)
  end

  # 3. Execução Periódica (usando Process.send_after)
  def schedule_task(interval, func) do
    spawn(fn ->
      Process.sleep(interval)
      func.()
      schedule_task(interval, func)
    end)
  end

  # 4. Processamento de grandes volumes sem carregar tudo na memória (Streams)
  def process_large_data(file_path) do
    File.stream!(file_path)
    |> Stream.map(&String.trim/1)
    |> Stream.map(&parse_transaction/1)
    |> Stream.each(fn tx -> 
      # 5. Operações Simultâneas (usando Task para não bloquear o fluxo)
      Task.start(fn -> 
        TransactionManager.start_link(tx.id)
        GenServer.call({:via, Registry, {TransactionRegistry, tx.id}}, {:update, tx})
      end)
    end)
    |> Stream.run()
  end

  defp parse_transaction(line) do
    [id, amount] = String.split(line, ",")
    %{id: id, amount: String.to_integer(amount)}
  end
end

# Exemplo de inicialização e uso:
# FinancialSystem.start_registry()
# FinancialSystem.TransactionManager.start_link("tx_1")
# GenServer.call({:via, Registry, {TransactionRegistry, "tx_1"}}, {:update, %{status: "pending"}})
# FinancialSystem.TransactionManager.list_all_active()
