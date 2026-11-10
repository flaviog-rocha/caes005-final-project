# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de pedidos de um e-commerce, atendendo somente aos seguintes requisitos:
#
# 1. Fazer uso de macros.
# 2. Recuperar processos após falhas inesperadas.
# 3. Permitir a execução de operações simultâneas.
# 4. Impedir atualizações parcialmente concluídas.
# 5. Quando fizer sentido para o domínio, considerar a comunicação com um banco de dados fictício e, opcionalmente, o uso de Ecto.Migration, sem tornar isso obrigatório.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule OrderSystem do
  @moduledoc """
  Sistema de pedidos utilizando Supervisores, GenServer e macros para transações.
  """

  # 1. Uso de macros para garantir atomicidade (transação fictícia)
  defmacro transaction(do: block) do
    quote do
      try do
        # Simula o início de uma transação de banco de dados
        IO.puts("[DB] Iniciando transação...")
        result = unquote(block)
        IO.puts("[DB] Commit realizado com sucesso.")
        result
      rescue
        e ->
          IO.puts("[DB] Rollback executado devido a: #{inspect(e)}")
          {:error, :transaction_failed}
      end
    end
  end

  defmodule OrderManager do
    use GenServer

    # 2. Recuperação de processos (Supervisão configurada no módulo principal)
    def start_link(_), do: GenServer.start_link(__MODULE__, %{}, name: __MODULE__)

    def init(state), do: {:ok, state}

    def handle_call({:process_order, order_id}, _from, state) do
      # 3. Execução simultânea (simulada via delay)
      Task.start(fn ->
        require OrderSystem
        OrderSystem.transaction do
          # 4. Impedir atualizações parciais
          # Se algo falhar aqui, o rescue da macro garante o rollback
          IO.puts("Processando pedido #{order_id}...")
          Process.sleep(1000)
          if order_id == "fail", do: raise "Erro de estoque"
        end
      end)

      {:reply, :ok, Map.put(state, order_id, :processing)}
    end
  end

  # 5. Infraestrutura de Supervisão (Fault Tolerance)
  defmodule Supervisor do
    use Elixir.Supervisor

    def start_link(_), do: Elixir.Supervisor.start_link(__MODULE__, :ok, name: __MODULE__)

    def init(:ok) do
      children = [
        {OrderManager, []}
      ]
      # A estratégia :one_for_one garante que o processo seja reiniciado após falha
      Elixir.Supervisor.init(children, strategy: :one_for_one)
    end
  end
end

# Execução do sistema
{:ok, _pid} = OrderSystem.Supervisor.start_link([])

# Demonstração de uso simultâneo
OrderSystem.OrderManager.handle_call({:process_order, "123"}, nil, %{})
OrderSystem.OrderManager.handle_call({:process_order, "fail"}, nil, %{})

# Aguardar processamento das tarefas assíncronas
Process.sleep(2000)
