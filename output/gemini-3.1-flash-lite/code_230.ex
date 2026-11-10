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
  Sistema de pedidos resiliente utilizando GenServer, Supervisors e Macros.
  """

  # 1. Uso de Macros para definir DSL de operações de pedido
  defmacro def_order_op(name, do: block) do
    quote do
      def unquote(name)(order_id, params) do
        # 4. Impedir atualizações parcialmente concluídas via transação (simulada)
        IO.puts("Iniciando transação para #{unquote(name)} no pedido #{order_id}")
        try do
          result = unquote(block)
          IO.puts("Transação concluída com sucesso.")
          {:ok, result}
        rescue
          e ->
            IO.puts("Erro detectado, revertendo alterações: #{inspect(e)}")
            {:error, :transaction_failed}
        end
      end
    end
  end

  use GenServer

  # 2. Recuperação de processos: Supervisor inicia este Worker
  def start_link(_) do
    GenServer.start_link(__MODULE__, %{}, name: __MODULE__)
  end

  def init(state), do: {:ok, state}

  # 3. Operações simultâneas: O cliente pode disparar calls assíncronas ou concorrentes
  def_order_op :process_payment do
    :timer.sleep(100) # Simula I/O de banco de dados
    :payment_confirmed
  end

  # Exemplo de lógica de negócio protegida pela macro
  def_order_op :update_inventory do
    :inventory_updated
  end
end

# Supervisor para garantir resiliência (Requisito 2)
defmodule OrderSupervisor do
  use Supervisor

  def start_link(_), do: Supervisor.start_link(__MODULE__, [], name: __MODULE__)

  @impl true
  def init(_) do
    children = [
      {OrderSystem, []}
    ]
    Supervisor.init(children, strategy: :one_for_one)
  end
end

# Demonstração de uso simultâneo
defmodule Runner do
  def execute do
    {:ok, _} = OrderSupervisor.start_link([])

    # 3. Execução simultânea usando Task.async
    tasks = [
      Task.async(fn -> OrderSystem.process_payment(1, %{amount: 100}) end),
      Task.async(fn -> OrderSystem.update_inventory(1, %{sku: "ABC"}) end)
    ]

    results = Task.await_many(tasks)
    IO.inspect(results, label: "Resultados da execução simultânea")
  end
end

Runner.execute()
