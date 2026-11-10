# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de entregas e rastreamento, atendendo somente aos seguintes requisitos:
#
# 1. Gerar notificações quando determinadas condições forem atingidas.
# 2. Processar grandes volumes de dados sem carregá-los integralmente na memória.
# 3. Recuperar processos após falhas inesperadas.
# 4. Implementar novas tentativas após falhas temporárias.
# 5. Detectar encerramentos inesperados de processos.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - Agent
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule DeliveryManager do
  @moduledoc """
  Sistema de gerenciamento de entregas utilizando Agents, supervisionamento 
  para resiliência e Streams para processamento eficiente de memória.
  """

  use Agent

  # 1. Gerenciamento de Estado via Agent
  def start_link(initial_state) do
    Agent.start_link(fn -> initial_state end, name: __MODULE__)
  end

  # 2. Processamento de grandes volumes (Lazy Streams)
  # O uso de Stream permite processar arquivos/listas gigantes sem carregar tudo na RAM.
  def process_deliveries(delivery_stream) do
    delivery_stream
    |> Stream.map(&handle_delivery/1)
    |> Stream.run()
  end

  defp handle_delivery(delivery) do
    # 4. Implementação de novas tentativas (Retry com recursão)
    retry_operation(fn -> perform_delivery_logic(delivery) end, 3)
  end

  defp perform_delivery_logic(delivery) do
    # Simulação de lógica de negócio
    if delivery.status == :pending do
      # 1. Notificação de condição
      IO.puts("Notificação: Entrega #{delivery.id} está pronta para saída.")
      
      # Atualiza estado no Agent
      Agent.update(__MODULE__, fn state -> 
        Map.put(state, delivery.id, :in_transit) 
      end)
    end
  end

  defp retry_operation(fun, retries) do
    try do
      fun.()
    rescue
      _ -> if retries > 0, do: retry_operation(fun, retries - 1), else: raise "Falha persistente"
    end
  end
end

# 3 & 5. Supervisão e Detecção de Falhas
# Utilizando Supervisor para garantir que o Agent seja reiniciado em caso de falha (Restart strategy)
defmodule DeliverySupervisor do
  use Supervisor

  def start_link(_) do
    Supervisor.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  @impl true
  def init(:ok) do
    children = [
      # O Agent é monitorado; se o processo morrer, o Supervisor o reinicia
      {DeliveryManager, %{}} 
    ]

    # one_for_one: Se o processo falhar, apenas ele é reiniciado
    Supervisor.init(children, strategy: :one_for_one)
  end
end

# Exemplo de execução simulando stream de dados
# O sistema processa linha a linha de uma fonte de dados infinita ou muito grande
deliveries = Stream.map(1..1000, fn i -> %{id: i, status: :pending} end)

# Inicialização
{:ok, _} = DeliverySupervisor.start_link([])
DeliveryManager.process_deliveries(deliveries)
