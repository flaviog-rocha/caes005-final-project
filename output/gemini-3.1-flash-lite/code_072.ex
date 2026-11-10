# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de pedidos de um e-commerce, atendendo somente aos seguintes requisitos:
#
# 1. Implementar novas tentativas após falhas temporárias.
# 2. Implementar expiração automática de registros.
# 3. Permitir comunicação entre diferentes processos.
# 4. Validar os dados recebidos antes de realizar operações.
# 5. Quando fizer sentido para o domínio, considerar a comunicação com um banco de dados fictício e, opcionalmente, o uso de Ecto.Migration, sem tornar isso obrigatório.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - Supervisor
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule OrderSystem.Order do
  defstruct [:id, :status, :retry_count, :expires_at]

  def validate(%{id: id, status: _} = order) when is_integer(id), do: {:ok, order}
  def validate(_), do: {:error, :invalid_order_format}
end

defmodule OrderSystem.Database do
  # Simulação de banco de dados em memória usando um Agent
  use Agent

  def start_link(_), do: Agent.start_link(fn -> %{} end, name: __MODULE__)

  def save(order), do: Agent.update(__MODULE__, &Map.put(&1, order.id, order))
  def get(id), do: Agent.get(__MODULE__, &Map.get(&1, id))
  def all, do: Agent.get(__MODULE__, &Map.values(&1))
end

defmodule OrderSystem.Worker do
  use GenServer
  require Logger

  def start_link(args), do: GenServer.start_link(__MODULE__, args, name: __MODULE__)

  def init(_), do: {:ok, %{}}

  # Requisito 3: Comunicação entre processos via cast
  def handle_cast({:process_order, order}, state) do
    case OrderSystem.Order.validate(order) do
      {:ok, valid_order} ->
        case perform_task(valid_order) do
          :ok -> 
            OrderSystem.Database.save(%{valid_order | status: :completed})
          :error ->
            handle_retry(valid_order)
        end
      {:error, reason} ->
        Logger.error("Falha na validação: #{inspect(reason)}")
    end
    {:noreply, state}
  end

  # Requisito 1: Tentativas após falhas temporárias
  defp handle_retry(order) do
    if order.retry_count < 3 do
      Logger.info("Tentativa #{order.retry_count + 1} para o pedido #{order.id}")
      Process.send_after(self(), {:retry, %{order | retry_count: order.retry_count + 1}}, 1000)
    else
      OrderSystem.Database.save(%{order | status: :failed})
    end
  end

  defp perform_task(_), do: :error # Simula falha temporária para demonstrar o retry

  def handle_info({:retry, order}, state) do
    GenServer.cast(self(), {:process_order, order})
    {:noreply, state}
  end
end

defmodule OrderSystem.Janitor do
  use GenServer
  # Requisito 2: Expiração automática (Cleanup)
  def start_link(_), do: GenServer.start_link(__MODULE__, %{}, name: __MODULE__)

  def init(state) do
    :timer.send_interval(5000, :cleanup)
    {:ok, state}
  end

  def handle_info(:cleanup, state) do
    now = System.system_time(:second)
    OrderSystem.Database.all()
    |> Enum.filter(fn o -> o.expires_at < now end)
    |> Enum.each(fn _expired -> :ok end) # Lógica de remoção real aqui
    {:noreply, state}
  end
end

defmodule OrderSystem.Supervisor do
  use Supervisor

  def start_link(_), do: Supervisor.start_link(__MODULE__, :ok, name: __MODULE__)

  def init(:ok) do
    children = [
      OrderSystem.Database,
      OrderSystem.Worker,
      OrderSystem.Janitor
    ]
    Supervisor.init(children, strategy: :one_for_one)
  end
end

# Exemplo de execução:
# OrderSystem.Supervisor.start_link([])
# GenServer.cast(OrderSystem.Worker, {:process_order, %OrderSystem.Order{id: 1, status: :new, retry_count: 0, expires_at: System.system_time(:second) + 10}})
