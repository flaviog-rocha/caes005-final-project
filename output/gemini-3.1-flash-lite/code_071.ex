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
  defstruct [:id, :status, :retries]

  @doc "Valida a estrutura do pedido"
  def validate(%{id: id}) when is_binary(id) and byte_size(id) > 0, do: {:ok, :valid}
  def validate(_), do: {:error, :invalid_order_data}
end

defmodule OrderSystem.Processor do
  use GenServer
  require Logger

  @max_retries 3
  @expiry_ms 60_000 # 1 minuto para expiração simulada

  # API
  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  def process_order(order) do
    case OrderSystem.Order.validate(order) do
      {:ok, :valid} -> GenServer.cast(__MODULE__, {:process, order})
      error -> error
    end
  end

  # Callbacks
  @impl true
  def init(_), do: {:ok, %{pending: %{}, expiry_timer: nil}}

  @impl true
  def handle_cast({:process, order}, state) do
    # Simula comunicação com banco (Ecto-like)
    case attempt_db_operation(order) do
      :ok ->
        {:noreply, state}
      :error ->
        new_state = handle_retry(order, state)
        {:noreply, new_state}
    end
  end

  @impl true
  def handle_info({:retry, order}, state) do
    Logger.info("Tentando processar novamente: #{order.id}")
    send(self(), {:process, order})
    {:noreply, state}
  end

  defp attempt_db_operation(_order), do: :error # Simula falha temporária

  defp handle_retry(order, state) do
    retries = Map.get(order, :retries, 0)
    if retries < @max_retries do
      Process.send_after(self(), {:retry, %{order | retries: retries + 1}}, 1000)
      state
    else
      # Expiração/Descarte após tentativas falhas
      Logger.error("Pedido #{order.id} expirado após falhas.")
      state
    end
  end
end

defmodule OrderSystem.Supervisor do
  use Supervisor

  def start_link(_), do: Supervisor.start_link(__MODULE__, :ok, name: __MODULE__)

  @impl true
  def init(:ok) do
    children = [
      {OrderSystem.Processor, []}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end

# Exemplo de execução:
# OrderSystem.Supervisor.start_link([])
# OrderSystem.Processor.process_order(%OrderSystem.Order{id: "ORD-123"})
