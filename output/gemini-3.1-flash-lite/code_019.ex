# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de entregas e rastreamento, atendendo somente aos seguintes requisitos:
#
# 1. Validar os dados recebidos antes de realizar operações.
# 2. Quando fizer sentido para o domínio, considerar a comunicação com um banco de dados fictício e, opcionalmente, o uso de Ecto.Migration, sem tornar isso obrigatório.
# 3. Recuperar processos após falhas inesperadas.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - DynamicSupervisor
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule DeliverySystem do
  @moduledoc """
  Sistema de gerenciamento de entregas utilizando DynamicSupervisor para
  isolamento de processos e recuperação de falhas.
  """

  use Supervisor

  def start_link(_), do: Supervisor.start_link(__MODULE__, :ok, name: __MODULE__)

  @impl true
  def init(:ok) do
    children = [
      {DynamicSupervisor, name: DeliverySupervisor, strategy: :one_for_one}
    ]
    Supervisor.init(children, strategy: :one_for_one)
  end

  # --- Interface Pública ---

  def create_delivery(id, destination) do
    # 1. Validação dos dados
    with {:ok, params} <- validate_params(id, destination) do
      DynamicSupervisor.start_child(DeliverySupervisor, {DeliveryWorker, params})
    end
  end

  defp validate_params(id, dest) when is_binary(id) and is_binary(dest) and id != "", do: {:ok, {id, dest}}
  defp validate_params(_, _), do: {:error, :invalid_input}
end

defmodule DeliveryWorker do
  use GenServer, restart: :transient # 3. Recuperação de falhas (restart: :transient)

  # --- API do Worker ---

  def start_link({id, destination}) do
    GenServer.start_link(__MODULE__, {id, destination}, name: via_tuple(id))
  end

  @impl true
  def init({id, destination}) do
    # 2. Simulação de interação com DB (Carregamento de estado)
    IO.puts("Iniciando monitoramento para entrega: #{id} destino: #{destination}")
    {:ok, %{id: id, destination: destination, status: :in_transit}}
  end

  @impl true
  def handle_call(:get_status, _from, state) do
    {:reply, state.status, state}
  end

  # Simulação de falha inesperada para testar recuperação
  @impl true
  def handle_cast(:trigger_failure, _state) do
    raise "Falha crítica no sistema de rastreamento!"
  end

  defp via_tuple(id), do: {:via, Registry, {DeliveryRegistry, id}}
end

# --- Setup necessário para o sistema ---

defmodule SystemSupervisor do
  use Supervisor

  def start_link(_), do: Supervisor.start_link(__MODULE__, :ok)

  @impl true
  def init(:ok) do
    children = [
      {Registry, keys: :unique, name: DeliveryRegistry},
      DeliverySystem
    ]
    Supervisor.init(children, strategy: :one_for_one)
  end
end
