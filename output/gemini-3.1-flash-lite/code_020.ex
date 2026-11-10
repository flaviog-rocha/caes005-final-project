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

defmodule DeliverySystem.Delivery do
  @moduledoc """
  Representa uma entidade de entrega com validação básica.
  """
  defstruct [:id, :status, :destination]

  def validate(%{id: id, destination: dest}) when is_binary(id) and is_binary(dest) do
    {:ok, %__MODULE__{id: id, status: :pending, destination: dest}}
  end

  def validate(_), do: {:error, :invalid_data}
end

defmodule DeliverySystem.Worker do
  @moduledoc """
  Worker que gerencia o estado de uma entrega individual.
  Utiliza GenServer para manter o estado em memória.
  """
  use GenServer

  def start_link(delivery) do
    GenServer.start_link(__MODULE__, delivery, name: via_tuple(delivery.id))
  end

  def init(delivery), do: {:ok, delivery}

  def handle_call(:get_status, _from, state), do: {:reply, state.status, state}

  def handle_cast({:update_status, new_status}, state) do
    # Simulação de persistência (ex: Repo.update)
    {:noreply, %{state | status: new_status}}
  end

  defp via_tuple(id), do: {:via, Registry, {DeliverySystem.Registry, id}}
end

defmodule DeliverySystem.Manager do
  @moduledoc """
  Gerencia o ciclo de vida das entregas via DynamicSupervisor.
  """
  use DynamicSupervisor

  def start_link(_), do: DynamicSupervisor.start_link(__MODULE__, :ok, name: __MODULE__)

  def init(:ok), do: DynamicSupervisor.init(strategy: :one_for_one)

  def register_delivery(data) do
    with {:ok, delivery} <- DeliverySystem.Delivery.validate(data) do
      spec = {DeliverySystem.Worker, delivery}
      DynamicSupervisor.start_child(__MODULE__, spec)
    end
  end
end

defmodule DeliverySystem.Application do
  @moduledoc """
  Supervision Tree que garante a recuperação de processos.
  O Registry permite localizar workers após falhas (self-healing).
  """
  use Application

  def start(_type, _args) do
    children = [
      {Registry, keys: :unique, name: DeliverySystem.Registry},
      DeliverySystem.Manager
    ]

    Supervisor.start_link(children, strategy: :one_for_one)
  end
end

# --- Exemplo de uso ---
# 1. Iniciar: DeliverySystem.Application.start(:normal, [])
# 2. Registrar: DeliverySystem.Manager.register_delivery(%{id: "DEL-001", destination: "Rua A"})
# 3. Consultar: GenServer.call({:via, Registry, {DeliverySystem.Registry, "DEL-001"}}, :get_status)
