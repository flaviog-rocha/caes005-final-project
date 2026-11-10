# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de entregas e rastreamento, atendendo somente aos seguintes requisitos:
#
# 1. Recuperar processos após falhas inesperadas.
# 2. Permitir que diferentes entidades sejam processadas independentemente.
# 3. Implementar novas tentativas após falhas temporárias.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule DeliverySystem.Supervisor do
  @moduledoc """
  Supervisor que gerencia o ciclo de vida dos processos de entrega.
  Garante a recuperação após falhas (Requisito 1).
  """
  use Supervisor

  def start_link(_), do: Supervisor.start_link(__MODULE__, :ok, name: __MODULE__)

  @impl true
  def init(:ok) do
    # Cada entrega será supervisionada de forma independente (Requisito 2)
    # Estratégia :one_for_one garante isolamento.
    children = [
      {DynamicSupervisor, name: DeliverySystem.DeliverySupervisor, strategy: :one_for_one}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end

defmodule DeliverySystem.Worker do
  @moduledoc """
  Worker que processa a entrega com lógica de retry (Requisito 3).
  """
  use GenServer, restart: :transient

  def start_link(delivery_id), do: GenServer.start_link(__MODULE__, delivery_id)

  @impl true
  def init(delivery_id) do
    send(self(), :process)
    {:ok, %{id: delivery_id, retries: 0, max_retries: 3}}
  end

  @impl true
  def handle_info(:process, state) do
    case perform_delivery(state.id) do
      :ok ->
        {:stop, :normal, state}

      {:error, :temporary} when state.retries < state.max_retries ->
        :timer.sleep(1000) # Backoff simples
        {:noreply, %{state | retries: state.retries + 1}}

      {:error, reason} ->
        {:stop, reason, state}
    end
  end

  # Simulação de lógica de negócio
  defp perform_delivery(id) do
    case :rand.uniform(10) do
      n when n > 8 -> :ok
      n when n > 5 -> {:error, :temporary}
      _ -> {:error, :permanent}
    end
  end
end

defmodule DeliverySystem do
  @doc """
  API para iniciar o processamento de uma entidade independente.
  """
  def process_delivery(delivery_id) do
    DynamicSupervisor.start_child(
      DeliverySystem.DeliverySupervisor,
      {DeliverySystem.Worker, delivery_id}
    )
  end
end
