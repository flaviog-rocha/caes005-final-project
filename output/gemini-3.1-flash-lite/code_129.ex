# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de entregas e rastreamento, atendendo somente aos seguintes requisitos:
#
# 1. Recuperar processos após falhas inesperadas.
# 2. Permitir que diferentes entidades sejam processadas independentemente.
# 3. Implementar novas tentativas após falhas temporárias.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule DeliveryManager.Supervisor do
  use Supervisor

  def start_link(_), do: Supervisor.start_link(__MODULE__, :ok, name: __MODULE__)

  @impl true
  def init(:ok) do
    # O DynamicSupervisor permite o processamento independente de cada entidade (entrega)
    children = [
      {DynamicSupervisor, name: DeliveryManager.Registry, strategy: :one_for_one}
    ]

    Supervisor.init(children, strategy: :one_for_all)
  end
end

defmodule DeliveryManager.Worker do
  use GenServer

  # Requisito 3: Implementar novas tentativas após falhas temporárias (Retry com exponencial backoff)
  @max_retries 3

  def start_link(delivery_id) do
    GenServer.start_link(__MODULE__, delivery_id, name: {:via, Registry, {DeliveryManager.ProcessRegistry, delivery_id}})
  end

  @impl true
  def init(delivery_id) do
    # Requisito 1: Recuperação pós-falha (process_flag :trap_exit seria util em cenários complexos,
    # mas aqui o estado é recuperado ao reiniciar o processo via DynamicSupervisor)
    send(self(), :process_delivery)
    {:ok, %{id: delivery_id, retries: 0}}
  end

  @impl true
  def handle_info(:process_delivery, state) do
    case perform_delivery(state.id) do
      :ok ->
        {:stop, :normal, state}

      :error when state.retries < @max_retries ->
        Process.send_after(self(), :process_delivery, :timer.seconds(2 ** state.retries))
        {:noreply, %{state | retries: state.retries + 1}}

      :error ->
        {:stop, :failed, state}
    end
  end

  defp perform_delivery(id) do
    # Simulação de lógica de negócio
    IO.puts("Processando entrega: #{id}")
    :ok
  end
end

# Execução e organização dos requisitos
defmodule DeliveryManager do
  def start do
    # Registros para localizar processos de entidades independentes
    Registry.start_link(keys: :unique, name: DeliveryManager.ProcessRegistry)
    DeliveryManager.Supervisor.start_link([])
  end

  # Requisito 2: Entidades processadas independentemente
  def track_delivery(delivery_id) do
    DynamicSupervisor.start_child(
      DeliveryManager.Registry,
      {DeliveryManager.Worker, delivery_id}
    )
  end
end
