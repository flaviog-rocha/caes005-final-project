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

defmodule DeliverySystem do
  @moduledoc """
  Sistema de gerenciamento de entregas utilizando supervisão, processamento em stream
  e estado gerenciado por Agents.
  """
  use Supervisor

  def start_link(_), do: Supervisor.start_link(__MODULE__, :ok, name: __MODULE__)

  @impl true
  def init(:ok) do
    children = [
      {Registry, keys: :unique, name: DeliveryRegistry},
      {DynamicSupervisor, name: DeliverySupervisor, strategy: :one_for_one}
    ]
    Supervisor.init(children, strategy: :one_for_one)
  end

  # Requisito 2: Processar grandes volumes via Stream (Lazy loading)
  def process_delivery_stream(stream) do
    stream
    |> Stream.map(&handle_delivery/1)
    |> Stream.run()
  end

  defp handle_delivery(data) do
    child_spec = %{
      id: {DeliveryAgent, data.id},
      start: {DeliveryAgent, :start_link, [data]},
      restart: :transient # Requisito 3 e 5: Recuperação e detecção de encerramento
    }
    DynamicSupervisor.start_child(DeliverySupervisor, child_spec)
  end
end

defmodule DeliveryAgent do
  use Agent
  require Logger

  # Requisito 1: Notificações baseadas em condições
  def start_link(data) do
    Agent.start_link(fn -> data end, name: via_tuple(data.id))
  end

  def update_status(id, status) do
    Agent.get_and_update(via_tuple(id), fn state ->
      new_state = %{state | status: status}
      check_conditions(new_state)
      {new_state, new_state}
    end)
  end

  # Requisito 4: Implementação de novas tentativas (Retry logic)
  def perform_delivery_action(id, action, retries \\ 3) do
    try do
      action.()
    rescue
      _ ->
        if retries > 0 do
          :timer.sleep(1000)
          perform_delivery_action(id, action, retries - 1)
        else
          Logger.error("Falha persistente na entrega #{id}")
          raise "Max retries exceeded"
        end
    end
  end

  defp check_conditions(state) do
    if state.status == :delivered do
      Logger.info("Notificação: Entrega #{state.id} concluída com sucesso.")
    end
  end

  defp via_tuple(id), do: {:via, Registry, {DeliveryRegistry, id}}
end

# Exemplo de uso:
# Stream de dados simulando grande volume (lido via Stream.map/chunk/etc)
# data_stream = Stream.map(1..1_000_000, fn i -> %{id: i, status: :pending} end)
# DeliverySystem.process_delivery_stream(data_stream)
