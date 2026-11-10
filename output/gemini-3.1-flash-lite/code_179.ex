# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de gerenciamento de notificações, atendendo somente aos seguintes requisitos:
#
# 1. Implementar novas tentativas após falhas temporárias.
# 2. Recuperar processos após falhas inesperadas.
# 3. Permitir que diferentes entidades sejam processadas independentemente.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule NotificationSystem do
  @moduledoc """
  Sistema de notificações resiliente utilizando supervisionamento por árvore
  e estratégias de retry com exponential backoff.
  """
  use Supervisor

  def start_link(_), do: Supervisor.start_link(__MODULE__, :ok, name: __MODULE__)

  @impl true
  def init(:ok) do
    # O DynamicSupervisor permite processar entidades independentemente
    children = [
      {DynamicSupervisor, name: NotificationManager, strategy: :one_for_one}
    ]

    Supervisor.init(children, strategy: :one_for_all)
  end

  @doc "Dispara o envio de uma notificação para uma entidade específica"
  def notify(entity_id, payload) do
    DynamicSupervisor.start_child(NotificationManager, {NotificationWorker, {entity_id, payload}})
  end
end

defmodule NotificationWorker do
  use GenServer, restart: :transient

  @max_retries 3
  @initial_backoff 1000

  def start_link({entity_id, payload}) do
    GenServer.start_link(__MODULE__, %{entity_id: entity_id, payload: payload, retries: 0})
  end

  @impl true
  def init(state) do
    send(self(), :process)
    {:ok, state}
  end

  @impl true
  def handle_info(:process, state) do
    case perform_notification(state.entity_id, state.payload) do
      :ok ->
        {:stop, :normal, state}

      {:error, :temporary} when state.retries < @max_retries ->
        # Implementa exponential backoff
        backoff = @initial_backoff * :math.pow(2, state.retries) |> round()
        Process.send_after(self(), :process, backoff)
        {:noreply, %{state | retries: state.retries + 1}}

      {:error, reason} ->
        # Falha persistente ou excedeu retries
        {:stop, reason, state}
    end
  end

  # Simulação de envio de notificação
  defp perform_notification(id, payload) do
    # Lógica de integração externa aqui
    if :rand.uniform(10) > 7 do
      {:error, :temporary}
    else
      IO.puts("Notificação enviada para #{id}: #{inspect(payload)}")
      :ok
    end
  end
end
