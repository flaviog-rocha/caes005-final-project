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
  Sistema de notificações utilizando OTP para resiliência e processamento independente.
  """

  use Supervisor

  @doc "Inicia o supervisor raiz do sistema."
  def start_link(_opts) do
    Supervisor.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  @impl true
  def init(:ok) do
    # O DynamicSupervisor permite gerenciar processos de notificação independentes
    children = [
      {DynamicSupervisor, name: NotificationSystem.WorkerSupervisor, strategy: :one_for_one}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  @doc "Dispara o envio de uma notificação para uma entidade específica."
  def notify(entity_id, payload) do
    DynamicSupervisor.start_child(
      NotificationSystem.WorkerSupervisor,
      {NotificationSystem.Worker, {entity_id, payload}}
    )
  end
end

defmodule NotificationSystem.Worker do
  use GenServer, restart: :temporary

  @max_retries 3

  def start_link({entity_id, payload}) do
    GenServer.start_link(__MODULE__, {entity_id, payload})
  end

  @impl true
  def init(state) do
    send(self(), :process)
    {:ok, state}
  end

  @impl true
  def handle_info(:process, {entity_id, payload} = state) do
    case perform_delivery(entity_id, payload) do
      :ok ->
        {:stop, :normal, state}

      {:error, :transient} ->
        # Implementa novas tentativas com retentativa simples
        retry(state, 1)
    end
  end

  # Simulação de lógica de envio
  defp perform_delivery(entity_id, payload) do
    IO.puts("Enviando para #{entity_id}: #{payload}")
    # Simula falha temporária aleatória
    if :rand.uniform() > 0.7, do: {:error, :transient}, else: :ok
  end

  defp retry(state, attempt) when attempt <= @max_retries do
    Process.sleep(1000 * attempt)
    send(self(), :process)
    {:noreply, state}
  end

  defp retry(_state, _attempt) do
    IO.puts("Falha persistente após tentativas.")
    {:stop, :normal, nil}
  end
end
