# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de gerenciamento de notificações, atendendo somente aos seguintes requisitos:
#
# 1. Gerar notificações quando determinadas condições forem atingidas.
# 2. Quando fizer sentido para o domínio, considerar a comunicação com um banco de dados fictício e, opcionalmente, o uso de Ecto.Migration, sem tornar isso obrigatório.
# 3. Diferenciar erros de validação de falhas operacionais.
# 4. Detectar encerramentos inesperados de processos.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule NotificationSystem do
  @moduledoc """
  Sistema de notificações utilizando GenServer para monitoramento de processos
  e tratamento de erros com tipos distintos.
  """

  use GenServer

  # --- Tipos e Estruturas ---

  defmodule Notification do
    defstruct [:id, :user_id, :message, :status]
  end

  # Representação de Erros
  defmodule ValidationError, do: defexception [:message]
  defmodule OperationalError, do: defexception [:message]

  # --- API do Servidor ---

  def start_link(_), do: GenServer.start_link(__MODULE__, %{}, name: __MODULE__)

  def notify(user_id, message) when is_binary(message) do
    GenServer.call(__MODULE__, {:notify, user_id, message})
  end

  def notify(_, _), do: raise(ValidationError, "Mensagem deve ser string")

  # --- Callbacks do GenServer ---

  @impl true
  def init(_), do: {:ok, %{db: %{}}}

  @impl true
  def handle_call({:notify, user_id, message}, _from, state) do
    # Simulação de lógica de negócio e validação
    case validate_notification(user_id, message) do
      {:ok, data} ->
        # Simulação de persistência (Banco de dados fictício)
        new_state = put_in(state, [:db, data.id], data)
        {:reply, {:ok, data}, new_state}

      {:error, %ValidationError{} = err} ->
        {:reply, {:error, err}, state}

      {:error, %OperationalError{} = err} ->
        {:reply, {:error, err}, state}
    end
  end

  # --- Lógica de Domínio ---

  defp validate_notification(user_id, message) do
    cond do
      is_nil(user_id) -> 
        {:error, %ValidationError{message: "User ID é obrigatório"}}
      String.length(message) > 100 -> 
        {:error, %OperationalError{message: "Limite de caracteres excedido (DB IO limit)"}}
      true -> 
        {:ok, %Notification{id: :erlang.unique_integer([:positive]), user_id: user_id, message: message}}
    end
  end

  # --- Monitoramento de Processos ---

  def spawn_worker(task_name) do
    # Monitora o processo para detectar encerramentos inesperados
    spawn_monitor(fn ->
      Process.sleep(1000)
      if task_name == :crash, do: raise("Erro crítico no worker!")
    end)
  end

  # Loop de monitoramento (pode ser integrado ao handle_info)
  def handle_info({:DOWN, _ref, :process, pid, reason}, state) do
    IO.puts("Monitor: Processo #{inspect(pid)} encerrou inesperadamente: #{inspect(reason)}")
    {:noreply, state}
  end
end

# --- Exemplo de Uso ---
# {:ok, _pid} = NotificationSystem.start_link([])
# NotificationSystem.notify(1, "Olá, Mundo!")
# {pid, ref} = NotificationSystem.spawn_worker(:crash)
