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
  e tratamento de erros por tipagem (Validação vs Operacional).
  """

  # --- Definição de Erros ---
  defmodule ValidationError, do: defexception [:message]
  defmodule OperationalError, do: defexception [:message]

  # --- Domínio ---
  defmodule Notification do
    defstruct [:id, :user_id, :message, :status]
  end

  # --- Supervisor/Gerenciador de Processos ---
  use GenServer

  def start_link(_), do: GenServer.start_link(__MODULE__, %{}, name: __MODULE__)

  @impl true
  def init(_) do
    Process.flag(:trap_exit, true)
    {:ok, %{db: %{}}}
  end

  # --- API Pública ---

  @doc "Dispara notificação com validação de domínio"
  def notify(user_id, message) when is_binary(message) and byte_size(message) > 0 do
    GenServer.call(__MODULE__, {:send, user_id, message})
  end
  def notify(_, _), do: {:error, %ValidationError{message: "Mensagem inválida"}}

  @doc "Simula um worker de envio"
  def spawn_worker(task) do
    spawn_link(fn ->
      if task == :fail, do: raise("Falha crítica no sistema!")
      :timer.sleep(1000)
      IO.puts("Notificação enviada com sucesso.")
    end)
  end

  # --- Callbacks ---

  @impl true
  def handle_call({:send, user_id, message}, _from, state) do
    # Simulação de persistência (DB Fictício)
    notification = %Notification{id: System.unique_integer([:positive]), user_id: user_id, message: message, status: :sent}
    
    try do
      # Simulação de IO com banco de dados
      new_db = Map.put(state.db, notification.id, notification)
      {:reply, {:ok, notification}, %{state | db: new_db}}
    rescue
      _ -> {:reply, {:error, %OperationalError{message: "Falha de conexão com DB"}}, state}
    end
  end

  @impl true
  def handle_info({:EXIT, pid, reason}, state) do
    # Detecção de encerramentos inesperados (Requisito 4)
    IO.warn("Processo #{inspect(pid)} encerrou inesperadamente: #{inspect(reason)}")
    {:noreply, state}
  end

  # --- Exemplo de Uso ---
  def run_demo do
    {:ok, _} = start_link(nil)

    # 1. Sucesso
    notify(1, "Olá, Mundo!")

    # 2. Erro de Validação
    IO.inspect(notify(1, ""), label: "Validação")

    # 3. Detecção de falha de processo
    pid = spawn_worker(:fail)
    Process.sleep(500)
    
    :ok
  end
end
