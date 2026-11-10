# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de uma fila de tarefas, atendendo somente aos seguintes requisitos:
#
# 1. Diferenciar erros de validação de falhas operacionais.
# 2. Quando fizer sentido para o domínio, considerar a comunicação com um banco de dados fictício e, opcionalmente, o uso de Ecto.Migration, sem tornar isso obrigatório.
# 3. Permitir que diferentes entidades sejam processadas independentemente.
# 4. Permitir consultar o estado atual das operações em andamento.
# 5. Gerar notificações quando determinadas condições forem atingidas.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule TaskManager do
  @moduledoc """
  Gerenciador de filas de tarefas funcional utilizando GenServer para estado
  e separação clara entre erros de domínio (validação) e falhas (operacionais).
  """

  use GenServer

  # --- Tipos de Erro ---
  defmodule ValidationError, do: defexception [:message]
  defmodule OperationalFailure, do: defexception [:message]

  # --- API Pública ---

  def start_link(_), do: GenServer.start_link(__MODULE__, %{}, name: __MODULE__)

  def enqueue(entity_id, task_data) do
    GenServer.call(__MODULE__, {:enqueue, entity_id, task_data})
  end

  def get_state, do: GenServer.call(__MODULE__, :get_state)

  # --- Callbacks do GenServer ---

  @impl true
  def init(_), do: {:ok, %{tasks: %{}, history: []}}

  @impl true
  def handle_call({:enqueue, entity_id, task_data}, _from, state) do
    case validate_task(task_data) do
      {:ok, data} ->
        # Simulação de persistência (DB fictício)
        log_to_db(entity_id, :queued)
        
        new_state = put_in(state, [:tasks, entity_id], %{data: data, status: :processing})
        
        # Execução assíncrona para simular processamento independente
        spawn(fn -> process_task(entity_id, data) end)
        
        {:reply, {:ok, :queued}, new_state}

      {:error, reason} ->
        {:reply, {:error, %ValidationError{message: reason}}, state}
    end
  end

  @impl true
  def handle_call(:get_state, _from, state), do: {:reply, state, state}

  # --- Lógica de Domínio ---

  defp validate_task(%{id: _}), do: {:ok, "valid"}
  defp validate_task(_), do: {:error, "Dados inválidos: ID da entidade ausente"}

  defp process_task(entity_id, _data) do
    # Simula uma falha operacional aleatória (ex: timeout de rede)
    if :rand.uniform() > 0.8 do
      notify(:error, "Falha crítica ao processar #{entity_id}")
      exit(:operational_failure)
    else
      notify(:success, "Entidade #{entity_id} processada com sucesso")
    end
  end

  defp notify(type, message) do
    IO.puts("[NOTIFICAÇÃO - #{String.upcase(Atom.to_string(type))}] #{message}")
  end

  defp log_to_db(entity_id, status) do
    # Placeholder para Ecto.Repo.insert/update
    IO.puts("DB: Registro de #{entity_id} atualizado para #{status}")
  end
end

# --- Exemplo de uso ---
# {:ok, _} = TaskManager.start_link([])
# TaskManager.enqueue("user_123", %{id: 123})
# TaskManager.enqueue("invalid", %{}) # Retorna ValidationError
# IO.inspect(TaskManager.get_state())
