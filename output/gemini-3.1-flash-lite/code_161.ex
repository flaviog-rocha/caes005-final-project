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

defmodule TaskQueue.Manager do
  @moduledoc """
  Gerenciador de tarefas utilizando GenServer para concorrência e estado.
  Separa validação (erros de input) de falhas operacionais (timeout/db).
  """
  use GenServer

  # --- Tipos e Definições ---
  defstruct tasks: %{}, subscribers: []

  # --- API Pública ---

  def start_link(_opts), do: GenServer.start_link(__MODULE__, %__MODULE__{}, name: __MODULE__)

  def enqueue(id, payload) when is_map(payload) do
    # Validação imediata: Erros de domínio
    if Map.get(payload, :data) == nil, 
      do: {:error, :validation_failed, "Payload requer campo :data"},
      else: GenServer.call(__MODULE__, {:enqueue, id, payload})
  end

  def get_state, do: GenServer.call(__MODULE__, :get_state)

  # --- Callbacks do GenServer ---

  @impl true
  def init(state), do: {:ok, state}

  @impl true
  def handle_call({:enqueue, id, payload}, _from, state) do
    # Simulação de persistência (DB Fictício)
    case save_to_db(id, payload) do
      :ok ->
        new_state = %{state | tasks: Map.put(state.tasks, id, %{status: :pending, data: payload})}
        {:reply, {:ok, :queued}, new_state}
      
      {:error, reason} ->
        # Falha operacional (ex: DB indisponível)
        {:reply, {:error, :operational_failure, reason}, state}
    end
  end

  @impl true
  def handle_call(:get_state, _from, state), do: {:reply, state.tasks, state}

  @impl true
  def handle_cast({:process, id}, state) do
    # Processamento independente por ID
    task = Map.get(state.tasks, id)
    
    cond do
      is_nil(task) -> {:noreply, state}
      true ->
        # Simulação de notificação de transição de estado
        notify_subscribers(id, :processing)
        {:noreply, %{state | tasks: Map.put(state.tasks, id, %{task | status: :processing})}}
    end
  end

  # --- Lógica Interna e Auxiliares ---

  defp save_to_db(id, _payload) do
    # Simulação de interface de banco (poderia usar Ecto aqui)
    if rem(System.unique_integer([:positive]), 10) == 0, 
      do: {:error, "DB Timeout"}, 
      else: :ok
  end

  defp notify_subscribers(id, status) do
    IO.puts("[Notificação] Tarefa #{id} alterou estado para #{status}")
  end
end

# --- Exemplo de Uso ---
# {:ok, _pid} = TaskQueue.Manager.start_link([])
# TaskQueue.Manager.enqueue(:job_1, %{data: "processar_pagamento"})
# TaskQueue.Manager.get_state()
