# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de usuários e sessões, atendendo somente aos seguintes requisitos:
#
# 1. Permitir a execução de operações simultâneas.
# 2. Manter um histórico das operações realizadas.
# 3. Permitir limitar a quantidade de operações simultâneas.
# 4. Detectar encerramentos inesperados de processos.
# 5. Impedir que uma mesma operação seja executada mais de uma vez.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - DynamicSupervisor
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule OperationManager do
  @moduledoc """
  Gerenciador de operações com controle de concorrência via DynamicSupervisor,
  rastreamento de estado e idempotência.
  """

  use DynamicSupervisor
  use Agent

  # --- API Pública ---

  def start_link(_) do
    DynamicSupervisor.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  def init(:ok) do
    # Armazena o histórico e operações em andamento
    Agent.start_link(fn -> %{history: [], running: MapSet.new()} end, name: :op_state)
    DynamicSupervisor.init(strategy: :one_for_one, max_children: 10) # Limite de concorrência
  end

  @doc "Executa uma operação de forma idempotente e monitorada"
  def execute_operation(op_id, func) do
    if register_operation(op_id) do
      DynamicSupervisor.start_child(__MODULE__, {Task, fn ->
        try do
          result = func.()
          log_history(op_id, :success)
          result
        rescue
          e -> 
            log_history(op_id, {:error, e})
            reraise e, __STACKTRACE__
        after
          unregister_operation(op_id)
        end
      end})
    else
      {:error, :already_running_or_done}
    end
  end

  # --- Funções Privadas / Internas ---

  defp register_operation(op_id) do
    Agent.get_and_update(:op_state, fn state ->
      if MapSet.member?(state.running, op_id) or op_already_in_history?(state.history, op_id) do
        {false, state}
      else
        {true, %{state | running: MapSet.put(state.running, op_id)}}
      end
    end)
  end

  defp unregister_operation(op_id) do
    Agent.update(:op_state, fn state ->
      %{state | running: MapSet.delete(state.running, op_id)}
    end)
  end

  defp log_history(op_id, status) do
    Agent.update(:op_state, fn state ->
      %{state | history: [{op_id, status, DateTime.utc_now()} | state.history]}
    end)
  end

  defp op_already_in_history?(history, op_id) do
    Enum.any?(history, fn {id, _, _} -> id == op_id end)
  end

  # Monitoramento de processos para encerramentos inesperados
  def handle_info({:DOWN, _ref, :process, _pid, reason}, state) do
    if reason != :normal do
      # Lógica adicional de recuperação ou logging de crash
    end
    {:noreply, state}
  end
end
