# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de transações financeiras, atendendo somente aos seguintes requisitos:
#
# 1. Implementar novas tentativas após falhas temporárias.
# 2. Validar os dados recebidos antes de realizar operações.
# 3. Detectar encerramentos inesperados de processos.
# 4. Diferenciar erros de validação de falhas operacionais.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule FinancialSystem.Transaction do
  @moduledoc """
  Gerenciador de transações com resiliência, validação e monitoramento.
  """

  defstruct [:id, :amount, :account_id]

  @type t :: %__MODULE__{id: String.t(), amount: float(), account_id: String.t()}
  @type error :: {:validation_error, String.t()} | {:operational_error, String.t()}

  # 1. Validação de dados
  def validate(%{amount: amount, account_id: id}) when amount > 0 and is_binary(id) do
    {:ok, %__MODULE__{id: System.unique_integer([:positive]) |> to_string(), amount: amount, account_id: id}}
  end
  def validate(_), do: {:validation_error, "Dados de transação inválidos"}

  # 2. Execução com Retentativas (Implementação funcional)
  def execute_with_retry(tx, retries \\ 3) do
    case perform_transfer(tx) do
      :ok -> 
        :ok
      {:error, :transient} when retries > 0 ->
        Process.sleep(500)
        execute_with_retry(tx, retries - 1)
      {:error, reason} -> 
        {:operational_error, reason}
    end
  end

  # Simulação de lógica de negócio
  defp perform_transfer(_tx) do
    if :rand.uniform() > 0.7, do: :ok, else: {:error, :transient}
  end

  # 3. Supervisão e Detecção de Encerramento Inesperado
  def spawn_transaction_worker(data) do
    parent = self()
    
    spawn_monitor(fn ->
      case validate(data) do
        {:ok, tx} ->
          result = execute_with_retry(tx)
          send(parent, {:result, tx.id, result})
        error ->
          send(parent, {:result, nil, error})
      end
    end)
  end

  # Loop de recepção que gerencia mensagens de sucesso e falhas de processo (DOWN)
  def monitor_loop do
    receive do
      {:result, id, result} ->
        IO.puts("Transação #{inspect(id)} finalizada: #{inspect(result)}")
        monitor_loop()

      {:DOWN, _ref, :process, _pid, reason} ->
        IO.puts("Alerta: Processo de transação terminou inesperadamente: #{inspect(reason)}")
        monitor_loop()
    end
  end
end
