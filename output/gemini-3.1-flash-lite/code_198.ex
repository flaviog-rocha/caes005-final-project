# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de transações financeiras, atendendo somente aos seguintes requisitos:
#
# 1. Implementar novas tentativas após falhas temporárias.
# 2. Validar os dados recebidos antes de realizar operações.
# 3. Detectar encerramentos inesperados de processos.
# 4. Diferenciar erros de validação de falhas operacionais.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule FinancialSystem do
  @moduledoc """
  Sistema de gerenciamento de transações com resiliência, validação e supervisão.
  """

  defmodule TransactionError do
    defexception [:message, :type]
  end

  @doc """
  Processa uma transação com retentativas e isolamento de falhas.
  """
  def process_transaction(transaction) do
    # 1. Validação de dados (Requisito 2)
    with {:ok, valid_data} <- validate(transaction) do
      # 2. Execução resiliente (Requisito 1)
      execute_with_retry(valid_data, 3)
    else
      {:error, reason} -> {:error, %TransactionError{message: reason, type: :validation}}
    end
  end

  # Requisito 2: Validação
  defp validate(%{amount: amount, id: _}) when is_number(amount) and amount > 0, do: {:ok, transaction}
  defp validate(_), do: {:error, "Dados inválidos: valor deve ser positivo"}

  # Requisito 1: Implementação de retentativas (Retry Logic)
  defp execute_with_retry(data, retries) do
    try do
      # Simulação de lógica de negócio propensa a falhas temporárias
      perform_operation(data)
    rescue
      e ->
        if retries > 0 do
          :timer.sleep(100)
          execute_with_retry(data, retries - 1)
        else
          {:error, %TransactionError{message: "Falha após retentativas: #{inspect(e)}", type: :operational}}
        end
    end
  end

  defp perform_operation(_data), do: :ok

  # Requisito 3 e 4: Monitoramento e Diferenciação de Erros
  defmodule TransactionSupervisor do
    use Supervisor

    def start_link(_), do: Supervisor.start_link(__MODULE__, [], name: __MODULE__)

    @impl true
    def init(_) do
      children = [
        {Task.Supervisor, name: TransactionTaskSupervisor}
      ]
      Supervisor.init(children, strategy: :one_for_one)
    end

    @doc """
    Executa uma transação monitorada para detectar encerramentos inesperados.
    """
    def run_monitored(transaction) do
      Task.Supervisor.async_nolink(TransactionTaskSupervisor, fn ->
        FinancialSystem.process_transaction(transaction)
      end)
      |> Task.yield(5000) # Timeout de segurança
      |> handle_result()
    end

    defp handle_result({:ok, result}), do: result
    defp handle_result(nil), do: {:error, "Processo excedeu tempo limite"}
    defp handle_result({:exit, reason}), do: {:error, "Encerramento inesperado: #{inspect(reason)}"}
  end
end
