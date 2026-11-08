# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de transações financeiras, atendendo somente aos seguintes requisitos:
#
# 1. Implementar novas tentativas após falhas temporárias.
# 2. Validar os dados recebidos antes de realizar operações.
# 3. Detectar encerramentos inesperados de processos.
# 4. Diferenciar erros de validação de falhas operacionais.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule FinancialTransactions do
  @moduledoc """
  Valida e executa transações financeiras com novas tentativas para falhas
  temporárias e monitoramento do processo que executa a operação.
  """

  @type transaction :: %{
          from_account: binary(),
          to_account: binary(),
          amount: number(),
          currency: binary()
        }

  @doc """
  Valida os dados e executa `operation` em um processo monitorado.

  A operação deve retornar `{:ok, result}`, `{:error, reason}` ou
  `{:error, {:temporary, reason}}`. `max_retries` indica o número de novas
  tentativas além da tentativa inicial; `retry_delay` é o intervalo em
  milissegundos entre elas.
  """
  def transfer(attrs, operation, opts \\ []) do
    with {:ok, transaction} <- validate_transaction(attrs),
         :ok <- validate_operation(operation),
         {:ok, options} <- validate_options(opts) do
      execute(transaction, operation, options, 0)
    end
  end

  defp validate_transaction(attrs) when is_map(attrs) do
    errors =
      %{}
      |> add_error(:from_account, valid_account?(Map.get(attrs, :from_account)))
      |> add_error(:to_account, valid_account?(Map.get(attrs, :to_account)))
      |> add_error(:amount, valid_amount?(Map.get(attrs, :amount)))
      |> add_error(:currency, valid_currency?(Map.get(attrs, :currency)))
      |> add_error(
        :accounts,
        Map.get(attrs, :from_account) != Map.get(attrs, :to_account)
      )

    if map_size(errors) == 0 do
      {:ok,
       %{
         from_account: Map.get(attrs, :from_account),
         to_account: Map.get(attrs, :to_account),
         amount: Map.get(attrs, :amount),
         currency: Map.get(attrs, :currency)
       }}
    else
      {:error, {:validation, errors}}
    end
  end

  defp validate_transaction(_attrs) do
    {:error, {:validation, %{transaction: "deve ser um mapa"}}}
  end

  defp add_error(errors, _field, true), do: errors
  defp add_error(errors, field, false), do: Map.put(errors, field, "valor inválido")

  defp valid_account?(value) do
    is_binary(value) and String.trim(value) != ""
  end

  defp valid_amount?(value) do
    is_number(value) and value > 0
  end

  defp valid_currency?(value) do
    is_binary(value) and Regex.match?(~r/\A[A-Z]{3}\z/, value)
  end

  defp validate_operation(operation) when is_function(operation, 1), do: :ok

  defp validate_operation(_operation) do
    {:error, {:validation, %{operation: "deve ser uma função de aridade 1"}}}
  end

  defp validate_options(opts) when is_list(opts) do
    if Keyword.keyword?(opts) do
      max_retries = Keyword.get(opts, :max_retries, 2)
      retry_delay = Keyword.get(opts, :retry_delay, 100)

      errors =
        %{}
        |> add_error(:max_retries, is_integer(max_retries) and max_retries >= 0)
        |> add_error(:retry_delay, is_integer(retry_delay) and retry_delay >= 0)

      if map_size(errors) == 0 do
        {:ok, %{max_retries: max_retries, retry_delay: retry_delay}}
      else
        {:error, {:validation, errors}}
      end
    else
      {:error, {:validation, %{options: "deve ser uma lista de palavras-chave"}}}
    end
  end

  defp validate_options(_opts) do
    {:error, {:validation, %{options: "deve ser uma lista de palavras-chave"}}}
  end

  defp execute(transaction, operation, options, retries_used) do
    case run_operation(transaction, operation) do
      {:ok, result} ->
        {:ok, result}

      {:retry, reason} when retries_used < options.max_retries ->
        Process.sleep(options.retry_delay)
        execute(transaction, operation, options, retries_used + 1)

      {:retry, reason} ->
        {:error, {:operational, {:retries_exhausted, reason}}}

      {:error, reason} ->
        {:error, {:operational, reason}}
    end
  end

  defp run_operation(transaction, operation) do
    parent = self()
    ref = make_ref()

    {pid, monitor_ref} =
      spawn_monitor(fn ->
        outcome = invoke(operation, transaction)
        send(parent, {ref, outcome})
      end)

    receive do
      {^ref, outcome} ->
        Process.demonitor(monitor_ref, [:flush])
        normalize_outcome(outcome)

      {:DOWN, ^monitor_ref, :process, ^pid, reason} ->
        {:error, {:process_exit, reason}}
    end
  end

  defp invoke(operation, transaction) do
    try do
      {:returned, operation.(transaction)}
    rescue
      exception ->
        {:exception, exception.__struct__, Exception.message(exception)}
    catch
      kind, reason ->
        {:caught, kind, reason}
    end
  end

  defp normalize_outcome({:returned, {:ok, result}}), do: {:ok, result}
  defp normalize_outcome({:returned, {:error, {:temporary, reason}}}), do: {:retry, reason}
  defp normalize_outcome({:returned, {:error, reason}}), do: {:error, reason}

  defp normalize_outcome({:returned, other}) do
    {:error, {:invalid_operation_response, other}}
  end

  defp normalize_outcome({:exception, module, message}) do
    {:error, {:operation_exception, module, message}}
  end

  defp normalize_outcome({:caught, kind, reason}) do
    {:error, {:operation_failure, kind, reason}}
  end
end
