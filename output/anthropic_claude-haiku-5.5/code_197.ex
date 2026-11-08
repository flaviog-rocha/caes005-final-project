# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de transações financeiras, atendendo somente aos seguintes requisitos:
#
# 1. Implementar novas tentativas após falhas temporárias.
# 2. Validar os dados recebidos antes de realizar operações.
# 3. Detectar encerramentos inesperados de processos.
# 4. Diferenciar erros de validação de falhas operacionais.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule TransactionManager do
  @moduledoc """
  Gerenciamento de transações financeiras com:

    1. Novas tentativas após falhas temporárias (backoff exponencial com jitter).
    2. Validação dos dados antes de qualquer operação.
    3. Detecção de encerramento inesperado do processo executor (via monitor).
    4. Distinção entre erros de validação e falhas operacionais.

  O executor é uma função de aridade 1 que recebe a transação validada e deve
  retornar uma das formas:

    * `{:ok, result}`               - sucesso
    * `{:error, :transient, reason}` - falha temporária (será reexecutada)
    * `{:error, :permanent, reason}` - falha definitiva (não será reexecutada)

  Retornos do `process/3`:

    * `{:ok, result}`
    * `{:error, {:validation, [reason]}}`
    * `{:error, {:operational, :permanent, %{reason: _, attempts: _}}}`
    * `{:error, {:operational, :exhausted, %{last_kind: _, reason: _, attempts: _}}}`

  Falhas operacionais `:transient`, `:crashed` (encerramento inesperado) e
  `:timeout` são reexecutadas até `max_attempts`.
  """

  @default_opts [
    max_attempts: 3,
    base_backoff_ms: 100,
    max_backoff_ms: 2_000,
    timeout_ms: 5_000
  ]

  # Tipos de falha operacional considerados temporários
  @retryable [:transient, :crashed, :timeout]

  @doc """
  Valida e executa a transação aplicando a política de novas tentativas.

  Opções: `:max_attempts`, `:base_backoff_ms`, `:max_backoff_ms`, `:timeout_ms`.
  """
  def process(tx, executor, opts \\ []) when is_function(executor, 1) do
    cfg = @default_opts |> Keyword.merge(opts) |> Map.new()

    with {:ok, valid_tx} <- validate(tx) do
      attempt(valid_tx, executor, 1, cfg)
    end
  end

  @doc """
  Valida os dados da transação. Retorna `{:ok, tx}` ou
  `{:error, {:validation, reasons}}` listando todas as regras violadas.
  """
  def validate(tx) when is_map(tx) do
    case for({reason, rule} <- rules(), not rule.(tx), do: reason) do
      [] -> {:ok, tx}
      reasons -> {:error, {:validation, reasons}}
    end
  end

  def validate(_), do: {:error, {:validation, [:not_a_map]}}

  # ---------------------------------------------------------------------------
  # Regras de validação (cada regra recebe a transação inteira)
  # ---------------------------------------------------------------------------

  defp rules do
    [
      invalid_id: &non_empty_string?(Map.get(&1, :id)),
      invalid_amount: &(is_integer(Map.get(&1, :amount)) and Map.get(&1, :amount) > 0),
      invalid_currency: &currency?(Map.get(&1, :currency)),
      invalid_source: &non_empty_string?(Map.get(&1, :from)),
      invalid_destination: &non_empty_string?(Map.get(&1, :to)),
      same_account_transfer: &(Map.get(&1, :from) != Map.get(&1, :to))
    ]
  end

  defp non_empty_string?(value), do: is_binary(value) and byte_size(String.trim(value)) > 0

  defp currency?(value), do: is_binary(value) and Regex.match?(~r/^[A-Z]{3}$/, value)

  # ---------------------------------------------------------------------------
  # Ciclo de tentativas
  # ---------------------------------------------------------------------------

  defp attempt(tx, executor, n, cfg) do
    case run_isolated(tx, executor, cfg.timeout_ms) do
      {:ok, result} ->
        {:ok, result}

      {:permanent, reason} ->
        {:error, {:operational, :permanent, %{reason: reason, attempts: n}}}

      {kind, _reason} = failure when kind in @retryable and n < cfg.max_attempts ->
        {_, reason} = failure
        _ = reason
        Process.sleep(backoff(n, cfg))
        attempt(tx, executor, n + 1, cfg)

      {kind, reason} ->
        {:error,
         {:operational, :exhausted, %{last_kind: kind, reason: reason, attempts: n}}}
    end
  end

  # Backoff exponencial limitado, com jitter de até 50% para evitar rajadas sincronizadas
  defp backoff(n, %{base_backoff_ms: base, max_backoff_ms: max_ms}) do
    delay = min(base * Integer.pow(2, n - 1), max_ms)
    delay + :rand.uniform(max(div(delay, 2), 1)) - 1
  end

  # ---------------------------------------------------------------------------
  # Execução isolada e monitorada
  # ---------------------------------------------------------------------------

  # Executa o executor em um processo separado e monitorado. Qualquer
  # encerramento inesperado (crash, exit, término sem resposta) é detectado
  # via DOWN; timeouts resultam em kill do processo.
  defp run_isolated(tx, executor, timeout) do
    parent = self()
    ref = make_ref()

    {pid, mref} =
      spawn_monitor(fn ->
        send(parent, {ref, executor.(tx)})
      end)

    receive do
      {^ref, response} ->
        Process.demonitor(mref, [:flush])
        normalize(response)

      {:DOWN, ^mref, :process, ^pid, reason} ->
        {:crashed, {:unexpected_exit, reason}}
    after
      timeout ->
        Process.exit(pid, :kill)
        Process.demonitor(mref, [:flush])
        flush_result(ref)
        {:timeout, :no_response_within_timeout}
    end
  end

  defp normalize({:ok, _} = ok), do: ok
  defp normalize({:error, :transient, reason}), do: {:transient, reason}
  defp normalize({:error, :permanent, reason}), do: {:permanent, reason}
  # Retorno fora do contrato é tratado como falha definitiva (erro de programação)
  defp normalize(other), do: {:permanent, {:invalid_executor_response, other}}

  # Remove resposta tardia de uma tentativa já abandonada por timeout
  defp flush_result(ref) do
    receive do
      {^ref, _} -> :ok
    after
      0 -> :ok
    end
  end
end
