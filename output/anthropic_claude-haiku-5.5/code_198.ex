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
  Gerenciamento de transações financeiras com validação prévia, novas tentativas
  para falhas temporárias, detecção de encerramentos inesperados de processos e
  distinção entre erros de validação e falhas operacionais.

  Retornos possíveis de `execute/3`:

    * `{:ok, result}` - operação concluída com sucesso.
    * `{:error, {:validation, [mensagem]}}` - dados inválidos; nenhuma operação foi executada.
    * `{:error, {:operational, motivo, tentativas}}` - falha durante a operação, após
      `tentativas` execuções. `motivo` pode ser `{:exhausted, causa}` quando as
      novas tentativas se esgotaram, ou o motivo de uma falha permanente.

  O executor deve retornar:

    * `{:ok, result}` - sucesso.
    * `{:error, {:transient, motivo}}` - falha temporária, elegível a nova tentativa.
    * `{:error, {:permanent, motivo}}` - falha definitiva, sem nova tentativa.

  Falhas transitórias do próprio mecanismo (processo encerrado inesperadamente
  ou timeout) são classificadas automaticamente como `:transient`.

  ## Exemplo

      executor = fn %TransactionManager.Transaction{} = tx ->
        case MyBank.transfer(tx) do
          {:ok, receipt} -> {:ok, receipt}
          {:error, :timeout} -> {:error, {:transient, :timeout}}
          {:error, :insufficient_funds} -> {:error, {:permanent, :insufficient_funds}}
        end
      end

      TransactionManager.execute(
        %{id: "tx-1", type: :transfer, amount: 1_500, currency: "BRL",
          from: "acc-1", to: "acc-2"},
        executor,
        max_attempts: 5, base_backoff_ms: 200, timeout_ms: 3_000
      )
  """

  @types [:transfer, :deposit, :withdrawal]

  @defaults [
    max_attempts: 3,
    base_backoff_ms: 100,
    max_backoff_ms: 2_000,
    timeout_ms: 5_000
  ]

  defmodule Transaction do
    @moduledoc "Estrutura de uma transação já validada."
    @enforce_keys [:id, :type, :amount, :currency, :from, :to]
    defstruct [:id, :type, :amount, :currency, :from, :to]

    @type t :: %__MODULE__{
            id: String.t(),
            type: :transfer | :deposit | :withdrawal,
            amount: pos_integer(),
            currency: String.t(),
            from: String.t(),
            to: String.t()
          }
  end

  @type executor_result ::
          {:ok, term()}
          | {:error, {:transient, term()}}
          | {:error, {:permanent, term()}}

  @type error_result ::
          {:error, {:validation, [String.t()]}}
          | {:error, {:operational, term(), pos_integer()}}

  @doc """
  Valida os dados e, se válidos, executa a operação com novas tentativas.

  Opções:
    * `:max_attempts` - número máximo de execuções (padrão #{@defaults[:max_attempts]}).
    * `:base_backoff_ms` - base do backoff exponencial (padrão #{@defaults[:base_backoff_ms]}).
    * `:max_backoff_ms` - teto do backoff (padrão #{@defaults[:max_backoff_ms]}).
    * `:timeout_ms` - tempo máximo por tentativa (padrão #{@defaults[:timeout_ms]}).
  """
  @spec execute(map(), (Transaction.t() -> executor_result()), keyword()) ::
          {:ok, term()} | error_result()
  def execute(params, executor, opts \\ [])
      when is_map(params) and is_function(executor, 1) do
    # Validação ocorre antes de qualquer operação; erros retornam como :validation.
    with {:ok, tx} <- validate(params) do
      cfg = Keyword.merge(@defaults, opts) |> Map.new()
      attempt(tx, executor, cfg, 1)
    end
  end

  @doc """
  Valida um mapa de parâmetros e retorna uma `Transaction` ou a lista de erros de validação.
  Função pura, sem efeitos colaterais.
  """
  @spec validate(term()) :: {:ok, Transaction.t()} | {:error, {:validation, [String.t()]}}
  def validate(params) when is_map(params) do
    errors = field_errors(params) ++ cross_errors(params)

    case errors do
      [] -> {:ok, build(params)}
      _ -> {:error, {:validation, errors}}
    end
  end

  def validate(_params), do: {:error, {:validation, ["payload deve ser um mapa"]}}

  # ---------------------------------------------------------------------------
  # Validação
  # ---------------------------------------------------------------------------

  # Regras por campo: {campo, predicado, mensagem de erro}.
  # Campos ausentes resultam em nil, que falha em todos os predicados.
  defp rules do
    [
      {:id, &(is_binary(&1) and byte_size(&1) > 0), "deve ser string não vazia"},
      {:type, &(&1 in @types), "deve ser um de #{inspect(@types)}"},
      {:amount, &(is_integer(&1) and &1 > 0), "deve ser inteiro positivo em unidades mínimas"},
      {:currency, &(is_binary(&1) and Regex.match?(~r/\A[A-Z]{3}\z/, &1)),
       "deve ser código ISO 4217 de 3 letras maiúsculas"},
      {:from, &valid_account?/1, "deve ser identificador de conta válido (1..64 bytes)"},
      {:to, &valid_account?/1, "deve ser identificador de conta válido (1..64 bytes)"}
    ]
  end

  defp field_errors(params) do
    for {field, valid?, message} <- rules(),
        not valid?.(Map.get(params, field)),
        do: "#{field} #{message}"
  end

  # Regras que envolvem mais de um campo.
  defp cross_errors(%{type: :transfer, from: a, to: b}) when is_binary(a) and a == b,
    do: ["from e to não podem ser iguais em transferência"]

  defp cross_errors(_), do: []

  defp valid_account?(value), do: is_binary(value) and byte_size(value) in 1..64

  defp build(params) do
    struct!(Transaction, Map.take(params, [:id, :type, :amount, :currency, :from, :to]))
  end

  # ---------------------------------------------------------------------------
  # Execução com novas tentativas
  # ---------------------------------------------------------------------------

  defp attempt(tx, executor, cfg, n) do
    case run_isolated(tx, executor, cfg.timeout_ms) do
      {:ok, result} ->
        {:ok, result}

      # Falha permanente: não há nova tentativa.
      {:error, {:permanent, reason}} ->
        {:error, {:operational, reason, n}}

      # Falha temporária com tentativas restantes: backoff e nova execução.
      {:error, {:transient, _reason}} when n < cfg.max_attempts ->
        Process.sleep(backoff(cfg, n))
        attempt(tx, executor, cfg, n + 1)

      # Falha temporária sem tentativas restantes.
      {:error, {:transient, reason}} ->
        {:error, {:operational, {:exhausted, reason}, n}}
    end
  end

  # Executa o executor em um processo isolado e monitorado.
  # Encerramentos inesperados são detectados via :DOWN e classificados como
  # transitórios; timeouts também.
  defp run_isolated(tx, executor, timeout) do
    parent = self()
    ref = make_ref()

    {pid, mon} =
      spawn_monitor(fn ->
        send(parent, {ref, executor.(tx)})
      end)

    receive do
      {^ref, result} ->
        Process.demonitor(mon, [:flush])
        normalize(result)

      {:DOWN, ^mon, :process, ^pid, reason} ->
        {:error, {:transient, {:crashed, reason}}}
    after
      timeout ->
        Process.exit(pid, :kill)
        Process.demonitor(mon, [:flush])
        flush(ref)
        {:error, {:transient, :timeout}}
    end
  end

  # Garante que o retorno do executor siga o contrato; retornos inválidos são permanentes.
  defp normalize({:ok, _} = ok), do: ok
  defp normalize({:error, {kind, _}} = err) when kind in [:transient, :permanent], do: err
  defp normalize(other), do: {:error, {:permanent, {:invalid_executor_return, other}}}

  defp flush(ref) do
    receive do
      {^ref, _} -> :ok
    after
      0 -> :ok
    end
  end

  # Backoff exponencial com jitter: valor entre metade e o total do intervalo calculado.
  defp backoff(cfg, n) do
    exp = min(cfg.base_backoff_ms * Integer.pow(2, n - 1), cfg.max_backoff_ms)
    half = div(exp, 2)
    half + :rand.uniform(max(half, 1))
  end
end
