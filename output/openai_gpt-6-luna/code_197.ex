# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de transações financeiras, atendendo somente aos seguintes requisitos:
#
# 1. Implementar novas tentativas após falhas temporárias.
# 2. Validar os dados recebidos antes de realizar operações.
# 3. Detectar encerramentos inesperados de processos.
# 4. Diferenciar erros de validação de falhas operacionais.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule FinancialTransactionManager do
  use GenServer

  @moduledoc """
  Gerencia transações com validação, tentativas para falhas temporárias
  e monitoramento de processos de execução.
  """

  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)

    case name do
      nil -> GenServer.start_link(__MODULE__, opts)
      _ -> GenServer.start_link(__MODULE__, opts, name: name)
    end
  end

  def transact(transaction), do: transact(__MODULE__, transaction)

  def transact(server, transaction) do
    GenServer.call(server, {:transact, transaction}, :infinity)
  end

  @impl true
  def init(opts) do
    backend = Keyword.get(opts, :backend, fn _transaction -> {:ok, :accepted} end)
    max_retries = Keyword.get(opts, :max_retries, 2)
    retry_delay = Keyword.get(opts, :retry_delay, 100)

    unless is_function(backend, 1) do
      raise ArgumentError, ":backend deve ser uma função de aridade 1"
    end

    unless is_integer(max_retries) and max_retries >= 0 do
      raise ArgumentError, ":max_retries deve ser um inteiro não negativo"
    end

    unless is_integer(retry_delay) and retry_delay >= 0 do
      raise ArgumentError, ":retry_delay deve ser um inteiro não negativo"
    end

    {:ok,
     %{
       backend: backend,
       max_retries: max_retries,
       retry_delay: retry_delay,
       pending: %{},
       ref_to_id: %{}
     }}
  end

  @impl true
  def handle_call({:transact, transaction}, from, state) do
    case validate(transaction) do
      [] ->
        id = make_ref()
        request = %{from: from, transaction: transaction, attempt: 0, pid: nil, ref: nil}
        state = put_in(state.pending[id], request)
        {:noreply, start_attempt(state, id)}

      errors ->
        {:reply, {:error, {:validation, errors}}, state}
    end
  end

  @impl true
  def handle_info({:worker_result, id, pid, result}, state) do
    case Map.get(state.pending, id) do
      %{pid: ^pid, ref: ref} ->
        Process.demonitor(ref, [:flush])
        state = remove_monitor(state, ref)

        case normalize_result(result) do
          {:ok, value} ->
            complete(state, id, {:ok, value})

          {:transient, reason} ->
            retry_or_fail(state, id, reason)

          {:failure, reason} ->
            complete(state, id, {:error, {:operational, reason}})
        end

      _ ->
        {:noreply, state}
    end
  end

  # Um DOWN sem resultado indica que o processo de execução terminou
  # inesperadamente; a transação será repetida conforme a configuração.
  def handle_info({:DOWN, ref, :process, _pid, reason}, state) do
    case Map.pop(state.ref_to_id, ref) do
      {nil, _ref_to_id} ->
        {:noreply, state}

      {id, ref_to_id} ->
        state = %{state | ref_to_id: ref_to_id}

        case Map.get(state.pending, id) do
          nil ->
            {:noreply, state}

          request ->
            request = %{request | pid: nil, ref: nil}
            state = put_in(state.pending[id], request)
            retry_or_fail(state, id, {:worker_terminated, reason})
        end
    end
  end

  def handle_info({:retry, id}, state) do
    case Map.get(state.pending, id) do
      %{pid: nil, ref: nil} -> {:noreply, start_attempt(state, id)}
      _ -> {:noreply, state}
    end
  end

  def handle_info(_message, state), do: {:noreply, state}

  def validate(transaction) when not is_map(transaction),
    do: [:transaction_must_be_a_map]

  def validate(transaction) do
    from = Map.get(transaction, :from_account)
    to = Map.get(transaction, :to_account)
    amount = Map.get(transaction, :amount)
    currency = Map.get(transaction, :currency)

    []
    |> maybe_add(not non_empty_binary?(from), :invalid_from_account)
    |> maybe_add(not non_empty_binary?(to), :invalid_to_account)
    |> maybe_add(non_empty_binary?(from) and from == to, :accounts_must_be_different)
    |> maybe_add(not positive_number?(amount), :amount_must_be_positive)
    |> maybe_add(not valid_currency?(currency), :invalid_currency)
  end

  defp start_attempt(state, id) do
    request = Map.fetch!(state.pending, id)
    parent = self()
    backend = state.backend
    attempt = request.attempt + 1

    {pid, ref} =
      spawn_monitor(fn ->
        result = backend.(request.transaction)
        send(parent, {:worker_result, id, self(), result})
      end)

    request = %{request | attempt: attempt, pid: pid, ref: ref}

    state
    |> put_in([:pending, id], request)
    |> put_in([:ref_to_id, ref], id)
  end

  defp normalize_result({:ok, value}), do: {:ok, value}
  defp normalize_result({:error, {:transient, reason}}), do: {:transient, reason}
  defp normalize_result({:error, reason}), do: {:failure, reason}

  defp normalize_result(other),
    do: {:failure, {:invalid_backend_response, other}}

  defp retry_or_fail(state, id, reason) do
    request = Map.fetch!(state.pending, id)
    state = remove_monitor(state, request.ref)

    request = %{request | pid: nil, ref: nil}
    state = put_in(state.pending[id], request)

    if request.attempt <= state.max_retries do
      Process.send_after(self(), {:retry, id}, state.retry_delay)
      {:noreply, state}
    else
      complete(
        state,
        id,
        {:error, {:operational, {:retries_exhausted, reason}}}
      )
    end
  end

  defp complete(state, id, result) do
    request = Map.fetch!(state.pending, id)
    state = remove_monitor(state, request.ref)
    GenServer.reply(request.from, result)
    {:noreply, %{state | pending: Map.delete(state.pending, id)}}
  end

  defp remove_monitor(state, nil), do: state

  defp remove_monitor(state, ref) do
    %{state | ref_to_id: Map.delete(state.ref_to_id, ref)}
  end

  defp maybe_add(errors, true, error), do: errors ++ [error]
  defp maybe_add(errors, false, _error), do: errors

  defp non_empty_binary?(value), do: is_binary(value) and byte_size(value) > 0

  defp positive_number?(value) when is_integer(value), do: value > 0
  defp positive_number?(value) when is_float(value), do: value > 0
  defp positive_number?(_value), do: false

  defp valid_currency?(currency) when is_binary(currency),
    do: Regex.match?(~r/\A[A-Z]{3}\z/, currency)

  defp valid_currency?(_currency), do: false
end
