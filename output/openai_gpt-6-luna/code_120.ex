# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de empréstimos de livros, atendendo somente aos seguintes requisitos:
#
# 1. Permitir que diferentes entidades sejam processadas independentemente.
# 2. Permitir comunicação entre diferentes processos.
# 3. Permitir limitar a quantidade de operações simultâneas.
# 4. Implementar novas tentativas após falhas temporárias.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - Stream
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule BookLoanManager do
  @moduledoc """
  Gerencia empréstimos isolados por entidade e processa operações com
  concorrência limitada e novas tentativas para falhas temporárias.

  Cada operação deve ser um mapa com as chaves :entity, :action e :book.
  As ações aceitas são :borrow e :return.
  """

  def run(operations, opts \\ []) do
    operations = Enum.to_list(operations)

    entities =
      operations
      |> Stream.map(&Map.fetch!(&1, :entity))
      |> Stream.uniq()
      |> Enum.to_list()

    case start_entities(entities, Keyword.get(opts, :temporary_failures, %{})) do
      {:ok, entity_pids} ->
        try do
          max_concurrency =
            opts
            |> Keyword.get(:max_concurrency, System.schedulers_online())
            |> max(1)

          max_attempts =
            opts
            |> Keyword.get(:max_attempts, 3)
            |> max(1)

          call_timeout = Keyword.get(opts, :call_timeout, 5_000)
          task_timeout = Keyword.get(opts, :task_timeout, 30_000)

          operations
          |> Stream.map(& &1)
          |> Task.async_stream(
            fn operation ->
              execute(operation, entity_pids, max_attempts, call_timeout)
            end,
            max_concurrency: max_concurrency,
            ordered: true,
            timeout: task_timeout
          )
          |> Enum.map(fn
            {:ok, result} -> result
            {:exit, reason} -> {:error, {:task_exit, reason}}
          end)
        after
          Enum.each(entity_pids, fn {_entity, pid} ->
            if Process.alive?(pid), do: GenServer.stop(pid)
          end)
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp start_entities(entities, temporary_failures) do
    Enum.reduce_while(entities, {:ok, %{}}, fn entity, {:ok, pids} ->
      case BookLoanManager.Entity.start(entity, temporary_failures) do
        {:ok, pid} ->
          {:cont, {:ok, Map.put(pids, entity, pid)}}

        {:error, reason} ->
          Enum.each(pids, fn {_key, pid} ->
            if Process.alive?(pid), do: GenServer.stop(pid)
          end)

          {:halt, {:error, reason}}
      end
    end)
  end

  defp execute(operation, entity_pids, max_attempts, call_timeout) do
    with {:ok, entity} <- Map.fetch(operation, :entity),
         {:ok, action} <- Map.fetch(operation, :action),
         {:ok, book} <- Map.fetch(operation, :book),
         {:ok, pid} <- Map.fetch(entity_pids, entity) do
      retry(pid, action, book, max_attempts, call_timeout)
    else
      :error -> {:error, :invalid_operation}
    end
  end

  defp retry(pid, action, book, attempts_left, call_timeout) do
    result =
      try do
        GenServer.call(pid, {:operate, action, book}, call_timeout)
      catch
        :exit, reason -> {:error, {:process_exit, reason}}
      end

    case result do
      {:error, :temporary} when attempts_left > 1 ->
        retry(pid, action, book, attempts_left - 1, call_timeout)

      other ->
        other
    end
  end
end

defmodule BookLoanManager.Entity do
  use GenServer

  def start(entity, temporary_failures) do
    GenServer.start(__MODULE__, {entity, temporary_failures})
  end

  @impl true
  def init({entity, temporary_failures}) do
    {:ok,
     %{
       entity: entity,
       loans: MapSet.new(),
       temporary_failures: temporary_failures
     }}
  end

  @impl true
  def handle_call({:operate, action, book}, _from, state) do
    failure_key = {state.entity, action, book}
    failures_left = Map.get(state.temporary_failures, failure_key, 0)

    if failures_left > 0 do
      updated_failures = Map.put(state.temporary_failures, failure_key, failures_left - 1)
      {:reply, {:error, :temporary}, %{state | temporary_failures: updated_failures}}
    else
      apply_operation(action, book, state)
    end
  end

  defp apply_operation(:borrow, book, state) do
    if MapSet.member?(state.loans, book) do
      {:reply, {:error, :already_borrowed}, state}
    else
      {:reply, {:ok, :borrowed}, %{state | loans: MapSet.put(state.loans, book)}}
    end
  end

  defp apply_operation(:return, book, state) do
    if MapSet.member?(state.loans, book) do
      {:reply, {:ok, :returned}, %{state | loans: MapSet.delete(state.loans, book)}}
    else
      {:reply, {:error, :not_borrowed}, state}
    end
  end

  defp apply_operation(action, _book, state) do
    {:reply, {:error, {:unsupported_action, action}}, state}
  end
end
