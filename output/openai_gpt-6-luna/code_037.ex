# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o monitoramento de sensores, atendendo somente aos seguintes requisitos:
#
# 1. Impedir que uma mesma operação seja executada mais de uma vez.
# 2. Processar grandes volumes de dados sem carregá-los integralmente na memória.
# 3. Manter um histórico das operações realizadas.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - GenServer
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule SensorMonitor do
  use GenServer

  @moduledoc """
  Monitora leituras de sensores, processando enumeráveis incrementalmente e
  mantendo um histórico de operações identificadas por chave única.
  """

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, :ok, opts)
  end

  @doc """
  Processa cada leitura uma vez por `operation_id`.

  `readings` pode ser um `Stream` ou outro `Enumerable`. `reducer` recebe cada
  leitura e o acumulador atual; o acumulador inicial é `initial_acc`.
  """
  def monitor(server, operation_id, readings) do
    monitor(server, operation_id, readings, fn _reading, count -> count + 1 end, 0)
  end

  def monitor(server, operation_id, readings, reducer, initial_acc)
      when is_function(reducer, 2) do
    GenServer.call(
      server,
      {:monitor, operation_id, readings, reducer, initial_acc},
      :infinity
    )
  end

  @doc "Retorna o histórico em ordem cronológica."
  def history(server) do
    GenServer.call(server, :history)
  end

  @impl true
  def init(:ok) do
    {:ok, %{operations: %{}, history: []}}
  end

  @impl true
  def handle_call(:history, _from, state) do
    {:reply, Enum.reverse(state.history), state}
  end

  def handle_call({:monitor, operation_id, readings, reducer, initial_acc}, _from, state) do
    case Map.fetch(state.operations, operation_id) do
      {:ok, entry} ->
        {:reply, {:already_processed, entry}, state}

      :error ->
        {reply, entry} = execute(operation_id, readings, reducer, initial_acc)
        next_state = %{
          state
          | operations: Map.put(state.operations, operation_id, entry),
            history: [entry | state.history]
        }

        {:reply, reply, next_state}
    end
  end

  defp execute(operation_id, readings, reducer, initial_acc) do
    try do
      {result, count} =
        Enum.reduce(readings, {initial_acc, 0}, fn reading, {acc, count} ->
          {reducer.(reading, acc), count + 1}
        end)

      entry = %{
        operation_id: operation_id,
        status: :completed,
        items_processed: count,
        completed_at: DateTime.utc_now()
      }

      {{:ok, result}, entry}
    rescue
      error ->
        entry = %{
          operation_id: operation_id,
          status: :failed,
          items_processed: nil,
          error: Exception.format(:error, error, __STACKTRACE__),
          completed_at: DateTime.utc_now()
        }

        {{:error, entry.error}, entry}
    catch
      kind, reason ->
        entry = %{
          operation_id: operation_id,
          status: :failed,
          items_processed: nil,
          error: "#{kind}: #{inspect(reason)}",
          completed_at: DateTime.utc_now()
        }

        {{:error, entry.error}, entry}
    end
  end
end
