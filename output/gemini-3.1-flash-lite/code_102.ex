# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de empréstimos de livros, atendendo somente aos seguintes requisitos:
#
# 1. Permitir comunicação entre diferentes processos.
# 2. Executar determinadas operações periodicamente.
# 3. Manter um histórico das operações realizadas.
# 4. Permitir cadastrar, consultar, atualizar e remover registros.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - DETS
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule LibraryManager do
  @moduledoc """
  Gerenciador de empréstimos de livros utilizando DETS.
  - Comunicação via GenServer (mensagens entre processos).
  - Execução periódica via Process.send_after.
  - Histórico em arquivo persistente.
  """

  use GenServer

  @table_name :library_storage
  @filename 'library.dets'

  # --- API Pública ---

  def start_link(_), do: GenServer.start_link(__MODULE__, nil, name: __MODULE__)

  def register_book(id, title), do: GenServer.call(__MODULE__, {:put, id, %{title: title, status: :available}})
  def get_book(id), do: GenServer.call(__MODULE__, {:get, id})
  def update_book(id, data), do: GenServer.call(__MODULE__, {:put, id, data})
  def remove_book(id), do: GenServer.call(__MODULE__, {:delete, id})
  def get_history, do: GenServer.call(__MODULE__, :history)

  # --- Callbacks do GenServer ---

  @impl true
  def init(_) do
    {:ok, _} = :dets.open_file(@table_name, file: @filename)
    schedule_cleanup()
    {:ok, %{history: []}}
  end

  @impl true
  def handle_call({:put, id, data}, _from, state) do
    :dets.insert(@table_name, {id, data})
    new_history = update_history(state.history, "INSERT/UPDATE: #{id}")
    {:reply, :ok, %{state | history: new_history}}
  end

  @impl true
  def handle_call({:get, id}, _from, state) do
    result = case :dets.lookup(@table_name, id) do
      [{^id, data}] -> data
      [] -> :not_found
    end
    {:reply, result, state}
  end

  @impl true
  def handle_call({:delete, id}, _from, state) do
    :dets.delete(@table_name, id)
    new_history = update_history(state.history, "DELETE: #{id}")
    {:reply, :ok, %{state | history: new_history}}
  end

  @impl true
  def handle_call(:history, _from, state) do
    {:reply, state.history, state}
  end

  # Execução periódica
  @impl true
  def handle_info(:cleanup, state) do
    IO.puts("[LOG] Executando rotina periódica de manutenção...")
    :dets.sync(@table_name)
    schedule_cleanup()
    {:noreply, state}
  end

  # --- Funções Privadas ---

  defp schedule_cleanup, do: Process.send_after(self(), :cleanup, 60_000)

  defp update_history(history, event) do
    timestamp = DateTime.utc_now() |> DateTime.to_string()
    [%{event: event, time: timestamp} | history] |> Enum.take(100)
  end

  def terminate(_reason, _state) do
    :dets.close(@table_name)
  end
end
