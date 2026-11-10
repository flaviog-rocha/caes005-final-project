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
  - Comunicação via processos (GenServer).
  - Operações periódicas (Process.send_after).
  - Histórico persistente.
  """
  use GenServer

  @table_name :library_storage
  @history_file "library_history.dets"

  # API Pública
  def start_link(_) do
    GenServer.start_link(__MODULE__, %{}, name: __MODULE__)
  end

  def add_book(id, title), do: GenServer.call(__MODULE__, {:add, id, title})
  def get_book(id), do: GenServer.call(__MODULE__, {:get, id})
  def update_book(id, title), do: GenServer.call(__MODULE__, {:update, id, title})
  def remove_book(id), do: GenServer.call(__MODULE__, {:remove, id})

  # Callbacks do GenServer
  @impl true
  def init(_) do
    # Inicializa DETS para armazenamento e histórico
    {:ok, _} = :dets.open_file(@table_name, file: 'library.dets')
    {:ok, _} = :dets.open_file(:history, file: 'library_history.dets')
    
    # Inicia ciclo de limpeza/backup periódico
    schedule_maintenance()
    {:ok, %{}}
  end

  @impl true
  def handle_call({:add, id, title}, _from, state) do
    :dets.insert(@table_name, {id, title, :available})
    log_operation("ADD", id)
    {:reply, :ok, state}
  end

  def handle_call({:get, id}, _from, state) do
    result = case :dets.lookup(@table_name, id) do
      [{^id, title, status}] -> {:ok, {title, status}}
      [] -> {:error, :not_found}
    end
    {:reply, result, state}
  end

  def handle_call({:update, id, title}, _from, state) do
    case :dets.lookup(@table_name, id) do
      [{^id, _, status}] ->
        :dets.insert(@table_name, {id, title, status})
        log_operation("UPDATE", id)
        {:reply, :ok, state}
      [] -> {:reply, {:error, :not_found}, state}
    end
  end

  def handle_call({:remove, id}, _from, state) do
    :dets.delete(@table_name, id)
    log_operation("REMOVE", id)
    {:reply, :ok, state}
  end

  @impl true
  def handle_info(:maintenance, state) do
    # Operação periódica: Garante que os dados sejam gravados no disco
    :dets.sync(@table_name)
    :dets.sync(:history)
    IO.puts("[Maintenance] DETS synced to disk.")
    schedule_maintenance()
    {:noreply, state}
  end

  # Funções Privadas
  defp log_operation(action, id) do
    timestamp = System.system_time(:second)
    :dets.insert(:history, {timestamp, action, id})
  end

  defp schedule_maintenance do
    # Executa a cada 60 segundos
    Process.send_after(self(), :maintenance, 60_000)
  end
end
