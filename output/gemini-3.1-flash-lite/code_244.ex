# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de um sistema de cache, atendendo somente aos seguintes requisitos:
#
# 1. Manter um histórico das operações realizadas.
# 2. Persistir os dados para recuperação após reinicializações.
# 3. Permitir comunicação entre diferentes processos.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule PersistentCache do
  @moduledoc """
  Um sistema de cache persistente baseado em GenServer, utilizando :dets 
  para persistência em disco e um histórico de operações em memória.
  """
  use GenServer

  @db_file "cache_storage.dets"

  # API Pública

  def start_link(_opts) do
    GenServer.start_link(__MODULE__, %{}, name: __MODULE__)
  end

  def put(key, value) do
    GenServer.call(__MODULE__, {:put, key, value})
  end

  def get(key) do
    GenServer.call(__MODULE__, {:get, key})
  end

  def get_history do
    GenServer.call(__MODULE__, :get_history)
  end

  # Callbacks do GenServer

  @impl true
  def init(_) do
    # Abre ou cria o arquivo para persistência (DETS)
    {:ok, table} = :dets.open_file(:cache_table, [file: ~c"#{@db_file}", type: :set])
    {:ok, %{table: table, history: []}}
  end

  @impl true
  def handle_call({:put, key, value}, _from, state) do
    :dets.insert(state.table, {key, value})
    :dets.sync(state.table)

    new_history = [{:put, key, System.system_time(:second)} | state.history]
    {:reply, :ok, %{state | history: new_history}}
  end

  @impl true
  def handle_call({:get, key}, _from, state) do
    result = case :dets.lookup(state.table, key) do
      [{^key, value}] -> {:ok, value}
      [] -> :not_found
    end

    new_history = [{:get, key, System.system_time(:second)} | state.history]
    {:reply, result, %{state | history: new_history}}
  end

  @impl true
  def handle_call(:get_history, _from, state) do
    {:reply, state.history, state}
  end

  @impl true
  def terminate(_reason, state) do
    :dets.close(state.table)
    :ok
  end
end
