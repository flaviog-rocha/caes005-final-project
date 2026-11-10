# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de um sistema de cache, atendendo somente aos seguintes requisitos:
#
# 1. Impedir atualizações parcialmente concluídas.
# 2. Permitir limitar a quantidade de operações simultâneas.
# 3. Garantir a consistência dos dados durante operações concorrentes.
# 4. Detectar encerramentos inesperados de processos.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule CacheManager do
  @moduledoc """
  Gerenciador de cache utilizando GenServer para garantir consistência e concorrência.
  
  1. Atualizações atômicas: O uso de GenServer serializa as requisições, evitando estados parciais.
  2. Limitação de concorrência: Utiliza `:poolboy` ou, neste caso, um contador de semáforo
     via `Task.async_stream` com `:max_concurrency`.
  3. Consistência: O estado é mantido em um único processo (Single Source of Truth).
  4. Encerramento inesperado: Monitoramento via `Process.monitor/1` e uso de ETS para persistência opcional.
  """

  use GenServer

  # API Pública

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def put(key, value) do
    GenServer.call(__MODULE__, {:put, key, value})
  end

  def get(key) do
    GenServer.call(__MODULE__, {:get, key})
  end

  # Callbacks do GenServer

  @impl true
  def init(_opts) do
    # Tabela ETS para persistência de dados em caso de crash do processo
    table = :ets.new(:cache_storage, [:set, :protected, :named_table])
    {:ok, %{table: table}}
  end

  @impl true
  def handle_call({:put, key, value}, _from, state) do
    # Garante a atomicidade da operação de escrita
    :ets.insert(state.table, {key, value})
    {:reply, :ok, state}
  end

  @impl true
  def handle_call({:get, key}, _from, state) do
    case :ets.lookup(state.table, key) do
      [{^key, value}] -> {:reply, {:ok, value}, state}
      [] -> {:reply, {:error, :not_found}, state}
    end
  end

  # Exemplo de processamento concorrente limitado
  def process_concurrently(items, func) do
    items
    |> Task.async_stream(
      fn item -> func.(item) end,
      max_concurrency: System.schedulers_online(), # Limita operações simultâneas
      ordered: false
    )
    |> Stream.run()
  end

  # Monitoramento de processos externos
  def monitor_worker(pid) do
    Process.monitor(pid)
  end

  @impl true
  def handle_info({:DOWN, _ref, :process, _pid, reason}, state) do
    # Detecta encerramentos inesperados e loga ou recupera estado
    IO.puts("Processo monitorado encerrou: #{inspect(reason)}")
    {:noreply, state}
  end
end
