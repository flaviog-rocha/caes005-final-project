# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o processamento de arquivos CSV, atendendo somente aos seguintes requisitos:
#
# 1. Permitir limitar a quantidade de operações simultâneas.
# 2. Permitir cadastrar, consultar, atualizar e remover registros.
# 3. Impedir atualizações parcialmente concluídas.
# 4. Detectar encerramentos inesperados de processos.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule CsvProcessor do
  @moduledoc """
  Processador de CSV com concorrência limitada, atomicidade,
  CRUD em memória (simulando estado persistente) e monitoramento.
  """
  use GenServer

  # --- API Pública ---

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def add(record), do: GenServer.call(__MODULE__, {:add, record})
  def get(id), do: GenServer.call(__MODULE__, {:get, id})
  def update(id, new_data), do: GenServer.call(__MODULE__, {:update, id, new_data})
  def delete(id), do: GenServer.call(__MODULE__, {:delete, id})

  # Processamento de arquivo com concorrência limitada via Task.async_stream
  def process_csv(file_path, concurrency \\ 4) do
    File.stream!(file_path)
    |> CSV.decode!() # Assume biblioteca externa ou parser simples
    |> Task.async_stream(fn row -> 
         # Simula processamento custoso
         add(row)
       end, max_concurrency: concurrency, ordered: false)
    |> Stream.run()
  end

  # --- Callbacks do GenServer ---

  @impl true
  def init(_opts) do
    # O estado é um Map para busca O(1)
    {:ok, %{records: %{}}}
  end

  @impl true
  def handle_call({:add, record}, _from, state) do
    id = Map.get(record, :id)
    {:reply, :ok, put_in(state, [:records, id], record)}
  end

  @impl true
  def handle_call({:get, id}, _from, state) do
    {:reply, Map.get(state.records, id), state}
  end

  # Transação: Impedir atualizações parciais
  @impl true
  def handle_call({:update, id, new_data}, _from, state) do
    case Map.has_key?(state.records, id) do
      true ->
        # O estado só é atualizado se a operação for concluída
        # Em um sistema real com persistência, usaria :ets ou transações de banco
        new_state = put_in(state, [:records, id], new_data)
        {:reply, :ok, new_state}
      false ->
        {:reply, {:error, :not_found}, state}
    end
  end

  @impl true
  def handle_call({:delete, id}, _from, state) do
    {:reply, :ok, %{state | records: Map.delete(state.records, id)}}
  end

  # --- Monitoramento ---

  # Detecta encerramentos inesperados de processos que dependem deste servidor
  def monitor_process(pid) do
    Process.monitor(pid)
  end

  @impl true
  def handle_info({:DOWN, _ref, :process, _pid, reason}, state) do
    IO.puts("Processo monitorado encerrou: #{inspect(reason)}")
    {:noreply, state}
  end
end
