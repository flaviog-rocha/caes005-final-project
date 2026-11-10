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
  Processador de CSV com controle de concorrência, atomicidade e monitoramento de processos.
  Utiliza um GenServer como gerenciador de estado e um Pool de Tasks para processamento.
  """

  use GenServer

  # --- API Pública ---

  def start_link(max_concurrency) do
    GenServer.start_link(__MODULE__, max_concurrency, name: __MODULE__)
  end

  def create(id, data), do: GenServer.call(__MODULE__, {:create, id, data})
  def read(id), do: GenServer.call(__MODULE__, {:read, id})
  def update(id, new_data), do: GenServer.call(__MODULE__, {:update, id, new_data})
  def delete(id), do: GenServer.call(__MODULE__, {:delete, id})

  def process_file(file_path) do
    # Limita concorrência via Task.async_stream
    limit = GenServer.call(__MODULE__, :get_limit)
    
    File.stream!(file_path)
    |> Stream.map(&String.trim/1)
    |> Task.async_stream(fn line ->
      # Lógica de processamento isolada
      IO.puts("Processando: #{line}")
    end, max_concurrency: limit, ordered: false)
    |> Stream.run()
  end

  # --- Callbacks do GenServer ---

  @impl true
  def init(max_concurrency) do
    # Process.flag(:trap_exit, true) permite detectar quedas de processos vinculados
    Process.flag(:trap_exit, true)
    {:ok, %{data: %{}, limit: max_concurrency}}
  end

  @impl true
  def handle_call({:create, id, val}, _from, state) do
    if Map.has_key?(state.data, id) do
      {:reply, {:error, :already_exists}, state}
    else
      {:reply, :ok, put_in(state, [:data, id], val)}
    end
  end

  @impl true
  def handle_call({:read, id}, _from, state) do
    {:reply, Map.get(state.data, id, {:error, :not_found}), state}
  end

  @impl true
  def handle_call({:update, id, new_val}, _from, state) do
    # Atomicidade garantida pelo GenServer (serialização de mensagens)
    # O estado só é alterado se o ID existir
    case Map.has_key?(state.data, id) do
      true -> {:reply, :ok, put_in(state, [:data, id], new_val)}
      false -> {:reply, {:error, :not_found}, state}
    end
  end

  @impl true
  def handle_call({:delete, id}, _from, state) do
    {:reply, :ok, Map.delete(state.data, id)}
  end

  @impl true
  def handle_call(:get_limit, _from, state), do: {:reply, state.limit, state}

  # --- Monitoramento de Processos ---

  @impl true
  def handle_info({:EXIT, pid, reason}, state) do
    # Detecta encerramento inesperado de workers ou processos vinculados
    IO.puts("Processo #{inspect(pid)} encerrou inesperadamente: #{inspect(reason)}")
    {:noreply, state}
  end
end
