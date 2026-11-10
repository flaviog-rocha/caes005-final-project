# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de transações financeiras, atendendo somente aos seguintes requisitos:
#
# 1. Permitir cadastrar, consultar, atualizar e remover registros.
# 2. Permitir consultar o estado atual das operações em andamento.
# 3. Executar determinadas operações periodicamente.
# 4. Processar grandes volumes de dados sem carregá-los integralmente na memória.
# 5. Permitir a execução de operações simultâneas.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - Registry
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule FinancialSystem do
  @moduledoc """
  Sistema de gerenciamento de transações utilizando GenServers, Registry e Streams.
  """

  # 1. Registro dinâmico de processos por ID de transação
  def child_spec(id) do
    %{
      id: {__MODULE__, id},
      start: {__MODULE__, :start_link, [id]},
      type: :worker,
      restart: :transient
    }
  end

  def start_link(id), do: GenServer.start_link(__MODULE__, id, name: {:via, Registry, {FinancialRegistry, id}})

  # 1. CRUD de Transações
  def create(id, data), do: DynamicSupervisor.start_child(TransactionSupervisor, {__MODULE__, id})
  def get(id), do: GenServer.call({:via, Registry, {FinancialRegistry, id}}, :get_state)
  def update(id, data), do: GenServer.cast({:via, Registry, {FinancialRegistry, id}}, {:update, data})
  def delete(id), do: DynamicSupervisor.terminate_child(TransactionSupervisor, GenServer.whereis({:via, Registry, {FinancialRegistry, id}}))

  # 2. Estado das operações
  def list_active do
    Registry.select(FinancialRegistry, [{{:_, :"$1", :_}, [], [{{:key, :"$1"}}]}])
  end

  # 4. Processamento de grandes volumes (Streams)
  def process_large_data(data_stream) do
    data_stream
    |> Stream.chunk_every(100)
    |> Task.async_stream(fn chunk ->
      # 5. Execução simultânea (Task.async_stream)
      Enum.each(chunk, fn item -> IO.puts("Processando: #{item}") end)
    end, max_concurrency: System.schedulers_online())
    |> Stream.run()
  end

  # 3. Operações periódicas (GenServer call via :timer)
  def init(id) do
    :timer.send_interval(60_000, :periodic_sync)
    {:ok, %{id: id, status: :pending, updated_at: DateTime.utc_now()}}
  end

  def handle_call(:get_state, _from, state), do: {:reply, state, state}

  def handle_cast({:update, data}, state), do: {:noreply, Map.merge(state, data)}

  def handle_info(:periodic_sync, state) do
    IO.puts("Sincronizando transação #{state.id} periodicamente...")
    {:noreply, state}
  end

  # Setup do Supervisor e Registry
  def start_services do
    children = [
      {Registry, keys: :unique, name: FinancialRegistry},
      {DynamicSupervisor, name: TransactionSupervisor, strategy: :one_for_one}
    ]
    Supervisor.start_link(children, strategy: :one_for_all)
  end
end

# Execução de exemplo:
# FinancialSystem.start_services()
# FinancialSystem.create("tx_1", %{})
# FinancialSystem.process_large_data(Stream.iterate(1, &(&1 + 1)) |> Stream.take(1000))
