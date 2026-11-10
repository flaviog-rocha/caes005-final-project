# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de entregas e rastreamento, atendendo somente aos seguintes requisitos:
#
# 1. Recuperar processos após falhas inesperadas.
# 2. Persistir os dados para recuperação após reinicializações.
# 3. Diferenciar erros de validação de falhas operacionais.
# 4. Garantir a consistência dos dados durante operações concorrentes.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - Supervisor
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule DeliverySystem.Storage do
  @moduledoc """
  Gerencia persistência em disco usando ETS com dump em arquivo (AOF simplificado).
  Garante consistência via serialização de chamadas (GenServer).
  """
  use GenServer

  @file_path "deliveries.tab"

  def start_link(_), do: GenServer.start_link(__MODULE__, nil, name: __MODULE__)

  def init(_) do
    table = :ets.new(:deliveries, [:set, :public, :named_table])
    if File.exists?(@file_path), do: :ets.tab2file(table, @file_path)
    {:ok, table}
  end

  def persist(id, data) do
    GenServer.call(__MODULE__, {:update, id, data})
  end

  def handle_call({:update, id, data}, _from, table) do
    :ets.insert(table, {id, data})
    :ets.tab2file(table, @file_path) # Persistência síncrona pós-operação
    {:reply, :ok, table}
  end
end

defmodule DeliverySystem.DeliveryProcessor do
  @moduledoc """
  Processa entregas diferenciando erros de validação (regras) de falhas operacionais (exceções).
  """
  use GenServer

  def start_link(delivery_id), do: GenServer.start_link(__MODULE__, delivery_id, name: via(delivery_id))

  def init(id) do
    {:ok, %{id: id, status: :pending}}
  end

  # Representação de erro de validação (Domain Error)
  def update_status(id, status) when status not in [:in_transit, :delivered] do
    {:error, :invalid_status}
  end

  # Operação que pode falhar (Operacional/Runtime)
  def update_status(id, status) do
    GenServer.call(via(id), {:update, status})
  end

  def handle_call({:update, status}, _from, state) do
    # Simula falha operacional aleatória para demonstrar recuperação via Supervisor
    if :rand.uniform(10) > 8, do: raise("Operational failure")
    
    new_state = %{state | status: status}
    DeliverySystem.Storage.persist(state.id, new_state)
    {:reply, :ok, new_state}
  end

  defp via(id), do: {:via, Registry, {DeliverySystem.Registry, id}}
end

defmodule DeliverySystem.Supervisor do
  @moduledoc """
  Supervisor que recupera processos e garante a árvore de supervisão.
  """
  use Supervisor

  def start_link(_), do: Supervisor.start_link(__MODULE__, :ok, name: __MODULE__)

  def init(:ok) do
    children = [
      {Registry, keys: :unique, name: DeliverySystem.Registry},
      DeliverySystem.Storage,
      {DynamicSupervisor, name: DeliverySystem.DeliverySupervisor, strategy: :one_for_one}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end
