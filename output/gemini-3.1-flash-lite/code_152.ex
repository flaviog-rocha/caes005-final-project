# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de entregas e rastreamento, atendendo somente aos seguintes requisitos:
#
# 1. Permitir cadastrar, consultar, atualizar e remover registros.
# 2. Impedir que uma mesma operação seja executada mais de uma vez.
# 3. Permitir limitar a quantidade de operações simultâneas.
# 4. Quando fizer sentido para o domínio, considerar a comunicação com um banco de dados fictício e, opcionalmente, o uso de Ecto.Migration, sem tornar isso obrigatório.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule DeliveryManager do
  @moduledoc """
  Gerenciador de entregas com controle de concorrência e idempotência.
  Utiliza um GenServer como "mock" de banco de dados em memória e 
  um Registry para garantir a exclusividade de operações por ID.
  """

  use GenServer

  # --- API Pública ---

  def start_link(_), do: GenServer.start_link(__MODULE__, %{}, name: __MODULE__)

  @doc "Cadastra uma entrega. Garante idempotência via :registry."
  def create(id, data) do
    with_idempotency(id, fn ->
      GenServer.call(__MODULE__, {:create, id, data})
    end)
  end

  def get(id), do: GenServer.call(__MODULE__, {:get, id})

  def update(id, data), do: GenServer.call(__MODULE__, {:update, id, data})

  def delete(id), do: GenServer.call(__MODULE__, {:delete, id})

  @doc "Limita operações simultâneas usando um pool via Task.Supervisor ou Semaphore."
  def process_batch(items, limit \\ 3) do
    items
    |> Task.async_stream(fn item -> create(item.id, item.data) end, max_concurrency: limit)
    |> Enum.to_list()
  end

  # --- Lógica de Idempotência ---

  defp with_idempotency(id, fun) do
    case Registry.register(__MODULE__.Registry, id, :locked) do
      {:ok, _pid} ->
        result = fun.()
        Registry.unregister(__MODULE__.Registry, id)
        result
      {:error, {:already_registered, _pid}} ->
        {:error, :operation_in_progress}
    end
  end

  # --- Callbacks do GenServer ---

  @impl true
  def init(_) do
    Registry.start_link(keys: :unique, name: __MODULE__.Registry)
    {:ok, %{}}
  end

  @impl true
  def handle_call({:create, id, data}, _from, state) do
    if Map.has_key?(state, id), 
      do: {:reply, {:error, :already_exists}, state},
      else: {:reply, :ok, Map.put(state, id, data)}
  end

  @impl true
  def handle_call({:get, id}, _from, state), do: {:reply, Map.get(state, id), state}

  @impl true
  def handle_call({:update, id, data}, _from, state) do
    if Map.has_key?(state, id),
      do: {:reply, :ok, Map.put(state, id, data)},
      else: {:reply, {:error, :not_found}, state}
  end

  @impl true
  def handle_call({:delete, id}, _from, state), do: {:reply, :ok, Map.delete(state, id)}
end

# Exemplo de uso:
# DeliveryManager.start_link([])
# DeliveryManager.create("ent-001", %{status: "pendente"})
# DeliveryManager.get("ent-001")
