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
  Utiliza um GenServer como "mock database" e controle de estado.
  """
  use GenServer

  # --- API Pública ---

  def start_link(_), do: GenServer.start_link(__MODULE__, %{data: %{}, locks: MapSet.new()}, name: __MODULE__)

  @doc "Cadastra uma entrega. O request_id garante idempotência."
  def create(id, payload, request_id) do
    GenServer.call(__MODULE__, {:execute, request_id, fn state ->
      if Map.has_key?(state.data, id), do: {:error, :already_exists}, else: {:ok, put_in(state.data[id], payload)}
    end})
  end

  @doc "Consulta uma entrega."
  def get(id), do: GenServer.call(__MODULE__, {:get, id})

  @doc "Atualiza uma entrega com controle de concorrência via Semaphore."
  def update(id, payload, request_id) do
    # Simula limite de 5 operações simultâneas via pool global (exemplo didático)
    :poolboy.transaction(:delivery_pool, fn _worker ->
      GenServer.call(__MODULE__, {:execute, request_id, fn state ->
        if Map.has_key?(state.data, id), do: {:ok, put_in(state.data[id], payload)}, else: {:error, :not_found}
      end})
    end)
  end

  @doc "Remove uma entrega."
  def delete(id, request_id) do
    GenServer.call(__MODULE__, {:execute, request_id, fn state ->
      {:ok, %{state | data: Map.delete(state.data, id)}}
    end})
  end

  # --- Callbacks do GenServer ---

  @impl true
  def init(state), do: {:ok, state}

  @impl true
  def handle_call({:get, id}, _from, state), do: {:reply, Map.get(state.data, id), state}

  @impl true
  def handle_call({:execute, request_id, func}, _from, state) do
    if MapSet.member?(state.locks, request_id) do
      {:reply, {:error, :idempotency_violation}, state}
    else
      case func.(state) do
        {:ok, new_state} when is_map(new_state) ->
          {:reply, :ok, %{new_state | locks: MapSet.put(state.locks, request_id)}}
        {:ok, result} ->
          {:reply, result, %{state | locks: MapSet.put(state.locks, request_id)}}
        {:error, reason} ->
          {:reply, {:error, reason}, state}
      end
    end
  end
end

# --- Exemplo de Migração (Conceitual) ---
# Em um cenário real com Ecto, usaríamos:
# defmodule Repo.Migrations.CreateDeliveries do
#   use Ecto.Migration
#   def change do
#     create table(:deliveries, primary_key: false) do
#       add :id, :binary_id, primary_key: true
#       add :payload, :map
#       add :request_id, :string # Unique index para garantir idempotência no DB
#     end
#     create unique_index(:deliveries, [:request_id])
#   end
# end
