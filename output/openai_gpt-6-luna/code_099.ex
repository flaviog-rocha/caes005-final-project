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

defmodule DeliveryTracker do
  @moduledoc """
  Gerenciamento de entregas com persistência em DETS e recuperação supervisionada.
  """

  @server DeliveryTracker.Server

  def start_link(opts \\ []) do
    DeliveryTracker.Supervisor.start_link(opts)
  end

  def create(id, destination) do
    with :ok <- validate_text(id, :id),
         :ok <- validate_text(destination, :destination) do
      safe_call({:create, id, destination})
    end
  end

  def track(id, status, location \\ nil) do
    with :ok <- validate_text(id, :id),
         :ok <- validate_status(status),
         :ok <- validate_optional_text(location, :location) do
      safe_call({:track, id, status, location})
    end
  end

  def get(id) do
    with :ok <- validate_text(id, :id) do
      safe_call({:get, id})
    end
  end

  def list do
    safe_call(:list)
  end

  defp safe_call(message) do
    GenServer.call(@server, message)
  catch
    :exit, reason -> {:error, {:operational, reason}}
  end

  defp validate_text(value, field) do
    if is_binary(value) and String.valid?(value) and String.trim(value) != "" do
      :ok
    else
      {:error, {:validation, {:invalid_field, field}}}
    end
  end

  defp validate_optional_text(nil, _field), do: :ok
  defp validate_optional_text(value, field), do: validate_text(value, field)

  defp validate_status(status) do
    if status in [:pending, :picked_up, :in_transit, :delivered, :cancelled] do
      :ok
    else
      {:error, {:validation, {:invalid_status, status}}}
    end
  end
end

defmodule DeliveryTracker.Server do
  use GenServer

  @table __MODULE__

  @transitions %{
    pending: [:picked_up, :cancelled],
    picked_up: [:in_transit, :cancelled],
    in_transit: [:delivered, :cancelled],
    delivered: [],
    cancelled: []
  }

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: DeliveryTracker)
  end

  @impl true
  def init(opts) do
    file = Keyword.get(opts, :file, "deliveries.dets")

    with :ok <- ensure_parent_directory(file),
         {:ok, @table} <-
           :dets.open_file(@table,
             file: String.to_charlist(file),
             type: :set,
             repair: true
           ) do
      {:ok, load_deliveries()}
    else
      {:error, reason} -> {:stop, {:operational, reason}}
      other -> {:stop, {:operational, other}}
    end
  end

  @impl true
  def handle_call({:create, id, destination}, _from, state) do
    if Map.has_key?(state, id) do
      {:reply, {:error, {:validation, :already_exists}}, state}
    else
      now = System.system_time(:second)

      delivery = %{
        id: id,
        destination: destination,
        status: :pending,
        location: nil,
        history: [%{status: :pending, location: nil, at: now}]
      }

      persist_and_reply(delivery, state)
    end
  end

  def handle_call({:track, id, status, location}, _from, state) do
    case Map.fetch(state, id) do
      :error ->
        {:reply, {:error, {:validation, :not_found}}, state}

      {:ok, delivery} ->
        if status == delivery.status or status in Map.fetch!(@transitions, delivery.status) do
          now = System.system_time(:second)
          new_location = location || delivery.location

          updated = %{
            delivery
            | status: status,
              location: new_location,
              history: [
                %{status: status, location: new_location, at: now}
                | delivery.history
              ]
          }

          persist_and_reply(updated, state)
        else
          {:reply, {:error, {:validation, {:invalid_transition, delivery.status, status}}},
           state}
        end
    end
  end

  def handle_call({:get, id}, _from, state) do
    case Map.fetch(state, id) do
      {:ok, delivery} -> {:reply, {:ok, delivery}, state}
      :error -> {:reply, {:error, {:validation, :not_found}}, state}
    end
  end

  def handle_call(:list, _from, state) do
    deliveries = state |> Map.values() |> Enum.sort_by(& &1.id)
    {:reply, {:ok, deliveries}, state}
  end

  defp persist_and_reply(delivery, state) do
    case persist(delivery) do
      :ok ->
        {:reply, {:ok, delivery}, Map.put(state, delivery.id, delivery)}

      {:error, reason} ->
        # Reiniciar o processo força a recarga do estado persistido.
        {:stop, {:persistence_failure, reason}, {:error, {:operational, reason}}, state}
    end
  end

  defp persist(delivery) do
    case :dets.insert(@table, {delivery.id, delivery}) do
      :ok ->
        case :dets.sync(@table) do
          :ok -> :ok
          {:error, reason} -> {:error, reason}
          other -> {:error, other}
        end

      {:error, reason} ->
        {:error, reason}

      other ->
        {:error, other}
    end
  catch
    kind, reason -> {:error, {kind, reason}}
  end

  defp load_deliveries do
    :dets.foldl(fn {id, delivery}, acc -> Map.put(acc, id, delivery) end, %{}, @table)
  end

  defp ensure_parent_directory(file) do
    file
    |> Path.dirname()
    |> File.mkdir_p()
  end
end

defmodule DeliveryTracker.Supervisor do
  use Supervisor

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    children = [
      {DeliveryTracker.Server, opts}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end
