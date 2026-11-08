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

defmodule DeliveryTracker.Server do
  use GenServer

  @table :delivery_tracker_data
  @statuses [:created, :picked_up, :in_transit, :out_for_delivery, :delivered, :failed]

  def start_link(opts \\ []) do
    GenServer.start_link(
      __MODULE__,
      opts,
      name: Keyword.get(opts, :name, __MODULE__)
    )
  end

  def create_delivery(id, details \\ %{}, server \\ __MODULE__) do
    safe_call(server, {:create_delivery, id, details})
  end

  def update_status(id, status, server \\ __MODULE__) do
    safe_call(server, {:update_status, id, status})
  end

  def get_delivery(id, server \\ __MODULE__) do
    safe_call(server, {:get_delivery, id})
  end

  def list_deliveries(server \\ __MODULE__) do
    safe_call(server, :list_deliveries)
  end

  @impl true
  def init(opts) do
    path = Keyword.get(opts, :path, "delivery_tracker.dets")

    with true <- is_binary(path) || {:error, :invalid_path},
         :ok <- File.mkdir_p(Path.dirname(path)),
         {:ok, @table} <-
           :dets.open_file(@table,
             file: String.to_charlist(path),
             type: :set,
             repair: true
           ) do
      {:ok, %{path: path}}
    else
      {:error, reason} -> {:stop, {:storage_initialization_failed, reason}}
      other -> {:stop, {:storage_initialization_failed, other}}
    end
  end

  @impl true
  def handle_call({:create_delivery, id, details}, _from, state) do
    with :ok <- validate_id(id),
         :ok <- validate_details(details),
         {:ok, []} <- storage(fn -> :dets.lookup(@table, id) end) do
      now = System.system_time(:millisecond)

      delivery = %{
        id: id,
        details: details,
        status: :created,
        history: [%{status: :created, at: now}]
      }

      reply =
        case storage(fn -> persist(id, delivery) end) do
          {:ok, :ok} -> {:ok, delivery}
          {:error, reason} -> {:error, reason}
        end

      {:reply, reply, state}
    else
      {:ok, [_existing]} ->
        {:reply, {:error, {:validation, :already_exists}}, state}

      {:error, {:operational, _reason}} = error ->
        {:reply, error, state}

      {:error, reason} ->
        {:reply, {:error, {:validation, reason}}, state}
    end
  end

  def handle_call({:update_status, id, status}, _from, state) do
    with :ok <- validate_id(id),
         :ok <- validate_status(status),
         {:ok, [delivery]} <- storage(fn -> :dets.lookup(@table, id) end),
         :ok <- validate_transition(delivery.status, status) do
      updated = %{
        delivery
        | status: status,
          history:
            delivery.history ++
              [%{status: status, at: System.system_time(:millisecond)}]
      }

      reply =
        case storage(fn -> persist(id, updated) end) do
          {:ok, :ok} -> {:ok, updated}
          {:error, reason} -> {:error, reason}
        end

      {:reply, reply, state}
    else
      {:ok, []} ->
        {:reply, {:error, {:validation, :not_found}}, state}

      {:error, {:operational, _reason}} = error ->
        {:reply, error, state}

      {:error, reason} ->
        {:reply, {:error, {:validation, reason}}, state}
    end
  end

  def handle_call({:get_delivery, id}, _from, state) do
    reply =
      with :ok <- validate_id(id),
           {:ok, result} <- storage(fn -> :dets.lookup(@table, id) end) do
        case result do
          [{^id, delivery}] -> {:ok, delivery}
          [] -> {:error, {:validation, :not_found}}
        end
      else
        {:error, {:operational, _reason}} = error -> error
        {:error, reason} -> {:error, {:validation, reason}}
      end

    {:reply, reply, state}
  end

  def handle_call(:list_deliveries, _from, state) do
    reply =
      case storage(fn ->
             :dets.foldl(fn {_id, delivery}, acc -> [delivery | acc] end, [], @table)
           end) do
        {:ok, deliveries} -> {:ok, Enum.sort_by(deliveries, & &1.id)}
        {:error, reason} -> {:error, reason}
      end

    {:reply, reply, state}
  end

  @impl true
  def terminate(_reason, _state) do
    try do
      :dets.close(@table)
    catch
      _, _ -> :ok
    end

    :ok
  end

  defp persist(id, delivery) do
    with :ok <- :dets.insert(@table, {id, delivery}),
         :ok <- :dets.sync(@table) do
      :ok
    end
  end

  defp storage(fun) do
    try do
      case fun.() do
        {:error, reason} -> {:error, {:operational, reason}}
        value -> {:ok, value}
      end
    rescue
      exception -> {:error, {:operational, {:exception, exception}}}
    catch
      kind, reason -> {:error, {:operational, {kind, reason}}}
    end
  end

  defp safe_call(server, request) do
    try do
      GenServer.call(server, request, 5_000)
    catch
      :exit, reason -> {:error, {:operational, {:server_unavailable, reason}}}
    end
  end

  defp validate_id(id) when is_binary(id) do
    if String.trim(id) == "", do: {:error, :invalid_id}, else: :ok
  end

  defp validate_id(_), do: {:error, :invalid_id}

  defp validate_details(details) when is_map(details), do: :ok
  defp validate_details(_), do: {:error, :details_must_be_a_map}

  defp validate_status(status) when status in @statuses, do: :ok
  defp validate_status(_), do: {:error, :invalid_status}

  defp validate_transition(:created, status) when status in [:picked_up, :failed], do: :ok
  defp validate_transition(:picked_up, status) when status in [:in_transit, :failed], do: :ok

  defp validate_transition(:in_transit, status)
       when status in [:out_for_delivery, :failed],
       do: :ok

  defp validate_transition(:out_for_delivery, status)
       when status in [:delivered, :failed],
       do: :ok

  defp validate_transition(current, next), do: {:error, {:invalid_transition, current, next}}
end

defmodule DeliveryTracker do
  use Supervisor

  alias DeliveryTracker.Server

  def start_link(opts \\ []) do
    Supervisor.start_link(
      __MODULE__,
      opts,
      name: Keyword.get(opts, :name, __MODULE__)
    )
  end

  def create_delivery(id, details \\ %{}, server \\ Server) do
    Server.create_delivery(id, details, server)
  end

  def update_status(id, status, server \\ Server) do
    Server.update_status(id, status, server)
  end

  def get_delivery(id, server \\ Server) do
    Server.get_delivery(id, server)
  end

  def list_deliveries(server \\ Server) do
    Server.list_deliveries(server)
  end

  @impl true
  def init(opts) do
    server_name = Keyword.get(opts, :server_name, Server)
    path = Keyword.get(opts, :path, "delivery_tracker.dets")

    children = [
      {Server, path: path, name: server_name}
    ]

    Supervisor.init(children,
      strategy: :one_for_one,
      max_restarts: 10,
      max_seconds: 5
    )
  end
end
