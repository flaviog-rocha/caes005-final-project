# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de entregas e rastreamento, atendendo somente aos seguintes requisitos:
#
# 1. Gerar notificações quando determinadas condições forem atingidas.
# 2. Processar grandes volumes de dados sem carregá-los integralmente na memória.
# 3. Recuperar processos após falhas inesperadas.
# 4. Implementar novas tentativas após falhas temporárias.
# 5. Detectar encerramentos inesperados de processos.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - Agent
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule DeliveryTracker do
  @moduledoc """
  A disk-backed delivery tracker. Input enumerables are consumed incrementally,
  while an Agent maintains bounded in-memory counters.
  """

  use Supervisor
  require Logger

  @store_name DeliveryTracker.Store

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: Keyword.get(opts, :name))
  end

  @impl true
  def init(opts) do
    path = Keyword.get(opts, :path, "delivery_tracker.log")
    notify_fun = Keyword.get(opts, :notify, &default_notify/1)
    condition = Keyword.get(opts, :notify_when, &default_condition/1)

    children = [
      %{
        id: DeliveryTracker.Store,
        start:
          {Agent, :start_link,
           [fn -> load_state(path, notify_fun, condition) end, [name: @store_name]]},
        restart: :permanent
      },
      {DeliveryTracker.Monitor, notify_fun}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  @doc "Records a delivery status and emits a notification when its condition is met."
  def track(id, status, metadata \\ %{}) do
    Agent.get_and_update(@store_name, fn state ->
      record = %{
        id: id,
        status: status,
        metadata: metadata,
        at: System.system_time(:millisecond)
      }

      case append_record(state.path, record) do
        :ok ->
          bucket = status_bucket(status)
          counts = Map.update!(state.counts, bucket, &(&1 + 1))
          {{:ok, record, state.notify_fun, state.condition}, %{state | counts: counts}}

        {:error, reason} ->
          {{:error, reason}, state}
      end
    end)
    |> case do
      {:ok, record, notify_fun, condition} ->
        if safely_matches?(condition, record), do: safely_notify(notify_fun, record)
        {:ok, record}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Processes an enumerable incrementally. The processor should return
  `{:ok, status}`, `{:ok, status, metadata}`, or `{:error, reason}`.
  Temporary failures can be returned as `{:error, {:temporary, reason}}`.
  """
  def process_stream(enumerable, processor, opts \\ []) when is_function(processor, 1) do
    id_fun = Keyword.get(opts, :id, &default_id/1)
    max_retries = Keyword.get(opts, :max_retries, 3)
    backoff = Keyword.get(opts, :backoff, &default_backoff/1)

    Enum.reduce(enumerable, %{processed: 0, failed: 0}, fn item, acc ->
      id = id_fun.(item)

      result =
        run_with_retries(item, processor, max_retries, backoff, 0)

      case result do
        {:ok, status, metadata} ->
          _ = track(id, status, metadata)
          %{acc | processed: acc.processed + 1}

        {:error, reason} ->
          _ = track(id, :failed, %{error: reason})
          %{acc | processed: acc.processed + 1, failed: acc.failed + 1}
      end
    end)
  end

  @doc "Returns bounded in-memory counters."
  def stats do
    Agent.get(@store_name, & &1.counts)
  end

  @doc "Looks up the latest record for an ID by streaming the on-disk log."
  def latest(id, path \\ "delivery_tracker.log") do
    if File.exists?(path) do
      path
      |> File.stream!([], :line)
      |> Enum.reduce(nil, fn line, latest ->
        case decode_record(line) do
          {:ok, %{id: ^id} = record} -> record
          _ -> latest
        end
      end)
    else
      nil
    end
  end

  defp run_with_retries(item, processor, max_retries, backoff, attempt) do
    result =
      try do
        processor.(item)
      rescue
        exception -> {:error, {:exception, exception}}
      catch
        kind, reason -> {:error, {:caught, kind, reason}}
      end

    case result do
      {:ok, status} ->
        {:ok, status, %{}}

      {:ok, status, metadata} ->
        {:ok, status, metadata}

      {:error, {:temporary, reason}} when attempt < max_retries ->
        Process.sleep(backoff.(attempt))
        run_with_retries(item, processor, max_retries, backoff, attempt + 1)

      {:error, :temporary} when attempt < max_retries ->
        Process.sleep(backoff.(attempt))
        run_with_retries(item, processor, max_retries, backoff, attempt + 1)

      {:error, reason} ->
        {:error, reason}

      other ->
        {:error, {:invalid_processor_result, other}}
    end
  end

  defp default_backoff(attempt), do: min(100 * trunc(:math.pow(2, attempt)), 5_000)

  defp default_id(%{id: id}), do: id
  defp default_id(item), do: item

  defp default_condition(%{status: status}), do: status in [:delivered, :failed]

  defp default_notify(event) do
    Logger.info("Delivery notification: #{inspect(event)}")
  end

  defp safely_matches?(condition, record) do
    try do
      condition.(record)
    rescue
      _ -> false
    catch
      _, _ -> false
    end
  end

  defp safely_notify(notify_fun, event) do
    try do
      notify_fun.(event)
    rescue
      exception -> Logger.warning("Delivery notification failed: #{inspect(exception)}")
    catch
      kind, reason ->
        Logger.warning("Delivery notification failed: #{inspect({kind, reason})}")
    end
  end

  defp status_bucket(:delivered), do: :delivered
  defp status_bucket(:failed), do: :failed
  defp status_bucket(_), do: :other

  defp load_state(path, notify_fun, condition) do
    counts = %{delivered: 0, failed: 0, other: 0}

    counts =
      if File.exists?(path) do
        path
        |> File.stream!([], :line)
        |> Enum.reduce(counts, fn line, acc ->
          case decode_record(line) do
            {:ok, record} ->
              Map.update!(acc, status_bucket(record.status), &(&1 + 1))

            :error ->
              acc
          end
        end)
      else
        counts
      end

    %{path: path, counts: counts, notify_fun: notify_fun, condition: condition}
  end

  defp append_record(path, record) do
    encoded = :erlang.term_to_binary(record) |> Base.encode64()

    case File.open(path, [:append, :binary]) do
      {:ok, file} ->
        result =
          with :ok <- IO.binwrite(file, [encoded, "\n"]),
               :ok <- :file.sync(file) do
            :ok
          end

        File.close(file)
        result

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp decode_record(line) do
    with {:ok, encoded} <- Base.decode64(String.trim(line)),
         {:ok, record} <- safe_decode_term(encoded) do
      {:ok, record}
    else
      _ -> :error
    end
  end

  defp safe_decode_term(binary) do
    try do
      {:ok, :erlang.binary_to_term(binary, [:safe])}
    rescue
      _ -> :error
    end
  end
end

defmodule DeliveryTracker.Monitor do
  use GenServer
  require Logger

  @store_name DeliveryTracker.Store

  def start_link(notify_fun) do
    GenServer.start_link(__MODULE__, notify_fun)
  end

  @impl true
  def init(notify_fun) do
    state = %{notify_fun: notify_fun, ref: nil, pid: nil}
    {:ok, attach_or_wait(state)}
  end

  @impl true
  def handle_info({:DOWN, ref, :process, pid, reason}, %{ref: ref, pid: pid} = state) do
    event = %{type: :process_down, process: @store_name, reason: reason}
    Logger.warning("Delivery tracker Agent terminated unexpectedly: #{inspect(reason)}")
    notify_safely(state.notify_fun, event)
    Process.send_after(self(), :find_replacement, 25)
    {:noreply, %{state | ref: nil, pid: nil}}
  end

  def handle_info(:find_replacement, state), do: {:noreply, attach_or_wait(state)}
  def handle_info(_message, state), do: {:noreply, state}

  defp attach_or_wait(state) do
    case Process.whereis(@store_name) do
      nil ->
        Process.send_after(self(), :find_replacement, 25)
        state

      pid ->
        %{state | pid: pid, ref: Process.monitor(pid)}
    end
  end

  defp notify_safely(notify_fun, event) do
    try do
      notify_fun.(event)
    rescue
      _ -> :ok
    catch
      _, _ -> :ok
    end
  end
end
