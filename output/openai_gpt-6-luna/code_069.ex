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
  Gerenciamento de entregas com processamento em fluxo, persistência em DETS,
  novas tentativas e monitoramento de encerramentos inesperados.

  O armazenamento DETS usa uma tabela e um nome de supervisor fixos; execute
  apenas uma instância deste módulo por nó.
  """

  use Supervisor

  @store DeliveryTracker.Store
  @watcher DeliveryTracker.Watcher
  @table :delivery_tracker_dets

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    notify_to = Keyword.get(opts, :notify_to)
    file = Keyword.get(opts, :file, "delivery_tracker.dets")

    children = [
      %{
        id: @store,
        start:
          {Agent, :start_link,
           [
             fn -> init_store(file, notify_to, Keyword.get(opts, :notify_on, [:delivered, :exception])) end,
             [name: @store]
           ]},
        restart: :permanent,
        shutdown: 5_000,
        type: :worker
      },
      %{
        id: @watcher,
        start: {DeliveryTracker.Watcher, :start_link, [notify_to]},
        restart: :permanent,
        shutdown: 5_000,
        type: :worker
      }
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  @doc """
  Processa qualquer Enumerable sem materializá-lo integralmente na memória.

  `:processor` pode ser uma função que recebe cada item e retorna
  `{:ok, entrega}` ou `{:error, motivo}`. Por padrão, cada item é usado
  diretamente como entrega. Retorna contagens, sem acumular itens com falha.
  """
  def process_stream(enumerable, opts \\ []) do
    processor = Keyword.get(opts, :processor, fn item -> {:ok, item} end)
    max_retries = Keyword.get(opts, :max_retries, 3)
    backoff_ms = Keyword.get(opts, :backoff_ms, 100)

    Enum.reduce(enumerable, %{processed: 0, failed: 0}, fn item, counts ->
      case process_with_retry(item, processor, max_retries, backoff_ms, 0) do
        {:ok, delivery} ->
          case put_delivery(delivery) do
            :ok -> %{counts | processed: counts.processed + 1}
            {:error, _reason} -> %{counts | failed: counts.failed + 1}
          end

        {:error, _reason} ->
          %{counts | failed: counts.failed + 1}
      end
    end)
  end

  @doc "Salva uma entrega e gera notificações se seu status atingir uma condição."
  def put_delivery(%{id: id} = delivery) do
    Agent.get_and_update(@store, fn state ->
      previous_status =
        case dets_lookup(state.table, {:delivery, id}) do
          {:ok, previous} -> Map.get(previous, :status)
          :error -> nil
        end

      case :dets.insert(state.table, {{:delivery, id}, delivery}) do
        :ok ->
          status = Map.get(delivery, :status)

          if status != previous_status and status in state.notify_on do
            notify_subscribers(state.subscribers, {:delivery_status, delivery})
          end

          {:ok, state}

        {:error, reason} ->
          {{:error, reason}, state}
      end
    end)
  end

  def put_delivery(_delivery), do: {:error, :delivery_id_required}

  @doc "Recupera uma entrega pelo identificador."
  def get_delivery(id) do
    Agent.get(@store, fn state ->
      case dets_lookup(state.table, {:delivery, id}) do
        {:ok, delivery} -> {:ok, delivery}
        :error -> :error
      end
    end)
  end

  @doc "Inscreve o processo chamador para receber notificações de status."
  def subscribe(pid \\ self()) when is_pid(pid) do
    Agent.update(@store, fn state ->
      %{state | subscribers: MapSet.put(state.subscribers, pid)}
    end)
  end

  defp init_store(file, notify_to, notify_on) do
    {:ok, table} =
      case :dets.open_file(@table, file: String.to_charlist(file), type: :set, repair: true) do
        {:ok, table} -> {:ok, table}
        {:error, reason} -> raise "não foi possível abrir o armazenamento DETS: #{inspect(reason)}"
      end

    subscribers =
      case notify_to do
        pid when is_pid(pid) -> MapSet.new([pid])
        _ -> MapSet.new()
      end

    %{table: table, notify_on: notify_on, subscribers: subscribers}
  end

  defp dets_lookup(table, key) do
    case :dets.lookup(table, key) do
      [{^key, value}] -> {:ok, value}
      [] -> :error
    end
  end

  defp notify_subscribers(subscribers, message) do
    Enum.each(subscribers, fn pid ->
      if Process.alive?(pid), do: send(pid, {:delivery_tracker, message})
    end)
  end

  defp process_with_retry(item, processor, max_retries, backoff_ms, attempt) do
    result =
      try do
        processor.(item)
      rescue
        error -> {:error, error}
      catch
        kind, reason -> {:error, {kind, reason}}
      end

    case result do
      {:ok, delivery} ->
        {:ok, delivery}

      {:error, _reason} = error when attempt >= max_retries ->
        error

      {:error, _reason} ->
        if backoff_ms > 0, do: Process.sleep(backoff_ms)
        process_with_retry(item, processor, max_retries, backoff_ms, attempt + 1)

      delivery when is_map(delivery) ->
        {:ok, delivery}

      other ->
        {:error, {:invalid_processor_result, other}}
    end
  end
end

defmodule DeliveryTracker.Watcher do
  @moduledoc false
  use GenServer

  @agent DeliveryTracker.Store

  def start_link(notify_to) do
    GenServer.start_link(__MODULE__, notify_to, name: __MODULE__)
  end

  @impl true
  def init(notify_to) do
    {:ok, monitor_agent(%{notify_to: notify_to, monitor: nil})}
  end

  @impl true
  def handle_info({:DOWN, monitor, :process, _pid, reason}, %{monitor: monitor} = state) do
    if is_pid(state.notify_to) do
      send(state.notify_to, {:delivery_tracker, {:process_down, @agent, reason}})
    end

    Process.send_after(self(), :find_agent, 50)
    {:noreply, %{state | monitor: nil}}
  end

  def handle_info(:find_agent, state) do
    {:noreply, monitor_agent(state)}
  end

  defp monitor_agent(%{monitor: nil} = state) do
    case Process.whereis(@agent) do
      nil ->
        Process.send_after(self(), :find_agent, 50)
        state

      pid ->
        %{state | monitor: Process.monitor(pid)}
    end
  end

  defp monitor_agent(state), do: state
end
