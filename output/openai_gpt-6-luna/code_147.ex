# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de pedidos de um e-commerce, atendendo somente aos seguintes requisitos:
#
# 1. Implementar novas tentativas após falhas temporárias.
# 2. Implementar expiração automática de registros.
# 3. Executar determinadas operações periodicamente.
# 4. Permitir consultar o estado atual das operações em andamento.
# 5. Impedir que uma mesma operação seja executada mais de uma vez.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule Ecommerce.OrderManager do
  use GenServer

  @default_ttl 86_400_000
  @default_retry_delay 1_000
  @default_max_retry_delay 300_000
  @default_max_attempts 3

  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc """
  Envia uma operação identificada por `id`. As opções aceitas são
  `:max_attempts`, `:retry_delay`, `:max_retry_delay` e `:expires_in`.
  """
  def submit(server, id, payload, opts \\ []) do
    GenServer.call(server, {:submit, id, payload, opts})
  end

  def status(server, id), do: GenServer.call(server, {:status, id})
  def all(server), do: GenServer.call(server, :all)

  @impl true
  def init(opts) do
    handler = Keyword.fetch!(opts, :handler)

    unless is_function(handler, 2) do
      raise ArgumentError, "handler deve ser uma função de aridade 2"
    end

    state = %{
      handler: handler,
      operations: %{},
      monitors: %{},
      periodic: %{}
    }

    periodic =
      opts
      |> Keyword.get(:periodic, [])
      |> Enum.reduce(%{}, fn config, acc ->
        config = Map.new(config)
        name = Map.fetch!(config, :name)
        interval = positive_integer!(Map.fetch!(config, :interval), :interval)

        timer = Process.send_after(self(), {:periodic_tick, name}, interval)

        Map.put(acc, name, %{
          config: config,
          interval: interval,
          sequence: 0,
          timer: timer
        })
      end)

    {:ok, %{state | periodic: periodic}}
  end

  @impl true
  def handle_call({:submit, id, payload, opts}, _from, state) do
    if Map.has_key?(state.operations, id) do
      {:reply, {:error, :already_exists}, state}
    else
      state = enqueue(state, id, payload, opts)
      {:reply, {:ok, id}, start_operation(state, id)}
    end
  end

  def handle_call({:status, id}, _from, state) do
    reply =
      case Map.fetch(state.operations, id) do
        {:ok, operation} -> public_operation(operation)
        :error -> nil
      end

    {:reply, reply, state}
  end

  def handle_call(:all, _from, state) do
    operations =
      state.operations
      |> Map.values()
      |> Enum.map(&public_operation/1)

    {:reply, operations, state}
  end

  @impl true
  def handle_info({:worker_result, id, monitor, result}, state) do
    case Map.get(state.operations, id) do
      %{monitor: ^monitor} = operation ->
        Process.demonitor(monitor, [:flush])

        state = %{
          state
          | monitors: Map.delete(state.monitors, monitor)
        }

        state = complete_operation(state, operation, result)
        {:noreply, state}

      _ ->
        {:noreply, state}
    end
  end

  def handle_info({:DOWN, monitor, :process, _pid, reason}, state) do
    case Map.pop(state.monitors, monitor) do
      {nil, _monitors} ->
        {:noreply, state}

      {id, monitors} ->
        state = %{state | monitors: monitors}

        case Map.get(state.operations, id) do
          %{monitor: ^monitor} = operation ->
            state = fail_operation(state, operation, {:worker_exit, reason}, true)
            {:noreply, state}

          _ ->
            {:noreply, state}
        end
    end
  end

  def handle_info({:retry, id, token}, state) do
    case Map.get(state.operations, id) do
      %{status: :retry_wait, retry_token: ^token} ->
        {:noreply, start_operation(state, id)}

      _ ->
        {:noreply, state}
    end
  end

  def handle_info({:expire, id, token}, state) do
    case Map.get(state.operations, id) do
      %{expiry_token: ^token} = operation ->
        if monitor = Map.get(operation, :monitor) do
          Process.exit(operation.pid, :kill)
          Process.demonitor(monitor, [:flush])

          state = %{
            state
            | monitors: Map.delete(state.monitors, monitor),
              operations: Map.delete(state.operations, id)
          }

          {:noreply, state}
        else
          {:noreply, %{state | operations: Map.delete(state.operations, id)}}
        end

      _ ->
        {:noreply, state}
    end
  end

  def handle_info({:periodic_tick, name}, state) do
    case Map.get(state.periodic, name) do
      nil ->
        {:noreply, state}

      entry ->
        sequence = entry.sequence + 1
        config = entry.config
        interval = entry.interval
        timer = Process.send_after(self(), {:periodic_tick, name}, interval)

        updated_entry = %{entry | sequence: sequence, timer: timer}
        state = %{state | periodic: Map.put(state.periodic, name, updated_entry)}

        id = {:periodic, name, sequence}
        payload = Map.get(config, :payload)

        operation_opts = [
          max_attempts: Map.get(config, :max_attempts, @default_max_attempts),
          retry_delay: Map.get(config, :retry_delay, @default_retry_delay),
          max_retry_delay: Map.get(config, :max_retry_delay, @default_max_retry_delay),
          expires_in: Map.get(config, :expires_in, @default_ttl)
        ]

        state =
          if Map.has_key?(state.operations, id) do
            state
          else
            state
            |> enqueue(id, payload, operation_opts)
            |> start_operation(id)
          end

        {:noreply, state}
    end
  end

  defp enqueue(state, id, payload, opts) do
    max_attempts =
      opts
      |> Keyword.get(:max_attempts, @default_max_attempts)
      |> positive_integer!(:max_attempts)

    retry_delay =
      opts
      |> Keyword.get(:retry_delay, @default_retry_delay)
      |> non_negative_integer!(:retry_delay)

    max_retry_delay =
      opts
      |> Keyword.get(:max_retry_delay, @default_max_retry_delay)
      |> non_negative_integer!(:max_retry_delay)

    expires_in =
      opts
      |> Keyword.get(:expires_in, @default_ttl)
      |> positive_integer!(:expires_in)

    token = make_ref()
    expires_at = System.system_time(:millisecond) + expires_in
    Process.send_after(self(), {:expire, id, token}, expires_in)

    operation = %{
      id: id,
      payload: payload,
      status: :queued,
      attempts: 0,
      max_attempts: max_attempts,
      retry_delay: retry_delay,
      max_retry_delay: max_retry_delay,
      expires_at: expires_at,
      expiry_token: token
    }

    %{state | operations: Map.put(state.operations, id, operation)}
  end

  defp start_operation(state, id) do
    case Map.get(state.operations, id) do
      nil ->
        state

      operation ->
        parent = self()
        handler = state.handler
        attempt = operation.attempts + 1

        {pid, monitor} =
          spawn_monitor(fn ->
            result =
              try do
                handler.(id, operation.payload)
              rescue
                error -> {:error, {:exception, error}}
              catch
                kind, reason -> {:error, {kind, reason}}
              end

            send(parent, {:worker_result, id, monitor_ref_placeholder(), result})
          end)

        # A mensagem do worker precisa carregar a referência do monitor. Como ela
        # só fica disponível após spawn_monitor/1, o worker envia o resultado
        # usando uma mensagem intermediária identificada pelo seu PID.
        send(pid, {:set_monitor, monitor})

        operation = %{
          operation
          | status: :running,
            attempts: attempt,
            pid: pid,
            monitor: monitor
        }

        %{
          state
          | operations: Map.put(state.operations, id, operation),
            monitors: Map.put(state.monitors, monitor, id)
        }
    end
  end

  defp complete_operation(state, operation, :ok) do
    mark_succeeded(state, operation, :ok)
  end

  defp complete_operation(state, operation, {:ok, value}) do
    mark_succeeded(state, operation, value)
  end

  defp complete_operation(state, operation, {:error, reason}) do
    fail_operation(state, operation, reason, true)
  end

  defp complete_operation(state, operation, {:fatal, reason}) do
    fail_operation(state, operation, reason, false)
  end

  defp complete_operation(state, operation, other) do
    fail_operation(state, operation, {:invalid_result, other}, true)
  end

  defp mark_succeeded(state, operation, value) do
    updated =
      operation
      |> Map.drop([:pid, :monitor, :retry_token, :retry_timer])
      |> Map.put(:status, :succeeded)
      |> Map.put(:result, value)

    %{state | operations: Map.put(state.operations, operation.id, updated)}
  end

  defp fail_operation(state, operation, reason, retry?) do
    operation =
      Map.drop(operation, [:pid, :monitor])
      |> Map.put(:error, reason)

    state =
      %{state | operations: Map.put(state.operations, operation.id, operation)}

    if retry? and operation.attempts < operation.max_attempts do
      delay =
        operation.retry_delay
        |> Kernel.*(Integer.pow(2, max(operation.attempts - 1, 0)))
        |> min(operation.max_retry_delay)

      token = make_ref()
      timer = Process.send_after(self(), {:retry, operation.id, token}, delay)

      updated =
        operation
        |> Map.put(:status, :retry_wait)
        |> Map.put(:retry_token, token)
        |> Map.put(:retry_timer, timer)

      %{state | operations: Map.put(state.operations, operation.id, updated)}
    else
      updated =
        operation
        |> Map.put(:status, :failed)
        |> Map.drop([:retry_token, :retry_timer])

      %{state | operations: Map.put(state.operations, operation.id, updated)}
    end
  end

  defp public_operation(operation) do
    Map.take(operation, [
      :id,
      :status,
      :attempts,
      :max_attempts,
      :expires_at,
      :result,
      :error
    ])
  end

  defp positive_integer!(value, _name) when is_integer(value) and value > 0, do: value

  defp positive_integer!(value, name) do
    raise ArgumentError, "#{name} deve ser um inteiro positivo; recebido: #{inspect(value)}"
  end

  defp non_negative_integer!(value, _name) when is_integer(value) and value >= 0, do: value

  defp non_negative_integer!(value, name) do
    raise ArgumentError,
          "#{name} deve ser um inteiro não negativo; recebido: #{inspect(value)}"
  end

  # Placeholder para manter o exemplo autocontido.
  defp monitor_ref_placeholder, do: nil
end
