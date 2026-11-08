# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de gerenciamento de notificações, atendendo somente aos seguintes requisitos:
#
# 1. Gerar notificações quando determinadas condições forem atingidas.
# 2. Quando fizer sentido para o domínio, considerar a comunicação com um banco de dados fictício e, opcionalmente, o uso de Ecto.Migration, sem tornar isso obrigatório.
# 3. Diferenciar erros de validação de falhas operacionais.
# 4. Detectar encerramentos inesperados de processos.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule NotificationSystem do
  @moduledoc """
  Sistema simples de notificações com armazenamento em memória.
  """

  def start_link(opts \\ []) do
    NotificationSystem.Supervisor.start_link(opts)
  end

  def add_rule(name, threshold) do
    GenServer.call(NotificationSystem.Server, {:add_rule, name, threshold})
  end

  def observe(name, value) do
    GenServer.call(NotificationSystem.Server, {:observe, name, value})
  end

  def notifications do
    GenServer.call(NotificationSystem.Server, :notifications)
  end

  def status do
    GenServer.call(NotificationSystem.Server, :status)
  end
end

defmodule NotificationSystem.Supervisor do
  use Supervisor

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    children = [
      NotificationSystem.Store,
      NotificationSystem.Server
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end

defmodule NotificationSystem.Store do
  use GenServer

  def start_link(_opts \\ []) do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  @impl true
  def init(_opts), do: {:ok, []}

  @impl true
  def handle_call({:save, notification}, _from, notifications) do
    {:reply, {:ok, notification}, [notification | notifications]}
  end

  def handle_call(:all, _from, notifications) do
    {:reply, {:ok, Enum.reverse(notifications)}, notifications}
  end
end

defmodule NotificationSystem.Server do
  use GenServer

  @store_retry_ms 100

  def start_link(_opts \\ []) do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    {:ok, monitor_store(%{rules: %{}, store_ref: nil, last_failure: nil})}
  end

  @impl true
  def handle_call({:add_rule, name, threshold}, _from, state) do
    cond do
      not valid_name?(name) ->
        {:reply, {:error, {:validation, :invalid_rule_name}}, state}

      not is_number(threshold) ->
        {:reply, {:error, {:validation, :threshold_must_be_numeric}}, state}

      true ->
        rule = %{threshold: threshold, active?: false}
        {:reply, :ok, put_in(state, [:rules, name], rule)}
    end
  end

  def handle_call({:observe, name, value}, _from, state) do
    cond do
      not valid_name?(name) ->
        {:reply, {:error, {:validation, :invalid_rule_name}}, state}

      not is_number(value) ->
        {:reply, {:error, {:validation, :value_must_be_numeric}}, state}

      not Map.has_key?(state.rules, name) ->
        {:reply, {:error, {:validation, :unknown_rule}}, state}

      true ->
        evaluate_rule(name, value, state)
    end
  end

  def handle_call(:notifications, _from, state) do
    case store_call(:all) do
      {:ok, notifications} ->
        {:reply, {:ok, notifications}, %{state | last_failure: nil}}

      {:error, reason} ->
        {:reply, {:error, {:operational, reason}}, record_failure(state, reason)}
    end
  end

  def handle_call(:status, _from, state) do
    {:reply,
     %{
       rules: state.rules,
       store_available?: is_pid(Process.whereis(NotificationSystem.Store)),
       last_failure: state.last_failure
     }, state}
  end

  @impl true
  def handle_info(
        {:DOWN, ref, :process, _pid, reason},
        %{store_ref: ref} = state
      ) do
    failure = %{
      kind: :unexpected_process_shutdown,
      process: NotificationSystem.Store,
      reason: reason
    }

    Process.send_after(self(), :check_store, @store_retry_ms)
    {:noreply, %{state | store_ref: nil, last_failure: failure}}
  end

  def handle_info(:check_store, state) do
    {:noreply, monitor_store(state)}
  end

  def handle_info(_message, state), do: {:noreply, state}

  defp evaluate_rule(name, value, state) do
    rule = Map.fetch!(state.rules, name)

    if value >= rule.threshold do
      if rule.active? do
        {:reply, {:ok, :already_triggered}, state}
      else
        notification = %{
          id: System.unique_integer([:positive, :monotonic]),
          rule: name,
          value: value,
          threshold: rule.threshold,
          inserted_at: DateTime.utc_now()
        }

        case store_call({:save, notification}) do
          {:ok, saved_notification} ->
            updated_rule = %{rule | active?: true}

            {:reply, {:ok, saved_notification},
             state
             |> put_in([:rules, name], updated_rule)
             |> Map.put(:last_failure, nil)}

          {:error, reason} ->
            {:reply, {:error, {:operational, reason}}, record_failure(state, reason)}
        end
      end
    else
      updated_rule = %{rule | active?: false}
      {:reply, {:ok, :not_triggered}, put_in(state, [:rules, name], updated_rule)}
    end
  end

  defp store_call(message) do
    try do
      GenServer.call(NotificationSystem.Store, message, 5_000)
    catch
      :exit, reason -> {:error, reason}
    end
  end

  defp monitor_store(state) do
    case Process.whereis(NotificationSystem.Store) do
      pid when is_pid(pid) ->
        if state.store_ref do
          state
        else
          %{state | store_ref: Process.monitor(pid)}
        end

      _ ->
        Process.send_after(self(), :check_store, @store_retry_ms)
        state
    end
  end

  defp record_failure(state, reason) do
    %{state | last_failure: %{kind: :operational_failure, reason: reason}}
  end

  defp valid_name?(name), do: is_binary(name) and String.trim(name) != ""
end
