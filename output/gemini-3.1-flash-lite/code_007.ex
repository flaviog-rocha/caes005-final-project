# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de usuários e sessões, atendendo somente aos seguintes requisitos:
#
# 1. Permitir limitar a quantidade de operações simultâneas.
# 2. Detectar encerramentos inesperados de processos.
# 3. Fazer uso de macros.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - Logger
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule UserSessionManager do
  @moduledoc """
  Gerenciador de sessões com limite de concorrência e monitoramento de processos.
  """
  require Logger

  defmodule State do
    defstruct max_concurrency: 0, current_count: 0, monitors: %{}
  end

  defmacro def_session_limit(max_ops) do
    quote do
      def start_link() do
        GenServer.start_link(__MODULE__, unquote(max_ops), name: __MODULE__)
      end
    end
  end

  use GenServer
  def_session_limit(5)

  def init(max_concurrency) do
    {:ok, %State{max_concurrency: max_concurrency}}
  end

  def acquire_session(user_id) do
    GenServer.call(__MODULE__, {:acquire, user_id})
  end

  def release_session(user_id) do
    GenServer.cast(__MODULE__, {:release, user_id})
  end

  def handle_call({:acquire, user_id}, {pid, _ref}, state) do
    if state.current_count < state.max_concurrency do
      ref = Process.monitor(pid)
      new_monitors = Map.put(state.monitors, ref, {user_id, pid})
      
      Logger.info("Sessão iniciada para usuário: #{user_id}")
      
      {:reply, :ok, %{state | current_count: state.current_count + 1, monitors: new_monitors}}
    else
      Logger.warn("Limite de concorrência atingido para o usuário: #{user_id}")
      {:reply, {:error, :limit_reached}, state}
    end
  end

  def handle_cast({:release, user_id}, state) do
    new_monitors = Enum.reject(state.monitors, fn {_, {uid, _}} -> uid == user_id end) |> Map.new()
    {:noreply, %{state | current_count: max(0, state.current_count - 1), monitors: new_monitors}}
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, state) do
    case Map.pop(state.monitors, ref) do
      {{user_id, _}, new_monitors} ->
        Logger.error("Processo de sessão para #{user_id} encerrou inesperadamente: #{inspect(reason)}")
        {:noreply, %{state | current_count: max(0, state.current_count - 1), monitors: new_monitors}}
      {nil, _} ->
        {:noreply, state}
    end
  end
end
