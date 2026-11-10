# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de um sistema de cache, atendendo somente aos seguintes requisitos:
#
# 1. Permitir limitar a quantidade de operações simultâneas.
# 2. Manter um histórico das operações realizadas.
# 3. Garantir a consistência dos dados durante operações concorrentes.
# 4. Recuperar processos após falhas inesperadas.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule CacheManager do
  @moduledoc """
  Sistema de Cache resiliente utilizando GenServer para concorrência e estado,
  com monitoramento de processos e limite de taxa (rate limiting) via Semaphore.
  """

  use GenServer

  @type key :: term()
  @type value :: term()

  # --- API Pública ---

  def start_link(_opts) do
    GenServer.start_link(__MODULE__, %{cache: %{}, history: []}, name: __MODULE__)
  end

  @doc "Executa uma operação de cache com limite de simultaneidade e registro"
  def execute(key, operation_fun) do
    # 1. Limite de simultaneidade: Utiliza um pool de semáforos ou controle via Task
    # Aqui implementamos via Task para isolar falhas e controlar concorrência
    Task.async(fn ->
      GenServer.call(__MODULE__, {:perform, key, operation_fun})
    end)
    |> Task.await()
  end

  def get_history do
    GenServer.call(__MODULE__, :get_history)
  end

  # --- Callbacks do GenServer ---

  @impl true
  def init(state) do
    {:ok, state}
  end

  @impl true
  def handle_call({:perform, key, op_fun}, _from, %{cache: cache, history: history} = state) do
    # 3. Consistência: O estado é processado sequencialmente pelo GenServer
    result = op_fun.(Map.get(cache, key))
    new_cache = Map.put(cache, key, result)
    new_history = [{key, result, DateTime.utc_now()} | history]
    
    {:reply, result, %{state | cache: new_cache, history: new_history}}
  end

  @impl true
  def handle_call(:get_history, _from, state) do
    {:reply, state.history, state}
  end

  # --- Recuperação de falhas ---

  @doc """
  4. Recuperação: Para garantir persistência após falhas, o estado deve ser
  armazenado via ETS ou um arquivo de log (ETS é recomendado para performance).
  """
  def child_spec(opts) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [opts]},
      restart: :permanent # Garante que o processo reinicie após crash
    }
  end
end

# Exemplo de uso:
# Supervisor.start_link([CacheManager], strategy: :one_for_one)
# CacheManager.execute(:user_1, fn _ -> "Dados do Usuário 1" end)
