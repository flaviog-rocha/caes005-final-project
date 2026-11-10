# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de transações financeiras, atendendo somente aos seguintes requisitos:
#
# 1. Fazer uso de macros.
# 2. Permitir consultar o estado atual das operações em andamento.
# 3. Executar determinadas operações periodicamente.
# 4. Manter um histórico das operações realizadas.
# 5. Recuperar processos após falhas inesperadas.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule TransactionSystem do
  @moduledoc """
  Sistema de gerenciamento de transações usando GenServer,
  macros para DSL e Supervisor para tolerância a falhas.
  """

  # 1. Uso de Macros para definir operações de forma declarativa
  defmacro def_operation(name, do: block) do
    quote do
      def unquote(name)(amount) do
        GenServer.call(__MODULE__, {:execute, unquote(name), amount, fn -> unquote(block) end})
      end
    end
  end

  use GenServer
  use Supervisor

  # --- API Pública ---

  def start_link(_), do: GenServer.start_link(__MODULE__, %{pending: [], history: []}, name: __MODULE__)

  def get_state, do: GenServer.call(__MODULE__, :get_state)

  # --- Macros aplicadas ---
  def_operation(deposit) do: :ok
  def_operation(withdraw) do: :ok

  # --- Callbacks do GenServer ---

  @impl true
  def init(state) do
    # 3. Executar operações periodicamente (a cada 60s)
    :timer.send_interval(60_000, :perform_periodic_cleanup)
    {:ok, state}
  end

  @impl true
  def handle_call({:execute, type, amount, func}, _from, state) do
    result = func.()
    new_history = [%{type: type, amount: amount, timestamp: System.system_time()} | state.history]
    {:reply, result, %{state | history: new_history}}
  end

  @impl true
  def handle_call(:get_state, _from, state), do: {:reply, state, state}

  @impl true
  def handle_info(:perform_periodic_cleanup, state) do
    # Lógica de limpeza de logs antigos
    {:noreply, %{state | history: Enum.take(state.history, 100)}}
  end

  # --- Estrutura de Supervisão ---
  # 5. Recuperação de processos (Supervisor garante reinicialização)
  def start_supervisor do
    children = [
      {TransactionSystem, []}
    ]
    Supervisor.start_link(children, strategy: :one_for_one)
  end
end

# Exemplo de uso:
# TransactionSystem.start_supervisor()
# TransactionSystem.deposit(100)
# TransactionSystem.get_state()
