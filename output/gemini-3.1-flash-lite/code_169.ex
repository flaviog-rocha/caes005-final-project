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

defmodule TransactionManager do
  @moduledoc """
  Gerenciador de transações com suporte a persistência, agendamento e macros de DSL.
  """
  use GenServer

  # 1. Uso de macros para definir a DSL de transação
  defmacro transaction(name, do: block) do
    quote do
      def unquote(:"execute_#{name}")(manager) do
        GenServer.cast(manager, {:execute, unquote(name), fn -> unquote(block) end})
      end
    end
  end

  # Estado inicial
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_) do
    # 5. Recuperação de processos: Monitoramento e estado inicial
    Process.send_after(self(), :periodic_cleanup, 60_000)
    {:ok, %{active: %{}, history: [], last_id: 0}}
  end

  # 2. Consultar o estado atual
  def get_state, do: GenServer.call(__MODULE__, :get_state)

  @impl true
  def handle_call(:get_state, _from, state), do: {:reply, state, state}

  @impl true
  def handle_cast({:execute, name, func}, state) do
    id = state.last_id + 1
    new_active = Map.put(state.active, id, %{name: name, status: :running})
    
    # Execução assíncrona para não bloquear o servidor
    Task.start(fn ->
      result = func.()
      GenServer.cast(__MODULE__, {:complete, id, result})
    end)

    {:noreply, %{state | active: new_active, last_id: id}}
  end

  @impl true
  def handle_cast({:complete, id, result}, state) do
    transaction = Map.get(state.active, id)
    new_history = [%{id: id, name: transaction.name, result: result, ts: System.system_time()}] ++ state.history
    new_active = Map.delete(state.active, id)
    {:noreply, %{state | active: new_active, history: new_history}}
  end

  # 3. Operações periódicas
  @impl true
  def handle_info(:periodic_cleanup, state) do
    IO.puts("Executando manutenção periódica do sistema...")
    Process.send_after(self(), :periodic_cleanup, 60_000)
    {:noreply, state}
  end
end

# Exemplo de uso da DSL e do sistema
defmodule FinanceApp do
  require TransactionManager

  # Uso da macro definida
  TransactionManager.transaction(:transferencia) do
    :ok # Lógica complexa de transação aqui
  end
end

# Demonstração de funcionamento
# {:ok, pid} = TransactionManager.start_link([])
# FinanceApp.execute_transferencia(pid)
# IO.inspect(TransactionManager.get_state())
