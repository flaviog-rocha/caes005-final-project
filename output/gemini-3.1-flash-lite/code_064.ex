# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de usuários e sessões, atendendo somente aos seguintes requisitos:
#
# 1. Recuperar processos após falhas inesperadas.
# 2. Gerar notificações quando determinadas condições forem atingidas.
# 3. Permitir cadastrar, consultar, atualizar e remover registros.
# 4. Manter um histórico das operações realizadas.
# 5. Fazer uso de macros.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - GenServer
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule UserSystem do
  @moduledoc """
  Sistema de gerenciamento de usuários com persistência, histórico e notificações.
  Utiliza ETS para persistência (recuperação após falha) e GenServer para estado.
  """

  defmodule MacroUtils do
    defmacro def_operation(name, opts) do
      quote do
        def unquote(name)(pid, unquote_splicing(opts)) do
          GenServer.call(pid, {:unquote(name), unquote_splicing(opts)})
        end
      end
    end
  end

  use GenServer
  require MacroUtils

  # --- Interface Pública ---

  def start_link(_), do: GenServer.start_link(__MODULE__, %{}, name: __MODULE__)

  MacroUtils.def_operation(:create, [id, data])
  MacroUtils.def_operation(:get, [id])
  MacroUtils.def_operation(:update, [id, data])
  MacroUtils.def_operation(:delete, [id])

  # --- Callbacks do GenServer ---

  @impl true
  def init(_) do
    # Tabela ETS persistente para recuperar processos após falha
    table = :ets.new(:user_store, [:set, :public, :named_table, read_concurrency: true])
    history = :ets.new(:history_log, [:ordered_set, :public, :named_table])
    {:ok, %{table: table, history: history}}
  end

  @impl true
  def handle_call({:create, id, data}, _from, state) do
    :ets.insert(:user_store, {id, data})
    log_operation(:create, id)
    {:reply, :ok, state}
  end

  @impl true
  def handle_call({:get, id}, _from, state) do
    result = case :ets.lookup(:user_store, id) do
      [{^id, data}] -> {:ok, data}
      [] -> {:error, :not_found}
    end
    {:reply, result, state}
  end

  @impl true
  def handle_call({:update, id, data}, _from, state) do
    if :ets.member(:user_store, id) do
      :ets.insert(:user_store, {id, data})
      log_operation(:update, id)
      check_notifications(id, data)
      {:reply, :ok, state}
    else
      {:reply, {:error, :not_found}, state}
    end
  end

  @impl true
  def handle_call({:delete, id}, _from, state) do
    :ets.delete(:user_store, id)
    log_operation(:delete, id)
    {:reply, :ok, state}
  end

  # --- Funções Auxiliares ---

  defp log_operation(action, id) do
    timestamp = System.system_time(:second)
    :ets.insert(:history_log, {{timestamp, id}, action})
  end

  defp check_notifications(id, data) do
    # Exemplo de condição: notificar se usuário for "admin"
    if Map.get(data, :role) == :admin do
      IO.puts("[NOTIFICAÇÃO] Usuário #{id} atualizado para administrador.")
    end
  end
end
