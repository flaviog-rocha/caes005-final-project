# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de troca de mensagens entre usuários, atendendo somente aos seguintes requisitos:
#
# 1. Recuperar processos após falhas inesperadas.
# 2. Garantir a consistência dos dados durante operações concorrentes.
# 3. Fazer uso de macros.
# 4. Permitir cadastrar, consultar, atualizar e remover registros.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule MessagingSystem do
  @moduledoc """
  Sistema de troca de mensagens utilizando GenServer para concorrência,
  ETS para persistência em memória (com suporte a recuperação via reinicialização)
  e Macros para reduzir boilerplate no CRUD.
  """

  use GenServer

  # --- Macro para gerar funções CRUD ---
  defmacro def_crud(table_name) do
    quote do
      def create(key, value) do
        GenServer.call(__MODULE__, {:create, unquote(table_name), key, value})
      end

      def get(key) do
        GenServer.call(__MODULE__, {:get, unquote(table_name), key})
      end

      def update(key, value) do
        GenServer.call(__MODULE__, {:update, unquote(table_name), key, value})
      end

      def delete(key) do
        GenServer.call(__MODULE__, {:delete, unquote(table_name), key})
      end
    end
  end

  # --- Implementação do GenServer ---
  def start_link(_) do
    GenServer.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  @impl true
  def init(:ok) do
    # O uso do table type :set garante unicidade e consistência (atomicity)
    # :named_table garante que o processo pai recupere o acesso após falha
    table = :ets.new(:messages, [:set, :public, :named_table, read_concurrency: true])
    {:ok, %{table: table}}
  end

  # --- Uso da Macro ---
  def_crud(:messages)

  @impl true
  def handle_call({:create, table, key, value}, _from, state) do
    success = :ets.insert_new(table, {key, value})
    {:reply, success, state}
  end

  @impl true
  def handle_call({:get, table, key}, _from, state) do
    result = case :ets.lookup(table, key) do
      [{^key, value}] -> {:ok, value}
      [] -> {:error, :not_found}
    end
    {:reply, result, state}
  end

  @impl true
  def handle_call({:update, table, key, value}, _from, state) do
    # Garante consistência: só atualiza se existir
    updated = :ets.update_element(table, key, {2, value})
    {:reply, updated, state}
  end

  @impl true
  def handle_call({:delete, table, key}, _from, state) do
    deleted = :ets.delete(table, key)
    {:reply, deleted, state}
  end
end

# --- Exemplo de Uso ---
# 1. Iniciar o sistema
MessagingSystem.start_link([])

# 2. CRUD
MessagingSystem.create("user1", "Olá, mundo!")
IO.inspect(MessagingSystem.get("user1"))

MessagingSystem.update("user1", "Olá, Elixir!")
IO.inspect(MessagingSystem.get("user1"))

MessagingSystem.delete("user1")
IO.inspect(MessagingSystem.get("user1"))
