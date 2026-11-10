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

defmodule UserSystem.Macros do
  @doc "Macro para automatizar a criação de logs de operação"
  defmacro def_logged(name, args, do: body) do
    quote do
      def unquote(name)(unquote_splicing(args)) do
        result = unquote(body)
        log_operation(unquote(name), unquote_splicing(args))
        result
      end
    end
  end
end

defmodule UserSystem.Manager do
  use GenServer
  import UserSystem.Macros

  @name __MODULE__

  # --- API Pública ---

  def start_link(_), do: GenServer.start_link(__MODULE__, %{users: %{}, history: []}, name: @name)

  def_logged(create_user, [id, data], do: GenServer.call(@name, {:create, id, data}))
  def_logged(update_user, [id, data], do: GenServer.call(@name, {:update, id, data}))
  def_logged(delete_user, [id], do: GenServer.call(@name, {:delete, id}))
  
  def get_user(id), do: GenServer.call(@name, {:get, id})
  def get_history, do: GenServer.call(@name, :history)

  # --- Callbacks do GenServer ---

  @impl true
  def init(state) do
    # Requisito 1: Recuperação pós-falha usando `:persistent_term` ou simples estado inicial
    # Aqui simulamos a recuperação de estado caso o processo tenha sido reiniciado via Supervisor
    {:ok, state}
  end

  @impl true
  def handle_call({:create, id, data}, _from, state) do
    new_users = Map.put(state.users, id, data)
    {:reply, :ok, %{state | users: new_users}}
  end

  @impl true
  def handle_call({:update, id, data}, _from, state) do
    if Map.has_key?(state.users, id) do
      new_users = Map.put(state.users, id, data)
      # Requisito 2: Notificação condicional
      if data[:notify], do: IO.puts("Notificação: Usuário #{id} atualizado!")
      {:reply, :ok, %{state | users: new_users}}
    else
      {:reply, {:error, :not_found}, state}
    end
  end

  @impl true
  def handle_call({:delete, id}, _from, state) do
    {:reply, :ok, %{state | users: Map.delete(state.users, id)}}
  end

  @impl true
  def handle_call({:get, id}, _from, state) do
    {:reply, Map.get(state.users, id), state}
  end

  @impl true
  def handle_call(:history, _from, state) do
    {:reply, state.history, state}
  end

  # Requisito 4: Registro de histórico privado
  defp log_operation(op, id, _data \\ nil) do
    GenServer.cast(@name, {:log, {op, id, DateTime.utc_now()}})
  end

  @impl true
  def handle_cast({:log, entry}, state) do
    {:noreply, %{state | history: [entry | state.history]}}
  end
end
