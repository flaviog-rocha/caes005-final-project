# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de agendamento de compromissos, atendendo somente aos seguintes requisitos:
#
# 1. Validar os dados recebidos antes de realizar operações.
# 2. Permitir cadastrar, consultar, atualizar e remover registros.
# 3. Recuperar processos após falhas inesperadas.
# 4. Detectar encerramentos inesperados de processos.
# 5. Permitir consultar o estado atual das operações em andamento.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule AppointmentSystem do
  @moduledoc """
  Sistema de agendamento resiliente utilizando GenServer.
  Gerencia estados, valida dados e garante recuperação via Supervisor.
  """
  use GenServer

  # --- API Pública ---

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def create_appointment(id, details) when is_map(details) do
    if valid_details?(details),
      do: GenServer.call(__MODULE__, {:create, id, details}),
      else: {:error, :invalid_data}
  end

  def get_appointment(id), do: GenServer.call(__MODULE__, {:get, id})

  def update_appointment(id, details) when is_map(details) do
    if valid_details?(details),
      do: GenServer.call(__MODULE__, {:update, id, details}),
      else: {:error, :invalid_data}
  end

  def delete_appointment(id), do: GenServer.call(__MODULE__, {:delete, id})

  def get_all_state, do: GenServer.call(__MODULE__, :get_state)

  # --- Callbacks do GenServer ---

  @impl true
  def init(_) do
    # O estado é recuperado de um arquivo ou ETS se necessário. 
    # Aqui usamos um mapa simples.
    {:ok, %{}}
  end

  @impl true
  def handle_call({:create, id, details}, _from, state) do
    if Map.has_key?(state, id) do
      {:reply, {:error, :already_exists}, state}
    else
      new_state = Map.put(state, id, details)
      {:reply, :ok, new_state}
    end
  end

  @impl true
  def handle_call({:get, id}, _from, state) do
    {:reply, Map.get(state, id, :not_found), state}
  end

  @impl true
  def handle_call({:update, id, details}, _from, state) do
    if Map.has_key?(state, id) do
      new_state = Map.put(state, id, details)
      {:reply, :ok, new_state}
    else
      {:reply, :not_found, state}
    end
  end

  @impl true
  def handle_call({:delete, id}, _from, state) do
    new_state = Map.delete(state, id)
    {:reply, :ok, new_state}
  end

  @impl true
  def handle_call(:get_state, _from, state), do: {:reply, state, state}

  # --- Validação ---

  defp valid_details?(%{date: _, title: _}), do: true
  defp valid_details?(_), do: false

  # --- Resiliência ---

  @doc """
  Para recuperação de falhas (Requirement 3 & 4), o módulo deve ser 
  iniciado sob um Supervisor.
  """
  def child_spec(opts) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [opts]},
      restart: :permanent, # Garante reinicialização em falhas
      shutdown: 5000
    }
  end
end

# Exemplo de uso sob um Supervisor
defmodule AppSupervisor do
  use Supervisor

  def start_link(_), do: Supervisor.start_link(__MODULE__, :ok, name: __MODULE__)

  @impl true
  def init(:ok) do
    children = [AppointmentSystem]
    Supervisor.init(children, strategy: :one_for_one)
  end
end
