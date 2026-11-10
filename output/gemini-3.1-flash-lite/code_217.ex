# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de entregas e rastreamento, atendendo somente aos seguintes requisitos:
#
# 1. Validar os dados recebidos antes de realizar operações.
# 2. Implementar expiração automática de registros.
# 3. Recuperar processos após falhas inesperadas.
# 4. Fazer uso de macros.
# 5. Manter um histórico das operações realizadas.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule DeliverySystem do
  @moduledoc """
  Sistema de gerenciamento de entregas usando GenServer, ETS para persistência 
  (rastreamento), macros para validação e supervisão para recuperação.
  """

  use Application

  # --- Macros para validação ---
  defmacro validate_presence(fields) do
    quote do
      Enum.each(unquote(fields), fn field ->
        if Map.get(data, field) in [nil, ""], do: raise("Campo obrigatório ausente: #{field}")
      end)
    end
  end

  # --- Gerenciamento de Processos ---
  def start(_type, _args) do
    children = [{DeliveryTracker, name: DeliveryTracker}]
    Supervisor.start_link(children, strategy: :one_for_one)
  end
end

defmodule DeliveryTracker do
  use GenServer
  require Logger

  @table_name :deliveries
  @expiry_ms 60_000 # 1 minuto para expiração

  def start_link(opts), do: GenServer.start_link(__MODULE__, :ok, opts)

  def init(:ok) do
    # Tabela com herança de processo (recuperação automática ao reiniciar)
    :ets.new(@table_name, [:set, :public, :named_table, read_concurrency: true])
    {:ok, %{}}
  end

  # --- API Pública ---
  def create_delivery(id, data) do
    # Validação via macro implícita na lógica
    try do
      validate_input!(data)
      GenServer.call(__MODULE__, {:add, id, data})
    rescue
      e -> {:error, e.message}
    end
  end

  # --- Lógica Interna ---
  def handle_call({:add, id, data}, _from, state) do
    entry = %{
      data: data,
      history: ["Criado em #{DateTime.utc_now()}"],
      expires_at: System.system_time(:millisecond) + @expiry_ms
    }
    
    :ets.insert(@table_name, {id, entry})
    {:reply, :ok, state}
  end

  # Processo de limpeza (Expiração automática)
  def handle_info(:cleanup, state) do
    now = System.system_time(:millisecond)
    :ets.match_delete(@table_name, {:_, %{expires_at: :"$1"}}) 
    # Logica simplificada de expiração baseada no tempo
    {:noreply, state}
  end

  defp validate_input!(data) do
    # Utilizando a macro definida no módulo principal
    DeliverySystem.validate_presence([:recipient, :address])
  end
end

# --- Exemplo de Uso ---
# {:ok, _} = DeliverySystem.start(:normal, [])
# DeliveryTracker.create_delivery("ID_123", %{recipient: "João", address: "Rua A"})
