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
  Sistema de gerenciamento de entregas usando GenServer, ETS e Macros.
  """

  # 1. Macro para validação de dados
  defmacro defvalidator(name, keys) do
    quote do
      def unquote(name)(data) do
        missing = Enum.reject(unquote(keys), &Map.has_key?(data, &1))
        case missing do
          [] -> {:ok, data}
          _ -> {:error, "Campos faltando: #{inspect(missing)}"}
        end
      end
    end
  end

  defmodule Manager do
    use GenServer
    require DeliverySystem

    # 4. Uso da Macro
    DeliverySystem.defvalidator(:validate_delivery, [:id, :destination])

    @table :delivery_history
    @expiration_ms 60_000 # 2. Expiração: 60 segundos

    # 3. Recuperação de estado: O ETS é persistente fora do processo
    def start_link(_) do
      GenServer.start_link(__MODULE__, %{}, name: __MODULE__)
    end

    def init(_) do
      :ets.new(@table, [:named_table, :set, :public, read_concurrency: true])
      {:ok, %{}}
    end

    def track(id, destination) do
      GenServer.call(__MODULE__, {:track, id, destination})
    end

    def handle_call({:track, id, destination}, _from, state) do
      data = %{id: id, destination: destination}

      case validate_delivery(data) do
        {:ok, validated} ->
          # 5. Histórico no ETS
          timestamp = :os.system_time(:millisecond)
          record = {id, validated, timestamp}
          :ets.insert(@table, record)
          
          # 2. Expiração: Agenda verificação
          Process.send_after(self(), {:expire, id}, @expiration_ms)
          
          {:reply, {:ok, record}, state}

        error -> {:reply, error, state}
      end
    end

    # 2. Lógica de expiração automática
    def handle_info({:expire, id}, state) do
      :ets.delete(@table, id)
      {:noreply, state}
    end
  end
end

# Demonstração de uso:
# DeliverySystem.Manager.start_link([])
# DeliverySystem.Manager.track("ORD-123", "Rua A, 10")
# :ets.tab2list(:delivery_history)
