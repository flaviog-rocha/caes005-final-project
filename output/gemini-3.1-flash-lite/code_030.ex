# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de entregas e rastreamento, atendendo somente aos seguintes requisitos:
#
# 1. Recuperar processos após falhas inesperadas.
# 2. Quando fizer sentido para o domínio, considerar a comunicação com um banco de dados fictício e, opcionalmente, o uso de Ecto.Migration, sem tornar isso obrigatório.
# 3. Executar determinadas operações periodicamente.
# 4. Validar os dados recebidos antes de realizar operações.
# 5. Diferenciar erros de validação de falhas operacionais.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - DynamicSupervisor
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule DeliverySystem do
  @moduledoc """
  Sistema de gerenciamento e rastreamento de entregas usando supervisão dinâmica.
  """

  # 1. Recuperação de processos: DynamicSupervisor permite reiniciar processos sob falhas.
  # 4. Validação de dados: Usando Ecto.Changeset (ou simulação simplificada).
  # 3. Operações periódicas: Process.send_after/3 loop.

  defmodule TrackingError, do: defexception [:message]
  defmodule ValidationError, do: defexception [:message]

  defmodule DeliveryWorker do
    use GenServer, restart: :transient

    def start_link(delivery_id) do
      GenServer.start_link(__MODULE__, delivery_id, name: via_tuple(delivery_id))
    end

    def init(delivery_id) do
      # Simula recuperação de estado do "banco de dados"
      send(self(), :schedule_tracking)
      {:ok, %{id: delivery_id, status: :in_transit}}
    end

    def handle_info(:schedule_tracking, state) do
      # 3. Operações periódicas
      Process.send_after(self(), :track, :timer.seconds(5))
      {:noreply, state}
    end

    def handle_info(:track, state) do
      # Simulação de falha operacional aleatória
      if :rand.uniform(10) > 8, do: raise "Falha de comunicação externa"

      IO.puts("Rastreando entrega #{state.id}: Status #{state.status}")
      Process.send_after(self(), :track, :timer.seconds(5))
      {:noreply, state}
    end

    defp via_tuple(id), do: {:via, Registry, {DeliveryRegistry, id}}
  end

  defmodule Manager do
    use DynamicSupervisor

    def start_link(_), do: DynamicSupervisor.start_link(__MODULE__, nil, name: __MODULE__)

    def init(_), do: DynamicSupervisor.init(strategy: :one_for_one)

    def start_delivery(id) when is_binary(id) do
      # 4. Validação de dados antes da operação
      case validate_id(id) do
        :ok -> DynamicSupervisor.start_child(__MODULE__, {DeliveryWorker, id})
        {:error, reason} -> {:error, %ValidationError{message: reason}}
      end
    end

    defp validate_id(id) do
      if String.length(id) > 3, do: :ok, else: {:error, "ID muito curto"}
    end
  end

  # Setup inicial para execução do exemplo
  def run do
    # Registro para localizar processos dinâmicos
    Registry.start_link(keys: :unique, name: DeliveryRegistry)
    Manager.start_link(nil)

    # Exemplo de uso
    case Manager.start_delivery("DELIVERY_123") do
      {:ok, _pid} -> IO.puts("Entrega iniciada.")
      {:error, %ValidationError{} = e} -> IO.puts("Erro de validação: #{e.message}")
      {:error, reason} -> IO.puts("Falha operacional: #{inspect(reason)}")
    end
  end
end
