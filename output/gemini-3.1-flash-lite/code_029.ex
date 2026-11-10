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
  Sistema de rastreamento de entregas utilizando supervisão dinâmica e resiliência.
  """

  # 1 & 3: Utilizando GenServer com periodicidade e estado persistente simulado.
  defmodule DeliveryWorker do
    use GenServer

    def start_link(delivery_id) do
      GenServer.start_link(__MODULE__, delivery_id, name: via_tuple(delivery_id))
    end

    def init(delivery_id) do
      # Recuperação: Ao iniciar, busca estado no "banco"
      state = MockDB.load(delivery_id) || %{id: delivery_id, status: :pending}
      {:ok, state, {:continue, :schedule_update}}
    end

    def handle_continue(:schedule_update, state) do
      # 3: Operação periódica (ex: verificar status a cada 10s)
      Process.send_after(self(), :tick, 10_000)
      {:noreply, state}
    end

    def handle_info(:tick, state) do
      # Lógica de atualização periódica...
      Process.send_after(self(), :tick, 10_000)
      {:noreply, state}
    end

    defp via_tuple(id), do: {:via, Registry, {DeliveryRegistry, id}}
  end

  # 5: Diferenciação de erros
  defmodule Errors do
    defexception [:message, :type]
    @impl true
    def exception(opts), do: %__MODULE__{message: opts[:message], type: opts[:type]}
  end

  # 4: Validação de Dados
  defmodule Validator do
    def validate_delivery(%{id: id, destination: dest}) when is_binary(id) and is_binary(dest), do: :ok
    def validate_delivery(_), do: {:error, %Errors{message: "Invalid Schema", type: :validation}}
  end

  # Gerenciador de processos (DynamicSupervisor)
  defmodule DeliveryManager do
    use DynamicSupervisor

    def start_link(_), do: DynamicSupervisor.start_link(__MODULE__, nil, name: __MODULE__)

    def init(_), do: DynamicSupervisor.init(strategy: :one_for_one)

    def start_delivery(delivery_params) do
      with :ok <- Validator.validate_delivery(delivery_params) do
        spec = {DeliveryWorker, delivery_params.id}
        DynamicSupervisor.start_child(__MODULE__, spec)
      else
        {:error, reason} -> {:error, reason} # Erro de validação
        _ -> {:error, %Errors{message: "Operational failure", type: :system}}
      end
    end
  end

  # Banco de dados fictício
  defmodule MockDB do
    def load(_id), do: nil # Simula busca no banco
    def save(_data), do: :ok
  end

  # Inicialização da árvore de supervisão
  defmodule Application do
    use Application
    def start(_type, _args) do
      children = [
        {Registry, keys: :unique, name: DeliveryRegistry},
        DeliveryManager
      ]
      Supervisor.start_link(children, strategy: :one_for_one)
    end
  end
end
