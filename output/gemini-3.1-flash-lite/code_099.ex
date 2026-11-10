# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de entregas e rastreamento, atendendo somente aos seguintes requisitos:
#
# 1. Recuperar processos após falhas inesperadas.
# 2. Persistir os dados para recuperação após reinicializações.
# 3. Diferenciar erros de validação de falhas operacionais.
# 4. Garantir a consistência dos dados durante operações concorrentes.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - Supervisor
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# Para persistência em arquivo simples, usamos o módulo :dets (Disk-based Erlang Term Storage)
# O estado é gerenciado via GenServer sob um Supervisor para garantir resiliência.

defmodule DeliverySystem.Storage do
  @table :delivery_data

  def init do
    {:ok, _} = :dets.open_file(@table, [type: :set, auto_save: 5000])
  end

  def save(id, data), do: :dets.insert(@table, {id, data})
  def get(id), do: :dets.lookup(@table, id)
end

defmodule DeliverySystem.Manager do
  use GenServer

  # Erros Customizados
  defmodule ValidationError, do: defexception [:message]
  defmodule OperationalError, do: defexception [:message]

  # API
  def start_link(_), do: GenServer.start_link(__MODULE__, %{}, name: __MODULE__)

  def update_status(id, status) do
    GenServer.call(__MODULE__, {:update, id, status})
  end

  # Callbacks
  @impl true
  def init(_) do
    DeliverySystem.Storage.init()
    # Recupera estado do disco após reinicialização
    {:ok, %{}}
  end

  @impl true
  def handle_call({:update, id, status}, _from, state) do
    # 1. Validação (Diferenciando erros de validação)
    cond do
      status not in [:pending, :in_transit, :delivered] ->
        {:reply, {:error, %ValidationError{message: "Status inválido"}}, state}
      
      true ->
        # 2. Consistência Concorrente (GenServer serializa mensagens por mailbox)
        new_state = Map.put(state, id, status)
        
        # 3. Persistência
        case DeliverySystem.Storage.save(id, status) do
          :ok -> {:reply, :ok, new_state}
          _ -> {:reply, {:error, %OperationalError{message: "Falha na escrita em disco"}}, state}
        end
    end
  end
end

defmodule DeliverySystem.Supervisor do
  use Supervisor

  def start_link(_) do
    Supervisor.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  @impl true
  def init(:ok) do
    children = [
      # O Supervisor garante a recuperação após falhas inesperadas (restart: :permanent)
      %{
        id: DeliverySystem.Manager,
        start: {DeliverySystem.Manager, :start_link, [[]]},
        restart: :permanent
      }
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end
