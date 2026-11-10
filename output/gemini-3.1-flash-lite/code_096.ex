# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de gerenciamento de notificações, atendendo somente aos seguintes requisitos:
#
# 1. Recuperar processos após falhas inesperadas.
# 2. Gerar notificações quando determinadas condições forem atingidas.
# 3. Permitir consultar o estado atual das operações em andamento.
# 4. Validar os dados recebidos antes de realizar operações.
# 5. Manter um histórico das operações realizadas.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - ETS
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule NotificationManager do
  @moduledoc """
  Sistema de gerenciamento de notificações utilizando GenServer e ETS para persistência.
  """
  use GenServer

  @table_name :notification_state
  @history_table :notification_history

  # API Pública

  def start_link(_) do
    GenServer.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  def notify(data) do
    with {:ok, validated_data} <- validate_data(data) do
      GenServer.call(__MODULE__, {:notify, validated_data})
    end
  end

  def get_state do
    :ets.tab2list(@table_name)
  end

  def get_history do
    :ets.tab2list(@history_table)
  end

  # Callbacks do GenServer

  @impl true
  def init(:ok) do
    # Recuperação: O ETS sobrevive a falhas do processo se criado fora ou via heir
    # Aqui garantimos que as tabelas existam
    :ets.new(@table_name, [:set, :public, :named_table, read_concurrency: true])
    :ets.new(@history_table, [:ordered_set, :public, :named_table])
    {:ok, %{}}
  end

  @impl true
  def handle_call({:notify, data}, _from, state) do
    # Lógica de negócio: Verificar condição (ex: status crítico)
    if Map.get(data, :priority) == :high do
      IO.puts("ALERTA: Notificação crítica recebida!")
    end

    # Atualizar estado e histórico
    timestamp = System.system_time(:millisecond)
    :ets.insert(@table_name, {data.id, data})
    :ets.insert(@history_table, {timestamp, data})

    {:reply, :ok, state}
  end

  # Funções Privadas

  defp validate_data(data) do
    cond do
      !Map.has_key?(data, :id) -> {:error, :missing_id}
      !Map.has_key?(data, :message) -> {:error, :missing_message}
      true -> {:ok, data}
    end
  end
end

# Exemplo de uso:
# {:ok, _} = NotificationManager.start_link(nil)
# NotificationManager.notify(%{id: 1, message: "Olá", priority: :high})
# IO.inspect(NotificationManager.get_state())
# IO.inspect(NotificationManager.get_history())
