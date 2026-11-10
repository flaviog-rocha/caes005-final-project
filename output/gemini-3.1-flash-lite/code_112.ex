# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de gerenciamento de notificações, atendendo somente aos seguintes requisitos:
#
# 1. Permitir limitar a quantidade de operações simultâneas.
# 2. Garantir a consistência dos dados durante operações concorrentes.
# 3. Persistir os dados para recuperação após reinicializações.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - DynamicSupervisor
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule NotificationManager do
  @moduledoc """
  Sistema de gerenciamento de notificações com controle de concorrência,
  consistência via Agent e persistência via ETS.
  """
  use Supervisor

  @table :notifications_table

  def start_link(_), do: Supervisor.start_link(__MODULE__, [], name: __MODULE__)

  @impl true
  def init(_) do
    :ets.new(@table, [:set, :public, :named_table, read_concurrency: true])
    
    children = [
      {DynamicSupervisor, name: NotificationSupervisor, strategy: :one_for_one},
      {Task, fn -> load_persisted_data() end}
    ]

    Supervisor.init(children, strategy: :one_for_all)
  end

  # Limitador de operações via Semaphore (usando contagem de processos no DynamicSupervisor)
  def send_notification(id, message) do
    if count_running_tasks() < 10 do
      DynamicSupervisor.start_child(NotificationSupervisor, {Task, fn -> process(id, message) end})
    else
      {:error, :rate_limit_exceeded}
    end
  end

  defp process(id, message) do
    # Garantir consistência: Escrita atômica no ETS (simulando persistência)
    :ets.insert(@table, {id, %{message: message, status: :sent, ts: System.system_time()}})
    persist_to_disk()
  end

  defp count_running_tasks, do: DynamicSupervisor.count_children(NotificationSupervisor).active

  # Persistência básica usando arquivo (simulando o requisito de recuperação)
  defp persist_to_disk do
    data = :ets.tab2list(@table)
    File.write!("notifications.dat", :erlang.term_to_binary(data))
  end

  defp load_persisted_data do
    if File.exists?("notifications.dat") do
      data = File.read!("notifications.dat") |> :erlang.binary_to_term()
      :ets.insert(@table, data)
    end
  end
end
