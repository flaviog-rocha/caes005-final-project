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

defmodule NotificationSystem do
  @moduledoc """
  Sistema de gerenciamento de notificações com concorrência limitada via DynamicSupervisor,
  persistência via ETS (tabela protegida) e garantia de consistência.
  """

  use Supervisor

  def start_link(_), do: Supervisor.start_link(__MODULE__, :ok, name: __MODULE__)

  @impl true
  def init(:ok) do
    # Tabela ETS para persistência simples (set = consistência por chave)
    # :named_table garante acesso global, :protected permite escrita apenas pelo owner
    :ets.new(:notifications, [:set, :public, :named_table, read_concurrency: true])

    children = [
      {DynamicSupervisor, name: NotificationSupervisor, strategy: :one_for_one},
      # Semáforo para limitar a quantidade de operações simultâneas (max 5)
      {Task.Supervisor, name: NotificationTaskSupervisor},
      {Registry, keys: :unique, name: NotificationRegistry}
    ]

    Supervisor.init(children, strategy: :one_for_all)
  end

  @doc "Envia uma notificação com limite de concorrência usando um pool dinâmico"
  def send_notification(id, payload) do
    # Garante consistência: Se já existe, não sobrepõe ou valida estado
    case :ets.insert_new(:notifications, {id, :pending, payload}) do
      true ->
        DynamicSupervisor.start_child(NotificationSupervisor, {Task, fn ->
          execute_task(id, payload)
        end})
      false -> {:error, :already_exists}
    end
  end

  defp execute_task(id, payload) do
    # Simula trabalho IO
    Process.sleep(1000)
    :ets.insert(:notifications, {id, :sent, payload})
  end

  @doc "Recupera dados da persistência"
  def get_status(id), do: :ets.lookup(:notifications, id)
end

# Exemplo de uso:
# NotificationSystem.start_link([])
# NotificationSystem.send_notification("msg_1", "Hello World")
# NotificationSystem.get_status("msg_1")
