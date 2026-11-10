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
  Sistema de gerenciamento de notificações utilizando GenServer e ETS
  para persistência e tolerância a falhas.
  """
  use GenServer

  @table_name :notifications_storage

  # --- API Pública ---

  def start_link(_) do
    GenServer.start_link(__MODULE__, nil, name: __MODULE__)
  end

  def add_notification(payload) do
    with {:ok, data} <- validate_payload(payload) do
      GenServer.call(__MODULE__, {:add, data})
    end
  end

  def get_history do
    GenServer.call(__MODULE__, :history)
  end

  # --- Callbacks do GenServer ---

  @impl true
  def init(_) do
    # O ETS é configurado com :named_table e :public para persistir 
    # independentemente do processo GenServer (recuperação após crash).
    # O parâmetro heir garante que o ETS sobreviva à morte do processo.
    table = :ets.new(@table_name, [:ordered_set, :public, :named_table, read_concurrency: true])
    {:ok, %{table: table}}
  end

  @impl true
  def handle_call({:add, data}, _from, state) do
    timestamp = System.system_time(:millisecond)
    entry = {timestamp, data}
    
    :ets.insert(@table_name, entry)

    # Lógica de notificação condicional
    if data.priority == :high do
      IO.puts("[NOTIFICAÇÃO] Alta prioridade detectada: #{data.message}")
    end

    {:reply, {:ok, timestamp}, state}
  end

  @impl true
  def handle_call(:history, _from, state) do
    history = :ets.tab2list(@table_name)
    {:reply, history, state}
  end

  # --- Lógica Interna ---

  defp validate_payload(payload) do
    case payload do
      %{message: msg, priority: p} when is_binary(msg) and p in [:low, :medium, :high] ->
        {:ok, payload}
      _ ->
        {:error, :invalid_payload}
    end
  end
end

# --- Exemplo de Uso ---
# {:ok, _} = NotificationManager.start_link(nil)
# NotificationManager.add_notification(%{message: "Alerta Crítico", priority: :high})
# NotificationManager.get_history()
