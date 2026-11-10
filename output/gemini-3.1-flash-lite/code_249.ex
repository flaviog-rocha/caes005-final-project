# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de troca de mensagens entre usuários, atendendo somente aos seguintes requisitos:
#
# 1. Implementar expiração automática de registros.
# 2. Persistir os dados para recuperação após reinicializações.
# 3. Executar determinadas operações periodicamente.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# Para persistência, utilizamos o :dets (Disk-based Erlang Term Storage).
# O GenServer gerencia o estado e o loop periódico de expiração.

defmodule MessageSystem do
  use GenServer
  require Logger

  @table_name :messages_store
  @expiry_seconds 3600 # 1 hora
  @cleanup_interval 60_000 # 1 minuto

  # API Pública
  def start_link(_opts) do
    GenServer.start_link(__MODULE__, %{}, name: __MODULE__)
  end

  def send_message(from, to, content) do
    GenServer.cast(__MODULE__, {:save, {from, to, content, :os.system_time(:second)}})
  end

  # Callbacks do GenServer
  @impl true
  def init(_) do
    # Abre arquivo para persistência no diretório atual
    :dets.open_file(@table_name, type: :set, file: 'messages.dets')
    
    # Inicia o timer para limpeza periódica
    schedule_cleanup()
    
    {:ok, %{}}
  end

  @impl true
  def handle_cast({:save, {from, to, content, timestamp}}, state) do
    :dets.insert(@table_name, {{from, to, timestamp}, content})
    {:noreply, state}
  end

  @impl true
  def handle_info(:cleanup, state) do
    now = :os.system_time(:second)
    
    # Remove registros expirados
    :dets.foldl(fn {key, _val}, _acc ->
      {_, _, timestamp} = key
      if (now - timestamp) > @expiry_seconds do
        :dets.delete(@table_name, key)
      end
    end, nil, @table_name)

    schedule_cleanup()
    {:noreply, state}
  end

  defp schedule_cleanup do
    Process.send_after(self(), :cleanup, @cleanup_interval)
  end
end

# Exemplo de inicialização:
# MessageSystem.start_link([])
# MessageSystem.send_message("alice", "bob", "Olá, mundo!")
