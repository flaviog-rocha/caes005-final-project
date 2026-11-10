# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de troca de mensagens entre usuários, atendendo somente aos seguintes requisitos:
#
# 1. Implementar expiração automática de registros.
# 2. Persistir os dados para recuperação após reinicializações.
# 3. Executar determinadas operações periodicamente.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# Para rodar este exemplo, salve como message_system.exs e execute: elixir message_system.exs
# Este sistema utiliza :ets para persistência em disco (via tabela protegida)
# e GenServer para gerenciar o ciclo de vida e limpeza periódica.

defmodule MessageSystem do
  use GenServer

  @table_name :messages
  @file_path "messages.ets"
  @cleanup_interval :timer.seconds(10)
  @ttl_seconds 30

  # API Pública
  def start_link(_) do
    GenServer.start_link(__MODULE__, %{}, name: __MODULE__)
  end

  def send_message(from, to, content) do
    GenServer.cast(__MODULE__, {:add, from, to, content})
  end

  def get_messages do
    :ets.tab2list(@table_name)
  end

  # Callbacks do GenServer
  @impl true
  def init(_) do
    # 1 & 2: Recuperação de dados do disco se existir
    table = :ets.file2tab(@file_path) |> case do
      {:ok, t} -> t
      _ -> :ets.new(@table_name, [:named_table, :set, :public, read_concurrency: true])
    end

    # 3: Agendar primeira limpeza
    schedule_cleanup()
    {:ok, %{table: table}}
  end

  @impl true
  def handle_cast({:add, from, to, content}, state) do
    expiry = System.system_time(:second) + @ttl_seconds
    :ets.insert(@table_name, {{:msg, System.unique_integer()}, from, to, content, expiry})
    persist()
    {:noreply, state}
  end

  @impl true
  def handle_info(:cleanup, state) do
    now = System.system_time(:second)
    
    # 1: Expiração automática
    :ets.match_delete(@table_name, {:_, :_, :_, :_, :"$1"})
    
    all = :ets.tab2list(@table_name)
    Enum.each(all, fn {id, _, _, _, expiry} ->
      if expiry < now, do: :ets.delete(@table_name, id)
    end)

    persist()
    schedule_cleanup()
    {:noreply, state}
  end

  # Funções Auxiliares
  defp schedule_cleanup, do: Process.send_after(self(), :cleanup, @cleanup_interval)

  defp persist do
    :ets.tab2file(@table_name, ~c"#{@file_path}")
  end
end

# Demonstração simples
{:ok, _} = MessageSystem.start_link([])
MessageSystem.send_message("alice", "bob", "Ola mundo!")
IO.puts("Mensagens atuais: #{inspect(MessageSystem.get_messages())}")
