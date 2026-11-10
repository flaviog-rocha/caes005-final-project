# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de votação online, atendendo somente aos seguintes requisitos:
#
# 1. Persistir os dados para recuperação após reinicializações.
# 2. Implementar expiração automática de registros.
# 3. Fazer uso de macros.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule VotingSystem do
  @moduledoc """
  Sistema de votação persistente com expiração automática usando ETS e GenServer.
  """
  use GenServer

  @table_name :voting_table
  @backup_file "votes.ets"

  # --- Macro para simplificar a definição de comandos de votação ---
  defmacro defvote(name, do: block) do
    quote do
      def unquote(name)(candidate) do
        GenServer.call(:voting_server, {:vote, unquote(name), candidate})
      end
    end
  end

  # --- API Pública ---
  def start_link(_) do
    GenServer.start_link(__MODULE__, nil, name: :voting_server)
  end

  defvote :poll_a do end
  defvote :poll_b do end

  def get_results do
    GenServer.call(:voting_server, :get_results)
  end

  # --- Callbacks do GenServer ---
  @impl true
  def init(_) do
    # Tenta carregar dados existentes do disco
    table = :ets.new(@table_name, [:set, :named_table, :public])
    if File.exists?(@backup_file), do: :ets.file2tab(@table_name, @backup_file)

    # Agenda limpeza periódica (expiração)
    schedule_cleanup()
    {:ok, %{table: table}}
  end

  @impl true
  def handle_call({:vote, poll, candidate}, _from, state) do
    now = System.system_time(:second)
    # TTL de 60 segundos para exemplo
    expiry = now + 60
    :ets.insert(@table_name, {{poll, candidate}, expiry})
    :ets.tab2file(@table_name, @backup_file)
    {:reply, :ok, state}
  end

  @impl true
  def handle_call(:get_results, _from, state) do
    results = :ets.tab2list(@table_name)
    {:reply, results, state}
  end

  @impl true
  def handle_info(:cleanup, state) do
    now = System.system_time(:second)
    # Remove registros expirados
    :ets.select_delete(@table_name, [{{{ :_, :_ }, :"$1"}, [{:<, :"$1", now}], [true]}])
    :ets.tab2file(@table_name, @backup_file)
    schedule_cleanup()
    {:noreply, state}
  end

  defp schedule_cleanup, do: Process.send_after(self(), :cleanup, 30_000)
end
