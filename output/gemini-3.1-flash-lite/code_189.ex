# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de votação online, atendendo somente aos seguintes requisitos:
#
# 1. Persistir os dados para recuperação após reinicializações.
# 2. Impedir que uma mesma operação seja executada mais de uma vez.
# 3. Processar grandes volumes de dados sem carregá-los integralmente na memória.
# 4. Fazer uso de macros.
# 5. Recuperar processos após falhas inesperadas.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule VotingSystem do
  @moduledoc """
  Sistema de votação resiliente utilizando ETS para persistência rápida e 
  GenServer com estratégia de supervisão para tolerância a falhas.
  """

  # Requisito 4: Uso de macros para definir comandos de votação
  defmacro defvote(command) do
    quote do
      def unquote(command)(user_id, candidate_id) do
        GenServer.call(__MODULE__, {:vote, unquote(command), user_id, candidate_id})
      end
    end
  end

  use GenServer

  @table_name :votes_table
  @file_path "votes.dat"

  # Requisito 5: Recuperar processos após falhas (via Supervisor/GenServer)
  def start_link(_) do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  @impl true
  def init(_) do
    # Requisito 1: Persistência (Carrega de arquivo se existir)
    table = :ets.new(@table_name, [:set, :named_table, :public])
    load_from_disk()
    {:ok, table}
  end

  # Requisito 4: Macro aplicada
  defvote(:cast_vote)

  @impl true
  def handle_call({:vote, :cast_vote, user_id, candidate_id}, _from, state) do
    # Requisito 2: Impedir operação duplicada (usando a atomicidade do ETS)
    key = {:vote, user_id}
    
    if :ets.member(@table_name, key) do
      {:reply, {:error, :already_voted}, state}
    else
      :ets.insert(@table_name, {key, candidate_id})
      persist_async(key, candidate_id)
      {:reply, :ok, state}
    end
  end

  # Requisito 3: Processamento de grandes volumes via Stream (sem carregar tudo na memória)
  def stream_votes do
    File.stream!(@file_path)
    |> Stream.map(&:erlang.binary_to_term/1)
  end

  defp persist_async(key, val) do
    File.write(@file_path, :erlang.term_to_binary({key, val}), [:append])
  end

  defp load_from_disk do
    if File.exists?(@file_path) do
      # Processa grandes volumes linha a linha sem carregar o arquivo inteiro
      File.stream!(@file_path, [], 1024)
      |> Stream.map(&:erlang.binary_to_term/1)
      |> Enum.each(fn {key, val} -> :ets.insert(@table_name, {key, val}) end)
    end
  end
end

# Exemplo de uso sob um Supervisor (Requisito 5)
defmodule VotingApp.Supervisor do
  use Supervisor

  def start_link(_), do: Supervisor.start_link(__MODULE__, [], name: __MODULE__)

  @impl true
  def init(_) do
    children = [{VotingSystem, []}]
    Supervisor.init(children, strategy: :one_for_one)
  end
end
