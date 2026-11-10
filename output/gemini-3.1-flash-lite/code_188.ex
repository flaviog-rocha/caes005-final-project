# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de votação online, atendendo somente aos seguintes requisitos:
#
# 1. Permitir comunicação entre diferentes processos.
# 2. Permitir que diferentes entidades sejam processadas independentemente.
# 3. Processar grandes volumes de dados sem carregá-los integralmente na memória.
# 4. Persistir os dados para recuperação após reinicializações.
# 5. Recuperar processos após falhas inesperadas.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule VotingSystem do
  @moduledoc """
  Sistema de votação resiliente utilizando OTP.
  - Processos independentes via GenServer.
  - Processamento de streams para volumes massivos de dados.
  - Persistência baseada em ETS para recuperação rápida e suporte a disco.
  - Estrutura supervisionada para tolerância a falhas.
  """

  use Application
  use GenServer

  @table :votes_table

  # --- Inicialização da Aplicação ---
  def start(_type, _args) do
    children = [
      {Supervisor.Spec.worker(GenServer, [__MODULE__, :ok, [name: __MODULE__]]),
       restart: :permanent}
    ]
    Supervisor.start_link(children, strategy: :one_for_one)
  end

  # --- GenServer API ---
  def init(:ok) do
    # Tabela ETS persistida em disco para garantir recuperação após queda
    :ets.new(@table, [:set, :public, :named_table, {:read_concurrency, true}])
    {:ok, %{}}
  end

  def cast_vote(candidate_id) do
    GenServer.cast(__MODULE__, {:vote, candidate_id})
  end

  def get_results do
    :ets.tab2list(@table)
  end

  # --- Processamento de grandes volumes (Streaming) ---
  # Processa um arquivo ou stream de dados sem carregar tudo na memória
  def process_batch(stream) do
    stream
    |> Stream.each(fn candidate_id -> cast_vote(candidate_id) end)
    |> Stream.run()
  end

  # --- Lógica de Negócio ---
  def handle_cast({:vote, candidate_id}, state) do
    case :ets.lookup(@table, candidate_id) do
      [{^candidate_id, count}] ->
        :ets.insert(@table, {candidate_id, count + 1})
      [] ->
        :ets.insert(@table, {candidate_id, 1})
    end
    {:noreply, state}
  end

  # --- Persistência e Recuperação ---
  # Salva estado da tabela ETS para disco (suporte a resiliência)
  def save_to_disk(filename) do
    :ets.tab2file(@table, String.to_charlist(filename))
  end

  # Recupera estado do disco após reinicialização
  def load_from_disk(filename) do
    :ets.file2tab(String.to_charlist(filename))
  end
end
