# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de votação online, atendendo somente aos seguintes requisitos:
#
# 1. Impedir que uma mesma operação seja executada mais de uma vez.
# 2. Permitir consultar o estado atual das operações em andamento.
# 3. Detectar encerramentos inesperados de processos.
# 4. Garantir a consistência dos dados durante operações concorrentes.
# 5. Implementar expiração automática de registros.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - Task
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule VotingSystem do
  @moduledoc """
  Sistema de votação concorrente utilizando GenServer para gerenciamento de estado
  e Task para operações assíncronas com garantia de consistência.
  """

  use GenServer

  # --- API Pública ---

  def start_link(_), do: GenServer.start_link(__MODULE__, %{}, name: __MODULE__)

  @doc "Registra um voto garantindo idempotência e validade temporal."
  def cast_vote(voter_id, option) do
    GenServer.call(__MODULE__, {:cast, voter_id, option})
  end

  @doc "Consulta o estado atual."
  def get_state, do: GenServer.call(__MODULE__, :get_state)

  # --- Callbacks do GenServer ---

  @impl true
  def init(_) do
    # Monitora o encerramento de processos filhos se necessário
    Process.flag(:trap_exit, true)
    {:ok, %{votes: %{}, timestamps: %{}}}
  end

  @impl true
  def handle_call({:cast, voter_id, option}, _from, state) do
    if Map.has_key?(state.votes, voter_id) do
      {:reply, {:error, :already_voted}, state}
    else
      # Requisito 5: Expiração (limite de 1 hora para o registro)
      # Requisito 4: Consistência garantida pelo Serializador do GenServer
      new_votes = Map.put(state.votes, voter_id, option)
      new_timestamps = Map.put(state.timestamps, voter_id, System.system_time(:second))

      # Requisito 1: Bloqueio de duplicidade via estado atômico
      new_state = %{votes: new_votes, timestamps: new_timestamps}

      # Requisito 3: Dispara Task assíncrona para limpeza futura (expiração)
      Task.start(fn ->
        Process.sleep(3600_000)
        GenServer.cast(__MODULE__, {:expire, voter_id})
      end)

      {:reply, :ok, new_state}
    end
  end

  @impl true
  def handle_call(:get_state, _from, state) do
    {:reply, state.votes, state}
  end

  @impl true
  def handle_cast({:expire, voter_id}, state) do
    new_votes = Map.delete(state.votes, voter_id)
    new_timestamps = Map.delete(state.timestamps, voter_id)
    {:noreply, %{votes: new_votes, timestamps: new_timestamps}}
  end

  # Requisito 3: Detectar encerramentos inesperados
  @impl true
  def handle_info({:EXIT, _from, reason}, state) do
    IO.puts("Processo monitorado encerrou: #{inspect(reason)}")
    {:noreply, state}
  end
end
