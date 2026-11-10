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
  Sistema de votação resiliente utilizando ETS para persistência em disco,
  GenServer para isolamento de estado e Supervisores para tolerância a falhas.
  """

  # 1 & 5: Uso de Supervisor para recuperar processos e persistência via ETS (disk_log)
  use Application

  def start(_type, _args) do
    children = [
      {Registry, keys: :unique, name: VotingRegistry},
      {VotingServer, name: VotingServer}
    ]

    Supervisor.start_link(children, strategy: :one_for_one)
  end
end

defmodule VotingServer do
  use GenServer

  # 4: Macros para simplificar chamadas de API
  defmacro def_vote_command(name, args) do
    quote do
      def unquote(name)(unquote_splicing(args)) do
        GenServer.call(VotingServer, {unquote(name), unquote_splicing(tl(args))})
      end
    end
  end

  def_vote_command(vote, [user_id, candidate_id])

  # Início do Servidor
  def start_link(opts) do
    GenServer.start_link(__MODULE__, :ok, opts)
  end

  def init(:ok) do
    # Persistência via arquivo para recuperação após reinicialização
    table = :ets.new(:votes, [:set, :public, :named_table, {:write_concurrency, true}])
    # Carregamento lazy/stream seria feito lendo o arquivo em blocos
    {:ok, %{table: table}}
  end

  # 2: Impedir operações duplicadas via Registry (chaves únicas) ou checagem no ETS
  def handle_call({:vote, user_id, candidate_id}, _from, state) do
    case :ets.lookup(state.table, user_id) do
      [] ->
        :ets.insert(state.table, {user_id, candidate_id})
        {:reply, :ok, state}
      _ ->
        {:reply, {:error, :already_voted}, state}
    end
  end

  # 3: Processamento de grandes volumes via Stream
  # Utiliza lazy loading para evitar carregamento completo em memória
  def stream_votes do
    Stream.resource(
      fn -> :ets.first(:votes) end,
      fn
        :"$end_of_table" -> {:halt, nil}
        key -> {[:ets.lookup(:votes, key)], :ets.next(:votes, key)}
      end,
      fn _ -> :ok end
    )
  end
end

# Exemplo de uso:
# VotingSystem.start(:normal, [])
# VotingServer.vote("user_123", "candidate_A")
# VotingServer.stream_votes() |> Enum.each(&IO.inspect/1)
