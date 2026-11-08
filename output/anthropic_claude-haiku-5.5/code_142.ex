# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de votação online, atendendo somente aos seguintes requisitos:
#
# 1. Persistir os dados para recuperação após reinicializações.
# 2. Implementar expiração automática de registros.
# 3. Fazer uso de macros.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# Sistema de votação online com persistência em DETS (biblioteca padrão),
# expiração automática de enquetes e uso de macros.
#
# Uso (arquivo único):
#   elixir voting_system.exs
#
# Ou em iex:
#   iex -S mix / iex> c("voting_system.exs")
#   {:ok, _} = VotingSystem.start_link(file: "votacao.dets")
#   {:ok, id} = VotingSystem.create_poll("Linguagem?", ["Elixir", "Erlang"], 60_000)
#   :ok = VotingSystem.vote(id, "alice", "Elixir")
#   {:ok, %{votes: votos}} = VotingSystem.results(id)

defmodule VotingSystem.Macros do
  @moduledoc """
  Macros que reduzem o boilerplate do GenServer.

    * `defcall/2` gera, a partir de um único bloco, a função pública da API
      (que faz `GenServer.call/2`) e a cláusula correspondente de `handle_call/3`.
    * `with_open_poll/2` verifica existência e expiração de uma enquete antes
      de executar o bloco, injetando a variável `poll` no escopo do chamador.
  """

  @doc """
  Define uma operação síncrona do servidor.

  O corpo deve retornar apenas a resposta; o estado é preservado automaticamente.
  """
  defmacro defcall({name, _meta, args}, do: body) do
    quote do
      # API pública (cliente)
      def unquote(name)(unquote_splicing(args)) do
        GenServer.call(__MODULE__, {unquote(name), unquote_splicing(args)})
      end

      # Handler (servidor). `var!` permite acessar `state` no escopo do chamador.
      def handle_call({unquote(name), unquote_splicing(args)}, _from, var!(state)) do
        {unquote(body), var!(state)}
      end
    end
  end

  @doc """
  Executa o bloco somente se a enquete existir e não estiver expirada.
  Injeta a variável `poll` no bloco.
  """
  defmacro with_open_poll(poll_id, do: body) do
    quote do
      case VotingSystem.fetch_open(unquote(poll_id)) do
        {:ok, var!(poll)} -> unquote(body)
        {:error, _reason} = error -> error
      end
    end
  end
end

defmodule VotingSystem do
  @moduledoc """
  Servidor de votação com:

    * Persistência: dados gravados em arquivo DETS e recuperados na
      reinicialização do processo.
    * Expiração automática: cada enquete tem um `expires_at`. Acessos a
      enquetes expiradas são recusados e uma varredura periódica remove
      enquetes expiradas juntamente com seus votos.
  """

  use GenServer
  import VotingSystem.Macros

  @table :voting_system_table
  @default_file "voting_system.dets"
  @sweep_interval_ms 5_000

  # ---------------------------------------------------------------------------
  # Inicialização
  # ---------------------------------------------------------------------------

  @doc """
  Inicia o servidor. Opções:
    * `:file` - caminho do arquivo de persistência (padrão: `#{@default_file}`).
  """
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    file = Keyword.get(opts, :file, @default_file)

    # Abre (ou cria) a tabela persistente. Dados existentes são recarregados.
    {:ok, @table} =
      :dets.open_file(@table, file: String.to_charlist(file), type: :set)

    # Garante fechamento limpo da tabela ao encerrar.
    Process.flag(:trap_exit, true)

    # Remove imediatamente registros expirados herdados de execuções anteriores.
    sweep()
    schedule_sweep()

    {:ok, %{}}
  end

  @impl true
  def terminate(_reason, _state) do
    :dets.close(@table)
    :ok
  end

  # ---------------------------------------------------------------------------
  # API pública (geradas pela macro defcall)
  # ---------------------------------------------------------------------------

  # Cria uma enquete. `ttl_ms` define o tempo de vida em milissegundos.
  defcall create_poll(question, options, ttl_ms) do
    cond do
      not valid_poll_args?(question, options, ttl_ms) ->
        {:error, :invalid_arguments}

      true ->
        id = new_id()

        poll = %{
          id: id,
          question: question,
          options: Enum.uniq(options),
          expires_at: now() + ttl_ms
        }

        put({:poll, id}, poll)
        {:ok, id}
    end
  end

  # Registra o voto de um eleitor. Cada eleitor vota apenas uma vez por enquete.
  defcall vote(poll_id, voter, option) do
    with_open_poll poll_id do
      cond do
        option not in poll.options ->
          {:error, :invalid_option}

        lookup({:vote, poll_id, voter}) != nil ->
          {:error, :already_voted}

        true ->
          put({:vote, poll_id, voter}, option)
          :ok
      end
    end
  end

  # Retorna a contagem de votos por opção.
  defcall results(poll_id) do
    with_open_poll poll_id do
      tally = Enum.frequencies(votes_for(poll_id))

      {:ok,
       %{
         id: poll.id,
         question: poll.question,
         expires_at: poll.expires_at,
         votes: Map.new(poll.options, &{&1, Map.get(tally, &1, 0)})
       }}
    end
  end

  # ---------------------------------------------------------------------------
  # Verificação de acesso (usada pela macro with_open_poll)
  # ---------------------------------------------------------------------------

  @doc false
  def fetch_open(poll_id) do
    case lookup({:poll, poll_id}) do
      nil ->
        {:error, :not_found}

      %{expires_at: expires_at} = poll ->
        if now() < expires_at, do: {:ok, poll}, else: {:error, :expired}
    end
  end

  # ---------------------------------------------------------------------------
  # Expiração automática
  # ---------------------------------------------------------------------------

  @impl true
  def handle_info(:sweep, state) do
    sweep()
    schedule_sweep()
    {:noreply, state}
  end

  defp schedule_sweep do
    Process.send_after(self(), :sweep, @sweep_interval_ms)
  end

  # Remove enquetes expiradas e todos os seus votos.
  defp sweep do
    current = now()

    expired_ids =
      :dets.foldl(
        fn
          {{:poll, id}, %{expires_at: expires_at}}, acc when expires_at <= current ->
            [id | acc]

          _entry, acc ->
            acc
        end,
        [],
        @table
      )

    Enum.each(expired_ids, fn id ->
      :dets.match_delete(@table, {{:vote, id, :_}, :_})
      :dets.delete(@table, {:poll, id})
    end)

    unless expired_ids == [], do: :dets.sync(@table)
    :ok
  end

  # ---------------------------------------------------------------------------
  # Camada de persistência (DETS)
  # ---------------------------------------------------------------------------

  defp put(key, value) do
    :ok = :dets.insert(@table, {key, value})
    # Força a gravação em disco para sobreviver a falhas/reinicializações.
    :ok = :dets.sync(@table)
  end

  defp lookup(key) do
    case :dets.lookup(@table, key) do
      [{^key, value}] -> value
      [] -> nil
    end
  end

  defp votes_for(poll_id) do
    for {_key, option} <- :dets.match_object(@table, {{:vote, poll_id, :_}, :_}),
        do: option
  end

  # ---------------------------------------------------------------------------
  # Utilidades
  # ---------------------------------------------------------------------------

  defp now, do: System.system_time(:millisecond)

  defp new_id do
    # Combina tempo do sistema (distinto entre reinicializações) com contador
    # monotônico do nó, reduzindo colisões de identificadores.
    Integer.to_string(System.system_time(:nanosecond), 36) <>
      Integer.to_string(System.unique_integer([:positive]), 36)
  end

  defp valid_poll_args?(question, options, ttl_ms) do
    is_binary(question) and
      is_list(options) and
      options != [] and
      Enum.all?(options, &is_binary/1) and
      is_integer(ttl_ms) and
      ttl_ms > 0
  end
end
