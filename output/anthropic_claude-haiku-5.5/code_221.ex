# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de reservas de salas, atendendo somente aos seguintes requisitos:
#
# 1. Garantir a consistência dos dados durante operações concorrentes.
# 2. Processar grandes volumes de dados sem carregá-los integralmente na memória.
# 3. Quando fizer sentido para o domínio, considerar a comunicação com um banco de dados fictício e, opcionalmente, o uso de Ecto.Migration, sem tornar isso obrigatório.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# reservas.ex
# Gerenciamento de reservas de salas: arquivo único, apenas biblioteca padrão.
#
# Garantias:
#   * Consistência sob concorrência: cada sala possui um único processo
#     (GenServer) que serializa "verificar conflito + gravar". Salas diferentes
#     são processadas em paralelo; a mesma sala nunca é escrita concorrentemente.
#   * Grandes volumes: importação e exportação usam File.stream!/Stream em lotes
#     (Stream.chunk_every) processados por Task.async_stream com back-pressure.
#     Nenhum conjunto completo de dados é carregado na memória.
#   * Persistência: comportamento Reservas.Repositorio. A implementação padrão usa
#     ETS (ordered_set). Pode ser trocada por configuração (:reservas, :repositorio),
#     por exemplo por um adaptador Ecto (ver exemplo comentado no final).

defmodule Reservas do
  @moduledoc """
  API pública do gerenciamento de reservas.

  Uso:

      {:ok, _} = Reservas.start_link()
      {:ok, _} = Reservas.reservar("sala-1", 1_700_000_000, 1_700_003_600, "Ana")
      {:error, :conflito} = Reservas.reservar("sala-1", 1_700_001_000, 1_700_002_000, "Bruno")
      stats = Reservas.importar_csv("reservas.csv")
      :ok = Reservas.exportar_csv("sala-1", "saida.csv")

  Horários são inteiros (segundos Unix), intervalo semiaberto [inicio, fim).
  Formato CSV: sala,inicio,fim,responsavel (com cabeçalho na primeira linha).
  """

  alias Reservas.{Booking, Sala, Repositorio}

  @lote_padrao 500

  @doc "Inicia a árvore de supervisão (repositório, registro de salas e supervisor dinâmico)."
  def start_link do
    children = [
      Repositorio.impl(),
      {Registry, keys: :unique, name: Reservas.Registro},
      {DynamicSupervisor, strategy: :one_for_one, name: Reservas.SalasSup}
    ]

    Supervisor.start_link(children, strategy: :one_for_one, name: Reservas.Supervisor)
  end

  @doc """
  Reserva uma sala. A verificação de conflito e a gravação ocorrem dentro do
  processo exclusivo da sala, então duas chamadas concorrentes para a mesma sala
  não podem ambas ser aceitas se houver sobreposição.
  """
  def reservar(sala, inicio, fim, responsavel) do
    with {:ok, booking} <- Booking.new(sala, inicio, fim, responsavel),
         {:ok, pid} <- sala_pid(sala) do
      Sala.reservar(pid, booking)
    end
  end

  @doc """
  Retorna um Stream lazy com as reservas da sala, ordenadas por início.
  Os dados são lidos em lotes diretamente do ETS, sem materialização completa.
  """
  def listar(sala), do: Repositorio.impl().stream_por_sala(sala)

  @doc """
  Importa reservas de um CSV em streaming.

  Opções:
    * `:lote` - tamanho de cada lote processado (padrão #{@lote_padrao}).

  Retorna `%{ok: n, erro: m}` com a contagem de linhas aceitas e rejeitadas.
  """
  def importar_csv(caminho, opts \\ []) do
    tamanho = Keyword.get(opts, :lote, @lote_padrao)

    caminho
    |> File.stream!()
    |> Stream.drop(1)
    |> Stream.map(&parse_linha/1)
    |> Stream.chunk_every(tamanho)
    |> Task.async_stream(&processar_lote/1,
      max_concurrency: System.schedulers_online(),
      timeout: :infinity,
      ordered: false
    )
    |> Enum.reduce(%{ok: 0, erro: 0}, fn {:ok, parcial}, acc ->
      %{ok: acc.ok + parcial.ok, erro: acc.erro + parcial.erro}
    end)
  end

  @doc "Exporta as reservas de uma sala para CSV, escrevendo em streaming."
  def exportar_csv(sala, caminho) do
    cabecalho = ["sala,inicio,fim,responsavel\n"]

    linhas =
      sala
      |> listar()
      |> Stream.map(fn {inicio, fim, responsavel} ->
        "#{sala},#{inicio},#{fim},#{responsavel}\n"
      end)

    cabecalho
    |> Stream.concat(linhas)
    |> Stream.into(File.stream!(caminho))
    |> Stream.run()
  end

  # ---------------------------------------------------------------------------
  # Internos
  # ---------------------------------------------------------------------------

  # Obtém (ou cria sob demanda) o processo exclusivo da sala.
  defp sala_pid(sala) do
    case Registry.lookup(Reservas.Registro, sala) do
      [{pid, _}] ->
        {:ok, pid}

      [] ->
        case DynamicSupervisor.start_child(Reservas.SalasSup, {Sala, sala}) do
          {:ok, pid} -> {:ok, pid}
          {:error, {:already_started, pid}} -> {:ok, pid}
          outro -> outro
        end
    end
  end

  # Cada lote é processado sequencialmente dentro de uma task; o paralelismo
  # vem de salas diferentes e do limite de max_concurrency.
  defp processar_lote(entradas) do
    Enum.reduce(entradas, %{ok: 0, erro: 0}, fn entrada, acc ->
      resultado =
        case entrada do
          {:ok, {sala, inicio, fim, responsavel}} -> reservar(sala, inicio, fim, responsavel)
          :erro -> {:error, :linha_invalida}
        end

      case resultado do
        {:ok, _} -> Map.update!(acc, :ok, &(&1 + 1))
        _ -> Map.update!(acc, :erro, &(&1 + 1))
      end
    end)
  end

  defp parse_linha(linha) do
    case linha |> String.trim() |> String.split(",", parts: 4) do
      [sala, inicio, fim, responsavel] ->
        with {i, ""} <- Integer.parse(inicio),
             {f, ""} <- Integer.parse(fim) do
          {:ok, {sala, i, f, responsavel}}
        else
          _ -> :erro
        end

      _ ->
        :erro
    end
  end
end

defmodule Reservas.Booking do
  @moduledoc "Entidade de reserva com validação do intervalo [inicio, fim)."

  @enforce_keys [:sala, :inicio, :fim, :responsavel]
  defstruct [:sala, :inicio, :fim, :responsavel]

  @type t :: %__MODULE__{
          sala: String.t(),
          inicio: integer(),
          fim: integer(),
          responsavel: String.t()
        }

  @doc "Cria uma reserva válida. O início deve ser estritamente menor que o fim."
  def new(sala, inicio, fim, responsavel)
      when is_binary(sala) and is_integer(inicio) and is_integer(fim) and
             inicio < fim and is_binary(responsavel) do
    {:ok, %__MODULE__{sala: sala, inicio: inicio, fim: fim, responsavel: responsavel}}
  end

  def new(_sala, _inicio, _fim, _responsavel), do: {:error, :dados_invalidos}
end

defmodule Reservas.Repositorio do
  @moduledoc """
  Contrato de persistência.

  Chave lógica: {sala, inicio}. Como as reservas de uma mesma sala não se
  sobrepõem (invariante mantida pelo processo da sala), basta consultar a
  reserva imediatamente anterior ao fim desejado para detectar conflitos.

  Implementação padrão: `Reservas.Repositorio.ETS` (memória).
  Para outro backend, configure: `config :reservas, :repositorio, MeuAdaptador`.
  Veja um exemplo de adaptador Ecto/migração ao final deste arquivo.
  """

  @callback inserir(Reservas.Booking.t()) :: :ok
  @callback ultima_antes(sala :: String.t(), fim :: integer()) ::
              {inicio :: integer(), fim :: integer(), responsavel :: String.t()} | nil
  @callback stream_por_sala(sala :: String.t()) :: Enumerable.t()

  @doc "Retorna o módulo de persistência configurado."
  def impl, do: Application.get_env(:reservas, :repositorio, Reservas.Repositorio.ETS)
end

defmodule Reservas.Repositorio.ETS do
  @moduledoc """
  Adaptador em memória usando ETS `:ordered_set` com chave {sala, inicio}.
  A tabela é pública (leituras concorrentes), mas as escritas de reservas
  ocorrem apenas nos processos de sala, o que preserva a consistência.
  """
  use GenServer
  @behaviour Reservas.Repositorio

  @tabela :reservas_bookings
  @lote 256

  def start_link(_arg), do: GenServer.start_link(__MODULE__, nil, name: __MODULE__)

  @impl GenServer
  def init(_arg) do
    :ets.new(@tabela, [
      :named_table,
      :ordered_set,
      :public,
      read_concurrency: true,
      write_concurrency: true
    ])

    {:ok, nil}
  end

  @impl Reservas.Repositorio
  def inserir(%Reservas.Booking{sala: sala, inicio: inicio, fim: fim, responsavel: resp}) do
    true = :ets.insert(@tabela, {{sala, inicio}, fim, resp})
    :ok
  end

  @impl Reservas.Repositorio
  def ultima_antes(sala, fim) do
    # prev/2 retorna a maior chave estritamente menor que {sala, fim}.
    case :ets.prev(@tabela, {sala, fim}) do
      {^sala, inicio} ->
        [{_, fim_ant, resp}] = :ets.lookup(@tabela, {sala, inicio})
        {inicio, fim_ant, resp}

      _ ->
        nil
    end
  end

  @impl Reservas.Repositorio
  def stream_por_sala(sala) do
    # Match spec com prefixo de chave literal: o ETS percorre apenas a faixa da sala.
    ms = [{{{sala, :"$1"}, :"$2", :"$3"}, [], [{{:"$1", :"$2", :"$3"}}]}]

    Stream.resource(
      fn -> :ets.select(@tabela, ms, @lote) end,
      fn
        :"$end_of_table" -> {:halt, nil}
        {itens, continuacao} -> {itens, :ets.select(continuacao)}
      end,
      fn _ -> :ok end
    )
  end
end

defmodule Reservas.Sala do
  @moduledoc """
  Processo exclusivo de uma sala. Serializa todas as operações de escrita
  daquela sala, garantindo que a verificação de conflito e a gravação sejam
  atômicas em relação a outras reservas concorrentes.
  """
  use GenServer, restart: :transient

  alias Reservas.Booking

  def start_link(sala) do
    GenServer.start_link(__MODULE__, sala, name: via(sala))
  end

  @impl GenServer
  def init(sala), do: {:ok, sala}

  @doc "Reserva dentro do processo da sala."
  def reservar(pid, %Booking{} = booking) do
    GenServer.call(pid, {:reservar, booking}, :infinity)
  end

  @impl GenServer
  def handle_call({:reservar, booking}, _from, sala) do
    resposta =
      if conflito?(booking) do
        {:error, :conflito}
      else
        :ok = Reservas.Repositorio.impl().inserir(booking)
        {:ok, booking}
      end

    {:reply, resposta, sala}
  end

  # Como as reservas não se sobrepõem, a reserva com maior início menor que o
  # fim solicitado é a única candidata a conflito: se ela terminar após o início
  # solicitado, há sobreposição.
  defp conflito?(%Booking{sala: sala, inicio: inicio, fim: fim}) do
    case Reservas.Repositorio.impl().ultima_antes(sala, fim) do
      nil -> false
      {_inicio_ant, fim_ant, _resp} -> fim_ant > inicio
    end
  end

  defp via(sala), do: {:via, Registry, {Reservas.Registro, sala}}
end

# ---------------------------------------------------------------------------
# Exemplo OPCIONAL de adaptador com Ecto (não compilado por padrão).
#
# Migração (priv/repo/migrations/*_criar_reservas.exs):
#
#   defmodule Reservas.Repo.Migrations.CriarReservas do
#     use Ecto.Migration
#
#     def change do
#       create table(:reservas, primary_key: false) do
#         add :sala, :string, null: false
#         add :inicio, :bigint, null: false
#         add :fim, :bigint, null: false
#         add :responsavel, :string, null: false
#       end
#
#       # Unicidade do início por sala e validade do intervalo, no banco.
#       create unique_index(:reservas, [:sala, :inicio])
#       create constraint(:reservas, :inicio_menor_que_fim, check: "inicio < fim")
#
#       # Em PostgreSQL, para impedir sobreposição totalmente no banco, use uma
#       # EXCLUDE constraint com btree_gist (sala com =, tsrange/int8range com &&).
#     end
#   end
#
# Adaptador (esboço):
#
#   defmodule Reservas.Repositorio.Ecto do
#     @behaviour Reservas.Repositorio
#     import Ecto.Query
#     @repo Reservas.Repo
#
#     def inserir(%Reservas.Booking{} = b) do
#       {:ok, _} = @repo.insert_all("reservas", [Map.from_struct(b)])
#       :ok
#     end
#
#     def ultima_antes(sala, fim) do
#       from(r in "reservas",
#         where: r.sala == ^sala and r.inicio < ^fim,
#         order_by: [desc: r.inicio],
#         limit: 1,
#         select: {r.inicio, r.fim, r.responsavel}
#       )
#       |> @repo.one()
#     end
#
#     def stream_por_sala(sala) do
#       # Dentro de uma transação, use @repo.stream/2 para leitura em lotes.
#       from(r in "reservas", where: r.sala == ^sala, order_by: r.inicio,
#            select: {r.inicio, r.fim, r.responsavel})
#       |> @repo.stream(max_rows: 500)
#     end
#   end
