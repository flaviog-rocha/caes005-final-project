# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de entregas e rastreamento, atendendo somente aos seguintes requisitos:
#
# 1. Quando fizer sentido para o domínio, considerar a comunicação com um banco de dados fictício e, opcionalmente, o uso de Ecto.Migration, sem tornar isso obrigatório.
# 2. Detectar encerramentos inesperados de processos.
# 3. Implementar expiração automática de registros.
# 4. Implementar novas tentativas após falhas temporárias.
# 5. Manter um histórico das operações realizadas.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# Arquivo único e autocontido: gerenciamento de entregas e rastreamento.
# Requisitos atendidos:
#   1. Persistência fictícia via behaviour (Entregas.Repo) com adaptador ETS.
#      Um adaptador Ecto pode ser plugado sem alterar o gerenciador (ver comentário no final).
#   2. Detecção de encerramentos inesperados via Process.monitor/2 e :DOWN.
#   3. Expiração automática de registros por varredura periódica.
#   4. Novas tentativas com backoff exponencial para falhas temporárias.
#   5. Histórico cronológico das operações realizadas.
#
# Para executar:
#   {:ok, _} = Entregas.Gerenciador.start_link()
#   :ok = Entregas.Gerenciador.criar(Entregas.Gerenciador, "PED-1", &minha_operacao/2)

defmodule Entregas.Repo do
  @moduledoc """
  Contrato de persistência (banco de dados fictício).

  Qualquer adaptador que implemente estes callbacks pode ser usado pelo gerenciador.
  """

  @doc "Inicializa o armazenamento. Retorna um handle usado nas demais chamadas."
  @callback init(opts :: keyword()) :: {:ok, handle :: term()}

  @doc "Insere ou substitui um registro."
  @callback put(handle :: term(), id :: term(), valor :: map()) :: :ok

  @doc "Busca um registro pelo id."
  @callback get(handle :: term(), id :: term()) :: {:ok, map()} | :error

  @doc "Remove um registro."
  @callback delete(handle :: term(), id :: term()) :: :ok

  @doc "Lista todos os registros."
  @callback all(handle :: term()) :: [map()]
end

defmodule Entregas.Repo.ETS do
  @moduledoc """
  Adaptador fictício de banco de dados baseado em ETS (biblioteca padrão).

  A tabela é privada ao processo que a criou (o gerenciador), portanto é
  destruída junto com ele.
  """
  @behaviour Entregas.Repo

  @impl true
  def init(_opts), do: {:ok, :ets.new(:entregas, [:set, :private])}

  @impl true
  def put(tab, id, valor) do
    :ets.insert(tab, {id, valor})
    :ok
  end

  @impl true
  def get(tab, id) do
    case :ets.lookup(tab, id) do
      [{^id, valor}] -> {:ok, valor}
      [] -> :error
    end
  end

  @impl true
  def delete(tab, id) do
    :ets.delete(tab, id)
    :ok
  end

  @impl true
  def all(tab), do: :ets.select(tab, [{{:_, :"$1"}, [], [:"$1"]}])
end

defmodule Entregas.Gerenciador do
  @moduledoc """
  Gerencia entregas, executa o rastreamento em processos isolados e aplica
  retentativas, expiração e histórico.

  Cada tentativa de rastreamento roda em um processo monitorado. O resultado
  é enviado ao gerenciador antes do término do processo; portanto, qualquer
  :DOWN recebido com a monitoração ainda ativa indica encerramento inesperado.

  Função de operação esperada (`operacao`):

      fn id, tentativa ->
        {:ok, dados}                      # sucesso
        {:error, :temporario, motivo}     # falha passível de nova tentativa
        {:error, :permanente, motivo}     # falha definitiva
      end

  Exceções lançadas pela operação são tratadas como falhas temporárias.
  Saídas abruptas (exit/kill) são detectadas como encerramentos inesperados.
  """
  use GenServer

  @ttl_padrao :timer.minutes(60)
  @intervalo_limpeza :timer.seconds(5)
  @max_tentativas 3
  @backoff_base 200
  @backoff_max 5_000

  # ---------------------------------------------------------------------------
  # API pública
  # ---------------------------------------------------------------------------

  @doc "Inicia o gerenciador. Opções: :name, :repo, :ttl, :max_tentativas, :intervalo_limpeza."
  def start_link(opts \\ []) do
    nome = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: nome)
  end

  @doc "Cria uma entrega e inicia imediatamente o rastreamento. Opções: :ttl, :max_tentativas."
  def criar(servidor, id, operacao, opts \\ []) do
    GenServer.call(servidor, {:criar, id, operacao, opts})
  end

  @doc "Consulta o estado atual de uma entrega."
  def consultar(servidor, id), do: GenServer.call(servidor, {:consultar, id})

  @doc "Retorna o histórico cronológico de eventos de uma entrega."
  def historico(servidor, id), do: GenServer.call(servidor, {:historico, id})

  # ---------------------------------------------------------------------------
  # Callbacks do GenServer
  # ---------------------------------------------------------------------------

  @impl true
  def init(opts) do
    repo = Keyword.get(opts, :repo, Entregas.Repo.ETS)
    {:ok, tab} = repo.init(opts)
    intervalo = Keyword.get(opts, :intervalo_limpeza, @intervalo_limpeza)
    Process.send_after(self(), :limpar, intervalo)

    {:ok,
     %{
       repo: repo,
       tab: tab,
       intervalo: intervalo,
       ttl: Keyword.get(opts, :ttl, @ttl_padrao),
       max_tentativas: Keyword.get(opts, :max_tentativas, @max_tentativas),
       # Mapa ref_do_monitor => id_da_entrega, para tentativas em andamento.
       monitores: %{},
       # Histórico global (mais recente primeiro). Em produção, limitar o tamanho
       # ou persistir em armazenamento apropriado.
       log: []
     }}
  end

  @impl true
  def handle_call({:criar, id, operacao, opts}, _from, state) do
    case state.repo.get(state.tab, id) do
      {:ok, _} ->
        {:reply, {:error, :ja_existe}, state}

      :error ->
        entrega = %{
          id: id,
          operacao: operacao,
          status: :pendente,
          tentativas: 0,
          max_tentativas: Keyword.get(opts, :max_tentativas, state.max_tentativas),
          expira_em: agora() + Keyword.get(opts, :ttl, state.ttl),
          pid: nil,
          ref: nil
        }

        state =
          state
          |> persistir(entrega)
          |> registrar(id, :criado)
          |> iniciar_tentativa(entrega)

        {:reply, :ok, state}
    end
  end

  def handle_call({:consultar, id}, _from, state) do
    resposta =
      case state.repo.get(state.tab, id) do
        {:ok, e} -> {:ok, Map.take(e, [:id, :status, :tentativas, :max_tentativas, :expira_em])}
        :error -> {:error, :nao_encontrada}
      end

    {:reply, resposta, state}
  end

  def handle_call({:historico, id}, _from, state) do
    eventos =
      state.log
      |> Enum.filter(&(&1.id == id))
      |> Enum.reverse()

    {:reply, eventos, state}
  end

  # Resultado enviado pelo processo de rastreamento antes de terminar.
  @impl true
  def handle_info({:resultado, id, resultado}, state) do
    case state.repo.get(state.tab, id) do
      {:ok, entrega} ->
        # Remove a monitoração antes que o :DOWN normal chegue.
        Process.demonitor(entrega.ref, [:flush])
        state = %{state | monitores: Map.delete(state.monitores, entrega.ref)}
        {:noreply, concluir(state, entrega, resultado)}

      :error ->
        # Entrega expirada enquanto o rastreamento estava em andamento.
        {:noreply, state}
    end
  end

  # Encerramento de processo monitorado (somente chega aqui se não houve resultado).
  def handle_info({:DOWN, ref, :process, _pid, motivo}, state) do
    case Map.pop(state.monitores, ref) do
      {nil, _} ->
        {:noreply, state}

      {id, monitores} ->
        state =
          %{state | monitores: monitores}
          |> registrar(id, :encerramento_inesperado, motivo)

        case state.repo.get(state.tab, id) do
          {:ok, entrega} ->
            {:noreply,
             falha_temporaria(state, limpar_vinculo(entrega), {:encerramento_inesperado, motivo})}

          :error ->
            {:noreply, state}
        end
    end
  end

  # Disparo de nova tentativa agendada.
  def handle_info({:retentar, id}, state) do
    case state.repo.get(state.tab, id) do
      {:ok, %{status: :aguardando_retry} = entrega} ->
        {:noreply, iniciar_tentativa(state, entrega)}

      _ ->
        {:noreply, state}
    end
  end

  # Varredura periódica de expiração.
  def handle_info(:limpar, state) do
    agora = agora()

    expirados =
      state.repo.all(state.tab)
      |> Enum.filter(&(&1.expira_em <= agora))

    state = Enum.reduce(expirados, state, &expirar/2)
    Process.send_after(self(), :limpar, state.intervalo)
    {:noreply, state}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  # ---------------------------------------------------------------------------
  # Fluxo interno
  # ---------------------------------------------------------------------------

  defp iniciar_tentativa(state, entrega) do
    gerenciador = self()
    id = entrega.id
    operacao = entrega.operacao
    tentativa = entrega.tentativas + 1

    {pid, ref} =
      spawn_monitor(fn ->
        send(gerenciador, {:resultado, id, executar(operacao, id, tentativa)})
      end)

    entrega = %{entrega | status: :em_rastreamento, tentativas: tentativa, pid: pid, ref: ref}

    state
    |> persistir(entrega)
    |> Map.update!(:monitores, &Map.put(&1, ref, id))
    |> registrar(id, :tentativa_iniciada, tentativa)
  end

  defp executar(operacao, id, tentativa) do
    try do
      case operacao.(id, tentativa) do
        {:ok, _} = ok -> ok
        {:error, tipo, _} = erro when tipo in [:temporario, :permanente] -> erro
        outro -> {:error, :permanente, {:resultado_invalido, outro}}
      end
    rescue
      e -> {:error, :temporario, {:excecao, Exception.message(e)}}
    end
  end

  defp concluir(state, entrega, {:ok, valor}) do
    entrega = %{limpar_vinculo(entrega) | status: :entregue}

    state
    |> persistir(entrega)
    |> registrar(entrega.id, :entregue, valor)
  end

  defp concluir(state, entrega, {:error, :temporario, motivo}) do
    falha_temporaria(state, limpar_vinculo(entrega), motivo)
  end

  defp concluir(state, entrega, {:error, :permanente, motivo}) do
    finalizar_falha(state, limpar_vinculo(entrega), motivo)
  end

  defp falha_temporaria(state, entrega, motivo) do
    if entrega.tentativas < entrega.max_tentativas do
      atraso = backoff(entrega.tentativas)
      Process.send_after(self(), {:retentar, entrega.id}, atraso)

      entrega = %{entrega | status: :aguardando_retry}

      state
      |> persistir(entrega)
      |> registrar(entrega.id, :retentativa_agendada, %{
        motivo: motivo,
        tentativa: entrega.tentativas,
        atraso_ms: atraso
      })
    else
      finalizar_falha(state, entrega, {:tentativas_esgotadas, motivo})
    end
  end

  defp finalizar_falha(state, entrega, motivo) do
    entrega = %{entrega | status: :falhou}

    state
    |> persistir(entrega)
    |> registrar(entrega.id, :falhou, motivo)
  end

  defp expirar(entrega, state) do
    if entrega.pid, do: Process.exit(entrega.pid, :kill)
    if entrega.ref, do: Process.demonitor(entrega.ref, [:flush])

    state.repo.delete(state.tab, entrega.id)

    %{state | monitores: Map.delete(state.monitores, entrega.ref)}
    |> registrar(entrega.id, :expirado, %{status_anterior: entrega.status})
  end

  # Backoff exponencial limitado: 200ms, 400ms, 800ms, ... até 5s.
  defp backoff(tentativas) do
    min(@backoff_base * Integer.pow(2, tentativas - 1), @backoff_max)
  end

  defp limpar_vinculo(entrega), do: %{entrega | pid: nil, ref: nil}

  defp persistir(state, entrega) do
    state.repo.put(state.tab, entrega.id, entrega)
    state
  end

  defp registrar(state, id, evento, detalhe \\ nil) do
    registro = %{at: agora(), id: id, evento: evento, detalhe: detalhe}
    %{state | log: [registro | state.log]}
  end

  defp agora, do: System.system_time(:millisecond)
end

# -----------------------------------------------------------------------------
# Opcional: adaptador Ecto (não obrigatório).
#
# Para usar Ecto, implemente um módulo `Entregas.Repo.Ecto` com @behaviour
# Entregas.Repo delegando para um Ecto.Repo, e passe `repo: Entregas.Repo.Ecto`
# ao iniciar o gerenciador. Migration correspondente, em priv/repo/migrations:
#
#   defmodule MyApp.Repo.Migrations.CreateEntregas do
#     use Ecto.Migration
#
#     def change do
#       create table(:entregas, primary_key: false) do
#         add :id, :string, primary_key: true
#         add :status, :string, null: false
#         add :tentativas, :integer, null: false, default: 0
#         add :max_tentativas, :integer, null: false
#         add :expira_em, :bigint, null: false
#         timestamps()
#       end
#
#       create index(:entregas, [:expira_em])
#     end
#   end
# -----------------------------------------------------------------------------
