# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de entregas e rastreamento, atendendo somente aos seguintes requisitos:
#
# 1. Permitir cadastrar, consultar, atualizar e remover registros.
# 2. Impedir que uma mesma operação seja executada mais de uma vez.
# 3. Permitir limitar a quantidade de operações simultâneas.
# 4. Quando fizer sentido para o domínio, considerar a comunicação com um banco de dados fictício e, opcionalmente, o uso de Ecto.Migration, sem tornar isso obrigatório.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# Arquivo único: gerenciamento de entregas e rastreamento.
# Usa apenas a biblioteca padrão do Elixir/OTP (GenServer, Supervisor, Process).
#
# Uso (exemplo, em iex):
#   {:ok, _} = Entregas.iniciar(max_simultaneas: 3)
#   {:ok, e} = Entregas.cadastrar(%{codigo_rastreio: "BR123", destinatario: "Ana",
#                                   endereco: "Rua A, 10"}, idempotencia: "req-1")
#   {:ok, e} = Entregas.atualizar(e.id, %{status: :em_transito}, idempotencia: "req-2")
#   {:ok, e} = Entregas.consultar(e.id)
#   {:ok, _} = Entregas.remover(e.id, idempotencia: "req-3")

defmodule Entregas.Entrega do
  @moduledoc """
  Estrutura de uma entrega e regras de validação dos seus campos.
  """

  @status_validos [:pendente, :em_transito, :entregue, :cancelado]
  @campos_editaveis [:codigo_rastreio, :destinatario, :endereco, :status]

  defstruct [
    :id,
    :codigo_rastreio,
    :destinatario,
    :endereco,
    :inserido_em,
    :atualizado_em,
    status: :pendente
  ]

  @type t :: %__MODULE__{}

  @doc "Cria uma nova entrega a partir de atributos, validando-os."
  def nova(attrs) do
    agora = DateTime.utc_now()

    %__MODULE__{inserido_em: agora, atualizado_em: agora}
    |> aplicar(attrs)
  end

  @doc "Aplica mudanças nos campos editáveis e revalida a entrega."
  def aplicar(%__MODULE__{} = entrega, attrs) do
    mudancas = attrs |> Map.new() |> Map.take(@campos_editaveis)

    entrega
    |> Map.merge(mudancas)
    |> Map.put(:atualizado_em, DateTime.utc_now())
    |> validar()
  end

  @doc "Valida os campos obrigatórios e o status."
  def validar(%__MODULE__{} = e) do
    cond do
      not texto_valido?(e.codigo_rastreio) -> {:error, {:invalido, :codigo_rastreio}}
      not texto_valido?(e.destinatario) -> {:error, {:invalido, :destinatario}}
      not texto_valido?(e.endereco) -> {:error, {:invalido, :endereco}}
      e.status not in @status_validos -> {:error, {:invalido, :status}}
      true -> {:ok, e}
    end
  end

  defp texto_valido?(valor), do: is_binary(valor) and byte_size(valor) > 0
end

defmodule Entregas.BancoFake do
  @moduledoc """
  Banco de dados fictício em memória (GenServer).

  Simula uma camada de persistência: gera IDs, garante unicidade do código
  de rastreio (como um índice único) e permite latência simulada.
  Pode ser substituído por um adaptador Ecto.Repo sem alterar a API pública.
  """

  use GenServer

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))
  end

  def inserir(entrega, servidor \\ __MODULE__),
    do: GenServer.call(servidor, {:inserir, entrega})

  def buscar(id, servidor \\ __MODULE__),
    do: GenServer.call(servidor, {:buscar, id})

  def listar(servidor \\ __MODULE__),
    do: GenServer.call(servidor, :listar)

  # `fun` recebe a entrega atual e retorna {:ok, nova} | {:error, motivo}.
  # A função roda dentro do "banco", o que torna a leitura-modificação-escrita atômica.
  def atualizar(id, fun, servidor \\ __MODULE__) when is_function(fun, 1),
    do: GenServer.call(servidor, {:atualizar, id, fun})

  def remover(id, servidor \\ __MODULE__),
    do: GenServer.call(servidor, {:remover, id})

  @impl true
  def init(opts) do
    estado = %{
      linhas: %{},
      proximo_id: 1,
      latencia_ms: Keyword.get(opts, :latencia_ms, 0)
    }

    {:ok, estado}
  end

  @impl true
  def handle_call({:inserir, entrega}, _from, estado) do
    simular_latencia(estado)

    if codigo_em_uso?(estado, entrega.codigo_rastreio, nil) do
      {:reply, {:error, :codigo_duplicado}, estado}
    else
      id = estado.proximo_id
      linha = %{entrega | id: id}

      novo_estado = %{
        estado
        | linhas: Map.put(estado.linhas, id, linha),
          proximo_id: id + 1
      }

      {:reply, {:ok, linha}, novo_estado}
    end
  end

  def handle_call({:buscar, id}, _from, estado) do
    case Map.fetch(estado.linhas, id) do
      {:ok, linha} -> {:reply, {:ok, linha}, estado}
      :error -> {:reply, {:error, :nao_encontrado}, estado}
    end
  end

  def handle_call(:listar, _from, estado) do
    linhas = estado.linhas |> Map.values() |> Enum.sort_by(& &1.id)
    {:reply, {:ok, linhas}, estado}
  end

  def handle_call({:atualizar, id, fun}, _from, estado) do
    simular_latencia(estado)

    case Map.fetch(estado.linhas, id) do
      :error ->
        {:reply, {:error, :nao_encontrado}, estado}

      {:ok, atual} ->
        case fun.(atual) do
          {:error, _} = erro ->
            {:reply, erro, estado}

          {:ok, nova} ->
            if codigo_em_uso?(estado, nova.codigo_rastreio, id) do
              {:reply, {:error, :codigo_duplicado}, estado}
            else
              novo_estado = %{estado | linhas: Map.put(estado.linhas, id, nova)}
              {:reply, {:ok, nova}, novo_estado}
            end
        end
    end
  end

  def handle_call({:remover, id}, _from, estado) do
    simular_latencia(estado)

    case Map.pop(estado.linhas, id) do
      {nil, _} ->
        {:reply, {:error, :nao_encontrado}, estado}

      {linha, linhas} ->
        {:reply, {:ok, linha}, %{estado | linhas: linhas}}
    end
  end

  defp codigo_em_uso?(estado, codigo, ignorar_id) do
    Enum.any?(estado.linhas, fn {id, linha} ->
      linha.codigo_rastreio == codigo and id != ignorar_id
    end)
  end

  defp simular_latencia(%{latencia_ms: ms}) when ms > 0, do: Process.sleep(ms)
  defp simular_latencia(_estado), do: :ok
end

defmodule Entregas.Idempotencia do
  @moduledoc """
  Garante que uma operação identificada por uma chave seja executada no máximo
  uma vez com sucesso.

  - Chave nova: executa e, se o resultado for `{:ok, _}`, armazena o resultado.
  - Chave já concluída: devolve o resultado armazenado, sem reexecutar.
  - Chave em andamento: devolve `{:error, :operacao_em_andamento}`.
  - Falha (`{:error, _}`, exceção ou morte do executor): libera a chave para nova tentativa.

  Observação: os registros ficam em memória. Em produção, persista-os
  (por exemplo, em uma tabela `operacoes_idempotentes`) com um índice único na chave.
  """

  use GenServer

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, Keyword.get(opts, :nome, :ok), name: __MODULE__)
  end

  @doc "Executa `fun` no máximo uma vez com sucesso para a `chave`."
  def executar(chave, fun) when is_function(fun, 0) do
    case GenServer.call(__MODULE__, {:reservar, chave}) do
      {:concluida, resultado} ->
        resultado

      :em_andamento ->
        {:error, :operacao_em_andamento}

      :reservada ->
        resultado =
          try do
            fun.()
          rescue
            e -> {:error, {:excecao, Exception.message(e)}}
          end

        GenServer.call(__MODULE__, {:finalizar, chave, resultado})
        resultado
    end
  end

  @impl true
  def init(_), do: {:ok, %{chaves: %{}}}

  @impl true
  def handle_call({:reservar, chave}, {pid, _}, estado) do
    case Map.get(estado.chaves, chave) do
      nil ->
        mref = Process.monitor(pid)
        chaves = Map.put(estado.chaves, chave, {:em_andamento, pid, mref})
        {:reply, :reservada, %{estado | chaves: chaves}}

      {:concluida, resultado} ->
        {:reply, {:concluida, resultado}, estado}

      {:em_andamento, _pid, _mref} ->
        {:reply, :em_andamento, estado}
    end
  end

  def handle_call({:finalizar, chave, resultado}, {pid, _}, estado) do
    case Map.get(estado.chaves, chave) do
      {:em_andamento, ^pid, mref} ->
        Process.demonitor(mref, [:flush])

        chaves =
          if match?({:ok, _}, resultado) do
            Map.put(estado.chaves, chave, {:concluida, resultado})
          else
            Map.delete(estado.chaves, chave)
          end

        {:reply, :ok, %{estado | chaves: chaves}}

      _ ->
        {:reply, :ignorado, estado}
    end
  end

  # Se o executor morrer antes de finalizar, a chave é liberada.
  @impl true
  def handle_info({:DOWN, mref, :process, _pid, _motivo}, estado) do
    chaves =
      estado.chaves
      |> Enum.reject(fn {_chave, valor} -> match?({:em_andamento, _, ^mref}, valor) end)
      |> Map.new()

    {:noreply, %{estado | chaves: chaves}}
  end
end

defmodule Entregas.Limitador do
  @moduledoc """
  Limita o número de operações executadas simultaneamente (semáforo).

  Chamadas excedentes ficam em fila FIFO até que um slot seja liberado.
  Os slots são associados ao processo que os adquiriu via monitor, então
  um slot é recuperado automaticamente se o processo morrer.
  """

  use GenServer

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, Keyword.get(opts, :max, 4), name: __MODULE__)
  end

  @doc "Executa `fun` respeitando o limite de operações simultâneas."
  def executar(fun) when is_function(fun, 0) do
    {:ok, token} = GenServer.call(__MODULE__, :adquirir, :infinity)

    try do
      fun.()
    after
      GenServer.cast(__MODULE__, {:liberar, token})
    end
  end

  @impl true
  def init(max) do
    {:ok, %{max: max, ativos: %{}, fila: :queue.new()}}
  end

  @impl true
  def handle_call(:adquirir, {pid, _} = from, estado) do
    if map_size(estado.ativos) < estado.max do
      {token, novo_estado} = conceder(estado, pid)
      {:reply, {:ok, token}, novo_estado}
    else
      fila = :queue.in({from, pid}, estado.fila)
      {:noreply, %{estado | fila: fila}}
    end
  end

  @impl true
  def handle_cast({:liberar, token}, estado) do
    {:noreply, liberar(estado, token)}
  end

  @impl true
  def handle_info({:DOWN, token, :process, _pid, _motivo}, estado) do
    {:noreply, liberar(estado, token)}
  end

  defp conceder(estado, pid) do
    token = Process.monitor(pid)
    {token, %{estado | ativos: Map.put(estado.ativos, token, pid)}}
  end

  defp liberar(estado, token) do
    if Map.has_key?(estado.ativos, token) do
      Process.demonitor(token, [:flush])
      estado = %{estado | ativos: Map.delete(estado.ativos, token)}

      case :queue.out(estado.fila) do
        {{:value, {from, pid}}, fila} ->
          {novo_token, estado} = conceder(%{estado | fila: fila}, pid)
          GenServer.reply(from, {:ok, novo_token})
          estado

        {:empty, _} ->
          estado
      end
    else
      estado
    end
  end
end

defmodule Entregas do
  @moduledoc """
  API pública para cadastro, consulta, atualização e remoção de entregas.

  - Operações de escrita aceitam a opção `:idempotencia` (chave única da operação).
    Com a mesma chave, a operação é executada no máximo uma vez com sucesso.
  - Por padrão, `cadastrar/2` usa como chave `{:cadastrar, codigo_rastreio}`.
  - Todas as escritas passam pelo `Entregas.Limitador`, que controla a concorrência.
  """

  alias Entregas.{BancoFake, Entrega, Idempotencia, Limitador}

  @doc """
  Inicia a árvore de processos. Opções:
    * `:max_simultaneas` - limite de operações simultâneas (padrão: 4)
    * `:latencia_ms` - latência simulada do banco fictício (padrão: 0)
  """
  def iniciar(opts \\ []) do
    Supervisor.start_link(
      [
        {BancoFake, latencia_ms: Keyword.get(opts, :latencia_ms, 0)},
        {Idempotencia, []},
        {Limitador, max: Keyword.get(opts, :max_simultaneas, 4)}
      ],
      strategy: :one_for_one,
      name: Entregas.Supervisor
    )
  end

  @doc "Cadastra uma nova entrega."
  def cadastrar(attrs, opts \\ []) do
    attrs = Map.new(attrs)
    chave = Keyword.get(opts, :idempotencia, {:cadastrar, Map.get(attrs, :codigo_rastreio)})

    executar(chave, fn ->
      with {:ok, entrega} <- Entrega.nova(attrs) do
        BancoFake.inserir(entrega)
      end
    end)
  end

  @doc "Consulta uma entrega pelo ID."
  def consultar(id), do: BancoFake.buscar(id)

  @doc "Lista todas as entregas."
  def listar, do: BancoFake.listar()

  @doc "Atualiza campos de uma entrega existente."
  def atualizar(id, attrs, opts \\ []) do
    attrs = Map.new(attrs)

    executar(Keyword.get(opts, :idempotencia), fn ->
      BancoFake.atualizar(id, fn atual -> Entrega.aplicar(atual, attrs) end)
    end)
  end

  @doc "Remove uma entrega pelo ID."
  def remover(id, opts \\ []) do
    executar(Keyword.get(opts, :idempotencia), fn ->
      BancoFake.remover(id)
    end)
  end

  # Sem chave, a operação roda apenas com controle de concorrência.
  defp executar(nil, operacao), do: Limitador.executar(operacao)

  # Com chave, a verificação de idempotência ocorre antes de consumir um slot.
  defp executar(chave, operacao) do
    Idempotencia.executar(chave, fn -> Limitador.executar(operacao) end)
  end
end

# ---------------------------------------------------------------------------
# Opcional: migração Ecto (não obrigatória). O código acima não depende de Ecto.
# Para persistir com Ecto, crie um arquivo em priv/repo/migrations/ com:
#
# defmodule Entregas.Repo.Migrations.CriarEntregas do
#   use Ecto.Migration
#
#   def change do
#     create table(:entregas) do
#       add :codigo_rastreio, :string, null: false
#       add :destinatario, :string, null: false
#       add :endereco, :string, null: false
#       add :status, :string, null: false, default: "pendente"
#       timestamps(inserted_at: :inserido_em, updated_at: :atualizado_em)
#     end
#
#     create unique_index(:entregas, [:codigo_rastreio])
#
#     create table(:operacoes_idempotentes) do
#       add :chave, :string, null: false
#       add :resultado, :binary
#     end
#
#     create unique_index(:operacoes_idempotentes, [:chave])
#   end
# end
