# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de entregas e rastreamento, atendendo somente aos seguintes requisitos:
#
# 1. Recuperar processos após falhas inesperadas.
# 2. Persistir os dados para recuperação após reinicializações.
# 3. Diferenciar erros de validação de falhas operacionais.
# 4. Garantir a consistência dos dados durante operações concorrentes.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - Supervisor
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# Arquivo autocontido: gerenciamento de entregas e rastreamento em Elixir.
# Execução: elixir entregas.exs

defmodule Entregas.Dominio do
  @moduledoc """
  Regras de negócio puras, sem efeitos colaterais.

  Erros de validação são sempre retornados como `{:error, {:validacao, mensagens}}`.
  Eles são uma condição esperada do cliente e nunca derrubam um processo.
  """

  # Transições de status permitidas (máquina de estados do rastreamento).
  @transicoes %{
    pendente: [:em_transito, :cancelada],
    em_transito: [:entregue, :extraviada],
    entregue: [],
    cancelada: [],
    extraviada: []
  }

  @doc "Valida os atributos e cria uma nova entrega no estado inicial."
  def criar(attrs, agora) when is_map(attrs) do
    erros =
      []
      |> exigir_texto(attrs, :id)
      |> exigir_texto(attrs, :origem)
      |> exigir_texto(attrs, :destino)

    case erros do
      [] ->
        {:ok,
         %{
           id: attrs.id,
           origem: attrs.origem,
           destino: attrs.destino,
           status: :pendente,
           versao: 1,
           eventos: [],
           criado_em: agora,
           atualizado_em: agora
         }}

      _ ->
        {:error, {:validacao, Enum.reverse(erros)}}
    end
  end

  def criar(_attrs, _agora), do: {:error, {:validacao, ["atributos devem ser um mapa"]}}

  @doc "Aplica uma transição de status validando a máquina de estados."
  def transicionar(%{status: atual} = entrega, novo_status, agora) do
    if novo_status in Map.get(@transicoes, atual, []) do
      evento = %{de: atual, para: novo_status, em: agora}

      {:ok,
       %{
         entrega
         | status: novo_status,
           versao: entrega.versao + 1,
           eventos: [evento | entrega.eventos],
           atualizado_em: agora
       }}
    else
      {:error, {:validacao, ["transição inválida de #{atual} para #{inspect(novo_status)}"]}}
    end
  end

  defp exigir_texto(erros, attrs, campo) do
    case Map.get(attrs, campo) do
      valor when is_binary(valor) and byte_size(valor) > 0 -> erros
      _ -> ["#{campo} é obrigatório e deve ser texto não vazio" | erros]
    end
  end
end

defmodule Entregas.Armazenamento do
  @moduledoc """
  Processo dono do estado das entregas.

  - Todas as escritas passam por este processo, o que serializa operações
    concorrentes e garante consistência sem locks externos.
  - Controle otimista de versão evita atualizações perdidas entre clientes.
  - Cada commit grava um snapshot atômico (arquivo temporário + rename) e só
    atualiza o estado em memória após a persistência ter sucesso.
  - Se o processo falhar, o supervisor o reinicia e o estado é recarregado do disco.
  """

  use GenServer

  @caminho_padrao "dados/entregas.bin"

  # ---------------------------------------------------------------------------
  # API pública
  # ---------------------------------------------------------------------------

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :nome, __MODULE__))
  end

  def criar(attrs, srv \\ __MODULE__), do: GenServer.call(srv, {:criar, attrs})

  def registrar(id, novo_status, versao_esperada, srv \\ __MODULE__),
    do: GenServer.call(srv, {:registrar, id, novo_status, versao_esperada})

  def consultar(id, srv \\ __MODULE__), do: GenServer.call(srv, {:consultar, id})

  def listar(srv \\ __MODULE__), do: GenServer.call(srv, :listar)

  # ---------------------------------------------------------------------------
  # GenServer
  # ---------------------------------------------------------------------------

  @impl true
  def init(opts) do
    caminho = Keyword.get(opts, :caminho, @caminho_padrao)

    case carregar(caminho) do
      {:ok, entregas} -> {:ok, %{caminho: caminho, entregas: entregas}}
      # Falha operacional na recuperação: o supervisor tentará novamente.
      {:error, motivo} -> {:stop, {:falha_recuperacao, motivo}}
    end
  end

  @impl true
  def handle_call({:criar, attrs}, _from, estado) do
    with {:ok, entrega} <- Entregas.Dominio.criar(attrs, agora()),
         :ok <- nao_existe(estado.entregas, entrega.id),
         novo = Map.put(estado.entregas, entrega.id, entrega),
         :ok <- persistir(estado.caminho, novo) do
      {:reply, {:ok, entrega}, %{estado | entregas: novo}}
    else
      erro -> responder_erro(erro, estado)
    end
  end

  def handle_call({:registrar, id, novo_status, versao_esperada}, _from, estado) do
    with {:ok, entrega} <- buscar(estado.entregas, id),
         :ok <- verificar_versao(entrega, versao_esperada),
         {:ok, atualizada} <- Entregas.Dominio.transicionar(entrega, novo_status, agora()),
         novo = Map.put(estado.entregas, id, atualizada),
         :ok <- persistir(estado.caminho, novo) do
      {:reply, {:ok, atualizada}, %{estado | entregas: novo}}
    else
      erro -> responder_erro(erro, estado)
    end
  end

  def handle_call({:consultar, id}, _from, estado) do
    {:reply, buscar(estado.entregas, id), estado}
  end

  def handle_call(:listar, _from, estado) do
    {:reply, {:ok, Map.values(estado.entregas)}, estado}
  end

  # ---------------------------------------------------------------------------
  # Auxiliares
  # ---------------------------------------------------------------------------

  # Separa erros de validação/conflito (cliente) de falhas operacionais (sistema).
  defp responder_erro({:error, {tipo, _} = erro}, estado) when tipo in [:validacao, :conflito] do
    {:reply, {:error, erro}, estado}
  end

  defp responder_erro({:error, motivo}, estado) do
    {:reply, {:error, {:operacional, motivo}}, estado}
  end

  defp buscar(entregas, id) do
    case Map.fetch(entregas, id) do
      {:ok, entrega} -> {:ok, entrega}
      :error -> {:error, {:validacao, ["entrega #{inspect(id)} não encontrada"]}}
    end
  end

  defp nao_existe(entregas, id) do
    if Map.has_key?(entregas, id),
      do: {:error, {:validacao, ["já existe entrega com id #{inspect(id)}"]}},
      else: :ok
  end

  defp verificar_versao(%{versao: atual}, esperada) when atual == esperada, do: :ok

  defp verificar_versao(%{versao: atual}, esperada) do
    {:error, {:conflito, ["versão esperada #{esperada}, versão atual #{atual}"]}}
  end

  defp agora, do: System.system_time(:millisecond)

  # Escrita atômica: grava em arquivo temporário, sincroniza e renomeia.
  defp persistir(caminho, entregas) do
    tmp = caminho <> ".tmp"

    with :ok <- File.mkdir_p(Path.dirname(caminho)),
         :ok <- gravar_sincronizado(tmp, :erlang.term_to_binary(entregas)),
         :ok <- File.rename(tmp, caminho) do
      :ok
    end
  end

  defp gravar_sincronizado(arquivo, binario) do
    case File.open(arquivo, [:write, :binary]) do
      {:ok, io} ->
        try do
          with :ok <- IO.binwrite(io, binario), do: :file.sync(io)
        after
          File.close(io)
        end

      {:error, motivo} ->
        {:error, motivo}
    end
  end

  defp carregar(caminho) do
    case File.read(caminho) do
      {:ok, binario} ->
        try do
          {:ok, :erlang.binary_to_term(binario, [:safe])}
        rescue
          ArgumentError -> {:error, :dados_corrompidos}
        end

      {:error, :enoent} ->
        {:ok, %{}}

      {:error, motivo} ->
        {:error, motivo}
    end
  end
end

defmodule Entregas.Sistema do
  @moduledoc """
  Árvore de supervisão. Reinicia o armazenamento automaticamente após falhas
  inesperadas; o estado é recuperado do snapshot persistido em disco.
  """

  use Supervisor

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    children = [
      {Entregas.Armazenamento, opts}
    ]

    Supervisor.init(children, strategy: :one_for_one, max_restarts: 5, max_seconds: 10)
  end
end
