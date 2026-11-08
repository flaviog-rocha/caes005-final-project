# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de votação online, atendendo somente aos seguintes requisitos:
#
# 1. Implementar expiração automática de registros.
# 2. Impedir atualizações parcialmente concluídas.
# 3. Recuperar processos após falhas inesperadas.
# 4. Permitir cadastrar, consultar, atualizar e remover registros.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# Sistema de votação online com:
#   1. Expiração automática de registros (TTL), com varredura periódica e filtragem em leituras.
#   2. Atomicidade: cada alteração é aplicada por completo ou não é aplicada (tudo ou nada),
#      e só é confirmada na memória após a persistência em disco ter sucesso.
#   3. Recuperação após falhas: snapshot atômico em disco (escrita em .tmp + rename) e
#      supervisão com reinício automático do processo, que recarrega o último estado confirmado.
#   4. CRUD de registros: create, read, update e delete.
#
# Uso (exemplo em IEx):
#   {:ok, _} = Votacao.Sistema.start_link(caminho: "votacao.snapshot", varredura_ms: 500)
#   {:ok, id} = Votacao.criar_votacao(Votacao.Store, "Melhor linguagem", ["Elixir", "Erlang"], 60_000)
#   :ok = Votacao.votar(Votacao.Store, id, "Elixir", "eleitor-1")
#   {:error, :ja_votou} = Votacao.votar(Votacao.Store, id, "Erlang", "eleitor-1")
#   {:ok, %{"Elixir" => 1, "Erlang" => 0}} = Votacao.resultado(Votacao.Store, id)

defmodule Votacao.Store do
  @moduledoc """
  Armazenamento chave-valor com TTL, transações atômicas, persistência em snapshot
  e recuperação automática após falhas.

  Cada registro tem a forma:
      %{dados: term, expira_em: integer | :infinity, versao: pos_integer}
  """

  use GenServer
  require Logger

  @varredura_padrao_ms 1_000
  @caminho_padrao "votacao.snapshot"

  # ---------------------------------------------------------------------------
  # API pública
  # ---------------------------------------------------------------------------

  @doc "Inicia o processo de armazenamento (normalmente feito pelo supervisor)."
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc "Cadastra um registro com TTL em milissegundos (ou :infinity). Retorna {:ok, id}."
  def create(srv, dados, ttl_ms \\ :infinity) do
    id = gerar_id()
    expira = expiracao_para(ttl_ms)

    transaction(srv, fn registros ->
      novo = %{dados: dados, expira_em: expira, versao: 1}
      {:ok, Map.put(registros, id, novo), id}
    end)
  end

  @doc "Consulta um registro vivo. Retorna {:ok, dados} ou {:error, :nao_encontrado}."
  def read(srv, id) do
    GenServer.call(srv, {:ler, id})
  end

  @doc """
  Atualiza um registro de forma atômica. A função recebe os dados atuais e deve retornar
  {:ok, novos_dados} ou {:error, motivo}. Em caso de erro, nada é alterado.
  """
  def update(srv, id, fun) when is_function(fun, 1) do
    transaction(srv, fn registros ->
      case Map.fetch(registros, id) do
        {:ok, registro} ->
          case fun.(registro.dados) do
            {:ok, novos_dados} ->
              atualizado = %{registro | dados: novos_dados, versao: registro.versao + 1}
              {:ok, Map.put(registros, id, atualizado), novos_dados}

            {:error, _} = erro ->
              erro
          end

        :error ->
          {:error, :nao_encontrado}
      end
    end)
  end

  @doc "Remove um registro. Retorna :ok ou {:error, :nao_encontrado}."
  def delete(srv, id) do
    transaction(srv, fn registros ->
      if Map.has_key?(registros, id) do
        {:ok, Map.delete(registros, id), :ok}
      else
        {:error, :nao_encontrado}
      end
    end)
  end

  @doc """
  Executa uma transação atômica. A função recebe o mapa de registros vivos e deve retornar
  {:ok, novo_mapa, resultado} ou {:error, motivo}. O novo mapa só é confirmado se a
  persistência em disco for bem-sucedida; qualquer exceção descarta a transação inteira.
  """
  def transaction(srv, fun) when is_function(fun, 1) do
    GenServer.call(srv, {:transacao, fun})
  end

  # ---------------------------------------------------------------------------
  # Callbacks do GenServer
  # ---------------------------------------------------------------------------

  @impl true
  def init(opts) do
    caminho = Keyword.get(opts, :caminho, @caminho_padrao)
    varredura_ms = Keyword.get(opts, :varredura_ms, @varredura_padrao_ms)

    # Recuperação: carrega o último snapshot confirmado (se existir).
    registros = carregar(caminho) |> vivos(agora())

    estado = %{caminho: caminho, varredura_ms: varredura_ms, registros: registros}
    agendar_varredura(varredura_ms)
    {:ok, estado}
  end

  @impl true
  def handle_call({:ler, id}, _from, estado) do
    resposta =
      case Map.fetch(estado.registros, id) do
        {:ok, %{expira_em: expira, dados: dados}} ->
          if expirado?(expira, agora()), do: {:error, :nao_encontrado}, else: {:ok, dados}

        :error ->
          {:error, :nao_encontrado}
      end

    {:reply, resposta, estado}
  end

  def handle_call({:transacao, fun}, _from, estado) do
    case executar(fun, vivos(estado.registros, agora())) do
      {:ok, novos_registros, resultado} ->
        # Commit: só altera o estado em memória após persistir com sucesso.
        case persistir(estado.caminho, novos_registros) do
          :ok ->
            {:reply, {:ok, resultado}, %{estado | registros: novos_registros}}

          {:error, motivo} ->
            {:reply, {:error, {:persistencia, motivo}}, estado}
        end

      {:error, _} = erro ->
        {:reply, erro, estado}
    end
  end

  @impl true
  def handle_info(:varredura, estado) do
    vivos_agora = vivos(estado.registros, agora())

    estado =
      if map_size(vivos_agora) == map_size(estado.registros) do
        estado
      else
        case persistir(estado.caminho, vivos_agora) do
          :ok ->
            %{estado | registros: vivos_agora}

          {:error, motivo} ->
            # Registros expirados continuam invisíveis (filtrados), apenas não removidos do snapshot.
            Logger.error("Falha ao persistir varredura de expiração: #{inspect(motivo)}")
            estado
        end
      end

    agendar_varredura(estado.varredura_ms)
    {:noreply, estado}
  end

  # ---------------------------------------------------------------------------
  # Execução de transações
  # ---------------------------------------------------------------------------

  defp executar(fun, visao) do
    case fun.(visao) do
      {:ok, novos, resultado} when is_map(novos) -> {:ok, novos, resultado}
      {:error, _} = erro -> erro
      outro -> {:error, {:retorno_invalido, outro}}
    end
  rescue
    e -> {:error, {:excecao, Exception.message(e)}}
  catch
    tipo, motivo -> {:error, {tipo, motivo}}
  end

  # ---------------------------------------------------------------------------
  # Persistência (snapshot atômico)
  # ---------------------------------------------------------------------------

  # Escreve em arquivo temporário, sincroniza e renomeia. O rename é atômico no SO,
  # então o snapshot principal está sempre íntegro, mesmo que o processo caia no meio.
  defp persistir(caminho, registros) do
    tmp = caminho <> ".tmp"
    bin = :erlang.term_to_binary(registros)

    with :ok <- escrever_e_sincronizar(tmp, bin),
         :ok <- File.rename(tmp, caminho) do
      :ok
    end
  end

  defp escrever_e_sincronizar(arquivo, bin) do
    case File.open(arquivo, [:write, :binary]) do
      {:ok, io} ->
        try do
          with :ok <- IO.binwrite(io, bin), do: :file.sync(io)
        after
          File.close(io)
        end

      {:error, _} = erro ->
        erro
    end
  end

  defp carregar(caminho) do
    case File.read(caminho) do
      {:ok, bin} ->
        try do
          case :erlang.binary_to_term(bin) do
            mapa when is_map(mapa) -> mapa
            _ -> raise "formato de snapshot inválido"
          end
        rescue
          _ ->
            quarentena(caminho)
            %{}
        end

      {:error, :enoent} ->
        %{}

      {:error, motivo} ->
        raise "não foi possível ler #{caminho}: #{inspect(motivo)}"
    end
  end

  # Preserva um snapshot corrompido para análise e inicia do zero.
  defp quarentena(caminho) do
    destino = "#{caminho}.corrompido-#{System.system_time(:second)}"
    _ = File.rename(caminho, destino)
    Logger.warning("Snapshot corrompido movido para #{destino}; iniciando vazio")
    :ok
  end

  # ---------------------------------------------------------------------------
  # Utilitários de TTL e identificação
  # ---------------------------------------------------------------------------

  defp vivos(registros, agora) do
    for {id, r} <- registros, not expirado?(r.expira_em, agora), into: %{}, do: {id, r}
  end

  defp expirado?(:infinity, _agora), do: false
  defp expirado?(expira, agora), do: agora >= expira

  defp expiracao_para(:infinity), do: :infinity
  defp expiracao_para(ms) when is_integer(ms) and ms > 0, do: agora() + ms
  defp expiracao_para(outro), do: raise(ArgumentError, "TTL inválido: #{inspect(outro)}")

  defp agora, do: System.os_time(:millisecond)

  defp gerar_id do
    "#{System.os_time(:nanosecond)}-#{System.unique_integer([:positive])}"
  end

  defp agendar_varredura(ms), do: Process.send_after(self(), :varredura, ms)
end

defmodule Votacao do
  @moduledoc """
  Regras de negócio de votação sobre o armazenamento transacional.
  Operações que envolvem mais de uma alteração (ex.: contar o voto e registrar o votante)
  ocorrem em uma única transação, evitando estados parcialmente concluídos.
  """

  alias Votacao.Store

  @doc "Cria uma votação com opções e TTL opcional (ms). Retorna {:ok, id}."
  def criar_votacao(srv, titulo, opcoes, ttl_ms \\ :infinity)
      when is_binary(titulo) and is_list(opcoes) and opcoes != [] do
    contagens = opcoes |> Enum.uniq() |> Map.new(&{&1, 0})

    Store.create(
      srv,
      %{tipo: :votacao, titulo: titulo, opcoes: contagens, votantes: MapSet.new()},
      ttl_ms
    )
  end

  @doc """
  Registra um voto. Incremento da opção e registro do votante são feitos atomicamente:
  ou ambos ocorrem, ou nenhum.
  """
  def votar(srv, votacao_id, opcao, votante) do
    Store.update(srv, votacao_id, fn
      %{tipo: :votacao, opcoes: opcoes, votantes: votantes} = votacao ->
        cond do
          not Map.has_key?(opcoes, opcao) ->
            {:error, :opcao_invalida}

          MapSet.member?(votantes, votante) ->
            {:error, :ja_votou}

          true ->
            {:ok,
             %{
               votacao
               | opcoes: Map.update!(opcoes, opcao, &(&1 + 1)),
                 votantes: MapSet.put(votantes, votante)
             }}
        end

      _ ->
        {:error, :nao_e_votacao}
    end)
  end

  @doc "Retorna a contagem atual de votos por opção."
  def resultado(srv, votacao_id) do
    case Store.read(srv, votacao_id) do
      {:ok, %{tipo: :votacao, opcoes: opcoes}} -> {:ok, opcoes}
      {:ok, _} -> {:error, :nao_e_votacao}
      {:error, _} = erro -> erro
    end
  end

  @doc "Altera o título de uma votação, sem alterar votos."
  def alterar_titulo(srv, votacao_id, novo_titulo) when is_binary(novo_titulo) do
    Store.update(srv, votacao_id, fn
      %{tipo: :votacao} = votacao -> {:ok, %{votacao | titulo: novo_titulo}}
      _ -> {:error, :nao_e_votacao}
    end)
  end

  @doc "Remove uma votação."
  def remover_votacao(srv, votacao_id), do: Store.delete(srv, votacao_id)
end

defmodule Votacao.Sistema do
  @moduledoc """
  Supervisor da aplicação. Se o Store falhar inesperadamente, ele é reiniciado
  e recarrega o último snapshot confirmado em disco.
  """

  use Supervisor

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    children = [{Votacao.Store, opts}]
    Supervisor.init(children, strategy: :one_for_one, max_restarts: 10, max_seconds: 10)
  end
end
