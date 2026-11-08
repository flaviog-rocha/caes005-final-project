# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de troca de mensagens entre usuários, atendendo somente aos seguintes requisitos:
#
# 1. Implementar expiração automática de registros.
# 2. Persistir os dados para recuperação após reinicializações.
# 3. Executar determinadas operações periodicamente.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule TrocaMensagens do
  @moduledoc """
  Sistema de troca de mensagens entre usuários com:

    * expiração automática de mensagens (TTL) com filtragem na leitura e varredura periódica;
    * persistência em disco (snapshot atômico) para recuperação após reinicializações;
    * operações periódicas agendadas (limpeza de expiradas) via `Process.send_after/3`.

  Usa apenas a biblioteca padrão. Exemplo de uso:

      {:ok, _} = TrocaMensagens.start_link(arquivo: "/tmp/msgs.dat", intervalo_limpeza_ms: 1_000)
      {:ok, id} = TrocaMensagens.enviar(TrocaMensagens, "ana", "bruno", "oi", 5_000)
      [%TrocaMensagens.Mensagem{} = m] = TrocaMensagens.caixa_entrada(TrocaMensagens, "bruno")
  """

  use GenServer

  @arquivo_padrao "troca_mensagens.dat"
  @intervalo_limpeza_padrao 5_000
  @ttl_padrao 60_000
  @versao_formato 1

  defmodule Mensagem do
    @moduledoc "Representa uma mensagem armazenada."

    @enforce_keys [:id, :de, :para, :corpo, :criada_em, :expira_em]
    defstruct [:id, :de, :para, :corpo, :criada_em, :expira_em]

    @type t :: %__MODULE__{
            id: pos_integer(),
            de: String.t(),
            para: String.t(),
            corpo: String.t(),
            criada_em: integer(),
            expira_em: integer()
          }
  end

  ## ---------------------------------------------------------------
  ## API pública
  ## ---------------------------------------------------------------

  @doc """
  Inicia o processo.

  Opções:
    * `:nome` - nome de registro do processo (padrão: `#{inspect(__MODULE__)}`)
    * `:arquivo` - caminho do arquivo de persistência
    * `:intervalo_limpeza_ms` - periodicidade da varredura de expiradas
  """
  def start_link(opts \\ []) do
    nome = Keyword.get(opts, :nome, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: nome)
  end

  @doc "Envia uma mensagem com tempo de vida `ttl_ms` em milissegundos."
  def enviar(servidor, de, para, corpo, ttl_ms \\ @ttl_padrao)
      when is_binary(de) and is_binary(para) and is_binary(corpo) and
             is_integer(ttl_ms) and ttl_ms > 0 do
    GenServer.call(servidor, {:enviar, de, para, corpo, ttl_ms})
  end

  @doc "Retorna as mensagens vigentes (não expiradas) recebidas pelo usuário."
  def caixa_entrada(servidor, usuario) when is_binary(usuario) do
    GenServer.call(servidor, {:caixa_entrada, usuario})
  end

  @doc "Lê uma mensagem específica, somente se for destinada ao usuário e estiver vigente."
  def ler(servidor, usuario, id) do
    GenServer.call(servidor, {:ler, usuario, id})
  end

  @doc "Remove uma mensagem destinada ao usuário."
  def apagar(servidor, usuario, id) do
    GenServer.call(servidor, {:apagar, usuario, id})
  end

  ## ---------------------------------------------------------------
  ## Callbacks do GenServer
  ## ---------------------------------------------------------------

  @impl true
  def init(opts) do
    arquivo = Keyword.get(opts, :arquivo, @arquivo_padrao)
    intervalo = Keyword.get(opts, :intervalo_limpeza_ms, @intervalo_limpeza_padrao)

    {mensagens, proximo_id} = carregar(arquivo)

    estado = %{
      arquivo: arquivo,
      intervalo: intervalo,
      mensagens: mensagens,
      proximo_id: proximo_id
    }

    agendar_limpeza(intervalo)
    {:ok, estado}
  end

  @impl true
  def handle_call({:enviar, de, para, corpo, ttl_ms}, _from, estado) do
    agora = agora()
    id = estado.proximo_id

    mensagem = %Mensagem{
      id: id,
      de: de,
      para: para,
      corpo: corpo,
      criada_em: agora,
      expira_em: agora + ttl_ms
    }

    novo = %{
      estado
      | mensagens: Map.put(estado.mensagens, id, mensagem),
        proximo_id: id + 1
    }

    confirmar_e_responder({:ok, id}, novo, estado)
  end

  def handle_call({:caixa_entrada, usuario}, _from, estado) do
    agora = agora()

    caixa =
      estado.mensagens
      |> Map.values()
      |> Enum.filter(&(&1.para == usuario and vigente?(&1, agora)))
      |> Enum.sort_by(& &1.id)

    {:reply, caixa, estado}
  end

  def handle_call({:ler, usuario, id}, _from, estado) do
    agora = agora()

    case Map.get(estado.mensagens, id) do
      %Mensagem{para: ^usuario} = m ->
        if vigente?(m, agora),
          do: {:reply, {:ok, m}, estado},
          else: {:reply, {:error, :nao_encontrada}, estado}

      _ ->
        {:reply, {:error, :nao_encontrada}, estado}
    end
  end

  def handle_call({:apagar, usuario, id}, _from, estado) do
    case Map.get(estado.mensagens, id) do
      %Mensagem{para: ^usuario} ->
        novo = %{estado | mensagens: Map.delete(estado.mensagens, id)}
        confirmar_e_responder(:ok, novo, estado)

      _ ->
        {:reply, {:error, :nao_encontrada}, estado}
    end
  end

  @impl true
  def handle_info(:limpar, estado) do
    # Operação periódica: remove mensagens expiradas e reagenda a próxima execução.
    agendar_limpeza(estado.intervalo)
    novo = remover_expiradas(estado, agora())

    if map_size(novo.mensagens) == map_size(estado.mensagens) do
      {:noreply, estado}
    else
      case persistir(novo) do
        :ok ->
          {:noreply, novo}

        {:error, motivo} ->
          # Mantém o estado anterior; a próxima varredura tentará novamente.
          IO.warn("TrocaMensagens: falha ao persistir limpeza: #{inspect(motivo)}")
          {:noreply, estado}
      end
    end
  end

  def handle_info(_msg, estado), do: {:noreply, estado}

  ## ---------------------------------------------------------------
  ## Funções auxiliares (puras, exceto persistência e agendamento)
  ## ---------------------------------------------------------------

  defp agora, do: System.os_time(:millisecond)

  defp vigente?(%Mensagem{expira_em: expira_em}, agora), do: expira_em > agora

  defp remover_expiradas(estado, agora) do
    vivas =
      estado.mensagens
      |> Enum.filter(fn {_id, m} -> vigente?(m, agora) end)
      |> Map.new()

    %{estado | mensagens: vivas}
  end

  defp agendar_limpeza(intervalo) do
    Process.send_after(self(), :limpar, intervalo)
  end

  # Persiste o novo estado antes de responder; em caso de falha, mantém o antigo.
  defp confirmar_e_responder(resposta, novo, antigo) do
    case persistir(novo) do
      :ok -> {:reply, resposta, novo}
      {:error, motivo} -> {:reply, {:error, {:persistencia, motivo}}, antigo}
    end
  end

  ## ---------------------------------------------------------------
  ## Persistência
  ## ---------------------------------------------------------------

  # Carrega o snapshot do disco. Se ausente ou corrompido, inicia vazio.
  defp carregar(arquivo) do
    with {:ok, bin} <- File.read(arquivo),
         {:ok, dados} <- decodificar(bin) do
      {dados.mensagens, dados.proximo_id}
    else
      _ -> {%{}, 1}
    end
  end

  defp decodificar(bin) do
    try do
      case :erlang.binary_to_term(bin, [:safe]) do
        %{versao: @versao_formato, mensagens: m, proximo_id: p} = dados
        when is_map(m) and is_integer(p) ->
          {:ok, %{dados | mensagens: m, proximo_id: p}}

        _ ->
          :invalido
      end
    rescue
      _ -> :invalido
    end
  end

  # Escrita atômica: grava em arquivo temporário, sincroniza e renomeia.
  defp persistir(estado) do
    tmp = estado.arquivo <> ".tmp"

    bin =
      :erlang.term_to_binary(%{
        versao: @versao_formato,
        mensagens: estado.mensagens,
        proximo_id: estado.proximo_id
      })

    with {:ok, io} <- File.open(tmp, [:write, :binary]) do
      resultado =
        with :ok <- IO.binwrite(io, bin),
             :ok <- :file.sync(io) do
          :ok
        end

      File.close(io)

      with :ok <- resultado do
        File.rename(tmp, estado.arquivo)
      end
    end
  end
end
