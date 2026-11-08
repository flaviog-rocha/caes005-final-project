# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de entregas e rastreamento, atendendo somente aos seguintes requisitos:
#
# 1. Validar os dados recebidos antes de realizar operações.
# 2. Executar determinadas operações periodicamente.
# 3. Gerar notificações quando determinadas condições forem atingidas.
# 4. Processar grandes volumes de dados sem carregá-los integralmente na memória.
# 5. Impedir que uma mesma operação seja executada mais de uma vez.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - Stream
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# Arquivo único: gerenciamento de entregas e rastreamento.
# Requisitos: validação, execução periódica, notificações por condição,
# processamento em streaming (Stream) e idempotência.

defmodule Entregas.Validacao do
  @moduledoc """
  Valida e normaliza os dados de entrada antes de qualquer operação.
  Formato de linha esperado (CSV): id,status,prazo_iso8601,atualizado_em_iso8601
  """

  # Mapa explícito evita a criação de átomos a partir de dados externos.
  @status %{
    "pendente" => :pendente,
    "em_transito" => :em_transito,
    "entregue" => :entregue,
    "devolvida" => :devolvida
  }

  @doc "Valida uma linha bruta de CSV. Retorna {:ok, entrega} ou {:error, motivos}."
  def linha(linha) do
    case linha |> String.trim() |> String.split(",") |> Enum.map(&String.trim/1) do
      [id, status, prazo, atualizado] ->
        entrega(%{id: id, status: status, prazo: prazo, atualizado_em: atualizado})

      _ ->
        {:error, [{:formato, :numero_de_campos_invalido}]}
    end
  end

  @doc "Valida um mapa de entrega, acumulando todos os erros encontrados."
  def entrega(%{} = m) do
    checagens = %{
      id: id(Map.get(m, :id)),
      status: status(Map.get(m, :status)),
      prazo: data(Map.get(m, :prazo)),
      atualizado_em: data(Map.get(m, :atualizado_em))
    }

    erros = for {campo, {:error, motivo}} <- checagens, do: {campo, motivo}

    if erros == [] do
      {:ok, Map.new(checagens, fn {campo, {:ok, valor}} -> {campo, valor} end)}
    else
      {:error, erros}
    end
  end

  def entrega(_), do: {:error, [{:formato, :esperado_mapa}]}

  defp id(valor) when is_binary(valor) and byte_size(valor) > 0, do: {:ok, valor}
  defp id(_), do: {:error, :obrigatorio}

  defp status(valor) do
    case Map.fetch(@status, valor) do
      {:ok, status} -> {:ok, status}
      :error -> {:error, :status_invalido}
    end
  end

  defp data(valor) when is_binary(valor) do
    case DateTime.from_iso8601(valor) do
      {:ok, dt, _offset} -> {:ok, dt}
      _ -> {:error, :data_invalida}
    end
  end

  defp data(_), do: {:error, :data_invalida}
end

defmodule Entregas.Regras do
  @moduledoc """
  Condições que geram notificações. Função pura: não possui efeitos colaterais.
  """

  @doc "Retorna a lista de eventos {tipo, payload} para uma entrega validada."
  def avaliar(entrega, agora) do
    atrasada? =
      entrega.status in [:pendente, :em_transito] and
        DateTime.compare(agora, entrega.prazo) == :gt

    devolvida? = entrega.status == :devolvida

    [{atrasada?, :entrega_atrasada}, {devolvida?, :entrega_devolvida}]
    |> Enum.filter(fn {condicao, _tipo} -> condicao end)
    |> Enum.map(fn {_condicao, tipo} ->
      {tipo,
       %{
         # Chave que identifica unicamente o evento para evitar notificações repetidas.
         chave: {entrega.id, entrega.atualizado_em},
         id: entrega.id,
         status: entrega.status,
         prazo: entrega.prazo
       }}
    end)
  end
end

defmodule Entregas.Rastreador do
  @moduledoc """
  GenServer que processa periodicamente um arquivo de entregas.

  - Operações: cada execução é identificada por {caminho, janela_de_tempo};
    a mesma combinação nunca é executada duas vezes (idempotência).
  - Eventos: cada notificação é identificada por {tipo, chave_do_evento};
    o mesmo evento nunca é notificado duas vezes.
  - Streaming: o arquivo é lido linha a linha com File.stream!/1 e processado
    por uma cadeia de Stream, sem carregar o conteúdo inteiro na memória.
  """

  use GenServer
  require Logger

  @intervalo_padrao :timer.minutes(5)

  # ---------------------------------------------------------------------------
  # API pública
  # ---------------------------------------------------------------------------

  @doc """
  Opções:
    * `:caminho` (obrigatório) - arquivo CSV de entregas
    * `:intervalo_ms` - intervalo da execução periódica (padrão: 5 minutos)
    * `:notificar` - função de aridade 1 chamada para cada notificação
  """
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :nome, __MODULE__))
  end

  @doc "Executa o processamento sob demanda. Idempotente dentro da mesma janela."
  def processar(servidor \\ __MODULE__, caminho) do
    GenServer.call(servidor, {:processar, caminho}, :infinity)
  end

  # ---------------------------------------------------------------------------
  # Callbacks do GenServer
  # ---------------------------------------------------------------------------

  @impl true
  def init(opts) do
    # Tabela ETS privada: controle de idempotência de operações e eventos.
    tab = :ets.new(:entregas_execucoes, [:set, :private])

    state = %{
      tab: tab,
      caminho: Keyword.fetch!(opts, :caminho),
      intervalo_ms: Keyword.get(opts, :intervalo_ms, @intervalo_padrao),
      notificar: Keyword.get(opts, :notificar, &notificar_padrao/1)
    }

    agendar(state)
    {:ok, state}
  end

  @impl true
  def handle_info(:tick, state) do
    janela = janela_atual(state)
    resultado = executar(state, state.caminho, janela)
    Logger.info("Execução periódica (janela #{janela}): #{inspect(resultado)}")

    agendar(state)
    {:noreply, state}
  end

  @impl true
  def handle_call({:processar, caminho}, _from, state) do
    resultado = executar(state, caminho, janela_atual(state))
    {:reply, resultado, state}
  end

  # ---------------------------------------------------------------------------
  # Funções privadas
  # ---------------------------------------------------------------------------

  defp agendar(state) do
    Process.send_after(self(), :tick, state.intervalo_ms)
  end

  defp janela_atual(state) do
    div(System.system_time(:millisecond), state.intervalo_ms)
  end

  defp executar(state, caminho, janela) do
    chave = {:operacao, caminho, janela}

    cond do
      not File.regular?(caminho) ->
        {:error, :arquivo_invalido}

      # insert_new é atômico: só uma execução consegue registrar a operação.
      not :ets.insert_new(state.tab, {chave, :em_execucao}) ->
        {:ignorado, :ja_executada}

      true ->
        try do
          {:ok, processar_arquivo(state.tab, caminho, DateTime.utc_now(), state.notificar)}
        rescue
          e ->
            # Em caso de falha, libera a chave para permitir nova tentativa.
            :ets.delete(state.tab, chave)
            reraise e, __STACKTRACE__
        end
    end
  end

  # Pipeline lazy: nenhuma linha é materializada além da que está sendo processada.
  defp processar_arquivo(tab, caminho, agora, notificar) do
    caminho
    |> File.stream!()
    |> Stream.drop(1)
    |> Stream.with_index(2)
    |> Stream.flat_map(fn {linha, n} -> eventos(linha, n, agora) end)
    |> Stream.filter(&nova?(tab, &1))
    |> Stream.each(notificar)
    |> Enum.reduce(%{}, fn {tipo, _payload}, acc ->
      Map.update(acc, tipo, 1, &(&1 + 1))
    end)
  end

  defp eventos(linha, n, agora) do
    cond do
      String.trim(linha) == "" ->
        []

      true ->
        case Entregas.Validacao.linha(linha) do
          {:ok, entrega} ->
            Entregas.Regras.avaliar(entrega, agora)

          {:error, motivos} ->
            [
              {:linha_invalida,
               %{chave: {:linha, n, :erlang.phash2(linha)}, linha: n, motivos: motivos}}
            ]
        end
    end
  end

  # Idempotência de eventos: registra {tipo, chave} apenas na primeira ocorrência.
  defp nova?(tab, {tipo, %{chave: chave}}) do
    :ets.insert_new(tab, {{:evento, tipo, chave}, true})
  end

  defp notificar_padrao(evento) do
    Logger.warning("Notificação de entrega: #{inspect(evento)}")
  end
end
