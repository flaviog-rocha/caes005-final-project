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

# Gerenciamento de entregas e rastreamento.
# Requisitos atendidos:
#   1. Validação dos dados antes de qualquer operação (Entregas.Validador)
#   2. Execução periódica via GenServer e Process.send_after (Entregas.Agendador)
#   3. Notificações quando condições são atingidas (Entregas.Processador)
#   4. Processamento lazy com Stream, sem carregar o arquivo inteiro (Entregas.Processador)
#   5. Idempotência via ETS com :ets.insert_new/2 (Entregas.Processador.executar_uma_vez/3)

defmodule Entregas.Validador do
  @moduledoc "Valida os dados de uma entrega antes de qualquer operação."

  @status ~w(postado em_transito saiu_para_entrega entregue extraviado)

  @doc "Retorna {:ok, entrega} ou {:error, [{campo, mensagem}]}."
  def validar(entrada) when is_map(entrada) do
    erros =
      for {campo, predicado, mensagem} <- regras(),
          not predicado.(Map.get(entrada, campo)),
          do: {campo, mensagem}

    case erros do
      [] -> {:ok, entrada}
      _ -> {:error, erros}
    end
  end

  def validar(_), do: {:error, [{:entrada, "deve ser um mapa"}]}

  defp regras do
    [
      {:id, &(is_binary(&1) and byte_size(&1) > 0), "id deve ser string não vazia"},
      {:status, &(&1 in @status), "status inválido"},
      {:atraso_min, &(is_integer(&1) and &1 >= 0), "atraso_min deve ser inteiro >= 0"},
      {:atualizado_em, &is_integer/1, "atualizado_em deve ser timestamp unix"}
    ]
  end
end

defmodule Entregas.Processador do
  @moduledoc """
  Processa a fonte de dados de forma preguiçosa (lazy) e aplica
  as regras de notificação com garantia de execução única.
  """

  @tamanho_lote 500

  @doc """
  Processa o arquivo linha a linha usando Stream.
  Retorna {:ok, resumo} ou {:error, :fonte_inexistente}.
  """
  def processar(caminho, tab, notificar) do
    if File.exists?(caminho) do
      resumo =
        caminho
        |> File.stream!()
        |> Stream.map(&parse_linha/1)
        |> Stream.map(&Entregas.Validador.validar/1)
        |> Stream.chunk_every(@tamanho_lote)
        |> Stream.flat_map(fn lote -> Enum.map(lote, &processar_item(&1, tab, notificar)) end)
        |> Enum.reduce(
          %{processadas: 0, duplicadas: 0, invalidas: 0, notificacoes: 0},
          &acumular/2
        )

      {:ok, resumo}
    else
      {:error, :fonte_inexistente}
    end
  end

  @doc """
  Executa `fun` somente se `chave` ainda não foi registrada no ETS.
  :ets.insert_new/2 é atômico, então duas execuções concorrentes
  não passam ambas pela verificação. Em caso de falha, a chave é
  removida para permitir nova tentativa.
  """
  def executar_uma_vez(tab, chave, fun) do
    if :ets.insert_new(tab, {chave, System.system_time(:second)}) do
      try do
        {:ok, fun.()}
      rescue
        e ->
          :ets.delete(tab, chave)
          reraise e, __STACKTRACE__
      end
    else
      :ja_executada
    end
  end

  defp processar_item({:error, _erros}, _tab, _notificar), do: :invalida

  defp processar_item({:ok, entrega}, tab, notificar) do
    chave = {:notificacoes, entrega.id, entrega.atualizado_em, entrega.status}

    case executar_uma_vez(tab, chave, fn -> notificar_condicoes(entrega, notificar) end) do
      {:ok, n} -> {:processada, n}
      :ja_executada -> :duplicada
    end
  end

  defp notificar_condicoes(entrega, notificar) do
    entrega
    |> condicoes()
    |> Enum.map(fn tipo ->
      notificar.({tipo, entrega})
      tipo
    end)
    |> length()
  end

  # Condições que geram notificação.
  defp condicoes(entrega) do
    regras = [
      {:extravio, &(&1.status == "extraviado")},
      {:atraso_critico, &(&1.atraso_min > 120)}
    ]

    for {tipo, condicao} <- regras, condicao.(entrega), do: tipo
  end

  defp acumular(:invalida, acc), do: %{acc | invalidas: acc.invalidas + 1}
  defp acumular(:duplicada, acc), do: %{acc | duplicadas: acc.duplicadas + 1}

  defp acumular({:processada, n}, acc) do
    %{acc | processadas: acc.processadas + 1, notificacoes: acc.notificacoes + n}
  end

  # Formato de linha: id;status;atraso_min;atualizado_em
  defp parse_linha(linha) do
    case linha |> String.trim() |> String.split(";") do
      [id, status, atraso, ts] ->
        %{id: id, status: status, atraso_min: inteiro(atraso), atualizado_em: inteiro(ts)}

      _ ->
        %{}
    end
  end

  defp inteiro(texto) do
    case Integer.parse(texto) do
      {n, ""} -> n
      _ -> nil
    end
  end
end

defmodule Entregas.Agendador do
  @moduledoc """
  Executa o processamento periodicamente.

  Opções:
    * :fonte        - caminho do arquivo de entregas (obrigatório)
    * :intervalo_ms - intervalo entre ciclos (padrão: 60_000)
    * :notificar    - função de um argumento recebendo {tipo, entrega}
    * :nome         - nome do processo (padrão: __MODULE__)
  """
  use GenServer
  require Logger

  alias Entregas.Processador

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :nome, __MODULE__))
  end

  @doc "Executa um ciclo imediatamente e retorna o resumo."
  def processar_agora(servidor \\ __MODULE__) do
    GenServer.call(servidor, :processar_agora, :infinity)
  end

  @impl true
  def init(opts) do
    estado = %{
      tab: :ets.new(:entregas_operacoes, [:set, :protected]),
      fonte: Keyword.fetch!(opts, :fonte),
      intervalo: Keyword.get(opts, :intervalo_ms, 60_000),
      notificar: Keyword.get(opts, :notificar, &notificar_padrao/1)
    }

    agendar(estado.intervalo)
    {:ok, estado}
  end

  @impl true
  def handle_call(:processar_agora, _from, estado) do
    {:reply, executar(estado), estado}
  end

  @impl true
  def handle_info(:tick, estado) do
    executar(estado)
    agendar(estado.intervalo)
    {:noreply, estado}
  end

  defp executar(estado) do
    case Processador.processar(estado.fonte, estado.tab, estado.notificar) do
      {:ok, resumo} ->
        Logger.info("Ciclo concluído: #{inspect(resumo)}")
        {:ok, resumo}

      {:error, motivo} = erro ->
        Logger.error("Falha no ciclo: #{inspect(motivo)}")
        erro
    end
  end

  defp agendar(intervalo) do
    Process.send_after(self(), :tick, intervalo)
  end

  defp notificar_padrao({tipo, entrega}) do
    Logger.warning(
      "[#{tipo}] entrega=#{entrega.id} status=#{entrega.status} atraso=#{entrega.atraso_min}min"
    )
  end
end
