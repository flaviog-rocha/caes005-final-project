# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de estoque de uma loja, atendendo somente aos seguintes requisitos:
#
# 1. Processar grandes volumes de dados sem carregá-los integralmente na memória.
# 2. Permitir consultar o estado atual das operações em andamento.
# 3. Manter um histórico das operações realizadas.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# estoque.ex
#
# Gerenciamento de estoque com processamento em streaming, consulta de operações
# em andamento e histórico. Usa apenas a biblioteca padrão do Elixir/OTP.
#
# Formato de entrada (uma movimentação por linha, separador ";"):
#   SKU;entrada;quantidade
#   SKU;saida;quantidade
#
# Exemplo de uso (em iex):
#   {:ok, _sup} = Estoque.iniciar()
#   {:ok, id} = Estoque.processar_arquivo("/caminho/movimentos.csv", tamanho_lote: 1_000)
#   Estoque.status_operacao(id)
#   Estoque.operacoes_em_andamento()
#   Estoque.historico(10)
#   Estoque.saldo("SKU-123")

defmodule Estoque do
  @moduledoc """
  Fachada pública do sistema de estoque.

  Inicia a árvore de supervisão com:
    * `Estoque.Saldo`       - saldos atuais por SKU (fonte da verdade do estoque)
    * `Estoque.Operacoes`   - registro de operações em andamento e histórico
    * `Estoque.TarefaSup`   - supervisor das tarefas de processamento de arquivos
  """

  @doc "Inicia os processos do sistema sob um supervisor."
  def iniciar do
    filhos = [
      Estoque.Saldo,
      Estoque.Operacoes,
      {Task.Supervisor, name: Estoque.TarefaSup}
    ]

    Supervisor.start_link(filhos, strategy: :one_for_one, name: Estoque.Sup)
  end

  @doc """
  Inicia o processamento assíncrono de um arquivo de movimentações.
  Retorna `{:ok, id}` imediatamente; o progresso pode ser consultado via `status_operacao/1`.
  """
  def processar_arquivo(caminho, opts \\ []), do: Estoque.Processador.processar(caminho, opts)

  @doc "Consulta o estado atual (em andamento ou finalizada) de uma operação."
  def status_operacao(id), do: Estoque.Operacoes.status(id)

  @doc "Lista as operações que estão sendo processadas neste momento."
  def operacoes_em_andamento, do: Estoque.Operacoes.em_andamento()

  @doc "Retorna as últimas `n` operações finalizadas, da mais recente para a mais antiga."
  def historico(n \\ 20), do: Estoque.Operacoes.historico(n)

  @doc "Consulta o saldo atual de um SKU."
  def saldo(sku), do: Estoque.Saldo.consultar(sku)
end

defmodule Estoque.Saldo do
  @moduledoc """
  Mantém o saldo de cada SKU. Todas as escritas passam por este processo,
  garantindo consistência sem necessidade de locks externos.
  Lotes são aplicados de forma atômica (do ponto de vista de outras chamadas).
  """
  use GenServer

  def start_link(_opts \\ []), do: GenServer.start_link(__MODULE__, %{}, name: __MODULE__)

  @doc "Retorna o saldo atual do SKU (0 se nunca movimentado)."
  def consultar(sku), do: GenServer.call(__MODULE__, {:consultar, sku})

  @doc """
  Aplica um lote de movimentações. Cada item é `{:ok, linha, sku, delta}` ou
  `{:error, linha, motivo}`. Retorna `{quantidade_aplicada, rejeitadas}`, onde
  `rejeitadas` é uma lista de `{linha, motivo}`.
  Saídas que deixariam o saldo negativo são rejeitadas.
  """
  def aplicar_lote(movimentos) do
    GenServer.call(__MODULE__, {:lote, movimentos}, :infinity)
  end

  @impl true
  def init(saldos), do: {:ok, saldos}

  @impl true
  def handle_call({:consultar, sku}, _from, saldos) do
    {:reply, Map.get(saldos, sku, 0), saldos}
  end

  def handle_call({:lote, movimentos}, _from, saldos) do
    {saldos_novos, aplicados, rejeitadas} =
      Enum.reduce(movimentos, {saldos, 0, []}, fn
        {:error, linha, motivo}, {s, a, r} ->
          {s, a, [{linha, motivo} | r]}

        {:ok, linha, sku, delta}, {s, a, r} ->
          novo_saldo = Map.get(s, sku, 0) + delta

          if novo_saldo < 0 do
            {s, a, [{linha, :estoque_insuficiente} | r]}
          else
            {Map.put(s, sku, novo_saldo), a + 1, r}
          end
      end)

    {:reply, {aplicados, Enum.reverse(rejeitadas)}, saldos_novos}
  end
end

defmodule Estoque.Operacoes do
  @moduledoc """
  Registro de operações.

  * `ativas`    - operações em andamento, atualizadas incrementalmente (progresso).
  * `historico` - operações finalizadas (concluídas ou falhas), limitado a
                  `@limite_historico` entradas para não crescer indefinidamente.
  """
  use GenServer

  @limite_historico 500
  @limite_amostra 10

  def start_link(_opts \\ []), do: GenServer.start_link(__MODULE__, nil, name: __MODULE__)

  def iniciar(id, arquivo), do: GenServer.call(__MODULE__, {:iniciar, id, arquivo})

  # Assíncrono: progresso é informativo e não deve bloquear o processamento.
  def progresso(id, parcial), do: GenServer.cast(__MODULE__, {:progresso, id, parcial})

  def concluir(id), do: GenServer.call(__MODULE__, {:concluir, id})

  def falhar(id, motivo), do: GenServer.call(__MODULE__, {:falhar, id, motivo})

  def status(id), do: GenServer.call(__MODULE__, {:status, id})

  def em_andamento, do: GenServer.call(__MODULE__, :em_andamento)

  def historico(n \\ 20), do: GenServer.call(__MODULE__, {:historico, n})

  @impl true
  def init(_), do: {:ok, %{ativas: %{}, historico: []}}

  @impl true
  def handle_call({:iniciar, id, arquivo}, _from, st) do
    op = %{
      id: id,
      arquivo: arquivo,
      status: :em_andamento,
      iniciada_em: agora(),
      finalizada_em: nil,
      linhas: 0,
      aplicados: 0,
      rejeitados: 0,
      amostra_erros: [],
      motivo_falha: nil
    }

    {:reply, :ok, %{st | ativas: Map.put(st.ativas, id, op)}}
  end

  def handle_call({:concluir, id}, _from, st) do
    {resposta, st} = finalizar(st, id, :concluida, %{})
    {:reply, resposta, st}
  end

  def handle_call({:falhar, id, motivo}, _from, st) do
    {resposta, st} = finalizar(st, id, :falhou, %{motivo_falha: motivo})
    {:reply, resposta, st}
  end

  def handle_call({:status, id}, _from, st) do
    resposta =
      case Map.fetch(st.ativas, id) do
        {:ok, op} ->
          {:ok, op}

        :error ->
          case Enum.find(st.historico, &(&1.id == id)) do
            nil -> {:error, :nao_encontrada}
            op -> {:ok, op}
          end
      end

    {:reply, resposta, st}
  end

  def handle_call(:em_andamento, _from, st) do
    {:reply, Map.values(st.ativas), st}
  end

  def handle_call({:historico, n}, _from, st) do
    {:reply, Enum.take(st.historico, n), st}
  end

  @impl true
  def handle_cast({:progresso, id, parcial}, st) do
    case Map.fetch(st.ativas, id) do
      {:ok, op} ->
        op = %{
          op
          | linhas: op.linhas + parcial.linhas,
            aplicados: op.aplicados + parcial.aplicados,
            rejeitados: op.rejeitados + parcial.rejeitados,
            amostra_erros: Enum.take(op.amostra_erros ++ parcial.amostra, @limite_amostra)
        }

        {:noreply, %{st | ativas: Map.put(st.ativas, id, op)}}

      :error ->
        {:noreply, st}
    end
  end

  # Remove a operação das ativas e a insere no histórico.
  defp finalizar(st, id, status, extras) do
    case Map.pop(st.ativas, id) do
      {nil, _} ->
        {{:error, :nao_encontrada}, st}

      {op, ativas} ->
        op = Map.merge(op, Map.merge(%{status: status, finalizada_em: agora()}, extras))
        historico = Enum.take([op | st.historico], @limite_historico)
        {:ok, %{st | ativas: ativas, historico: historico}}
    end
  end

  defp agora, do: DateTime.utc_now()
end

defmodule Estoque.Processador do
  @moduledoc """
  Processa arquivos de movimentações em streaming.

  O arquivo é lido linha a linha (`File.stream!/1`), agrupado em lotes de
  tamanho fixo (`Stream.chunk_every/2`) e cada lote é aplicado ao saldo e
  reportado ao registro de operações antes de o próximo ser lido. Assim, o uso
  de memória é proporcional ao tamanho do lote, não ao tamanho do arquivo.
  """
  alias Estoque.{Operacoes, Saldo}

  @tamanho_lote_padrao 1_000

  @doc "Agenda o processamento do arquivo e retorna `{:ok, id}` imediatamente."
  def processar(caminho, opts \\ []) do
    tamanho = Keyword.get(opts, :tamanho_lote, @tamanho_lote_padrao)
    id = System.unique_integer([:positive, :monotonic])

    :ok = Operacoes.iniciar(id, caminho)

    {:ok, _pid} =
      Task.Supervisor.start_child(Estoque.TarefaSup, fn ->
        executar(id, caminho, tamanho)
      end)

    {:ok, id}
  end

  defp executar(id, caminho, tamanho) do
    try do
      caminho
      |> File.stream!()
      |> Stream.with_index(1)
      |> Stream.map(fn {linha, numero} -> interpretar(linha, numero) end)
      |> Stream.chunk_every(tamanho)
      |> Stream.each(fn lote ->
        {aplicados, rejeitadas} = Saldo.aplicar_lote(lote)

        Operacoes.progresso(id, %{
          linhas: length(lote),
          aplicados: aplicados,
          rejeitados: length(rejeitadas),
          amostra: Enum.take(rejeitadas, 10)
        })
      end)
      |> Stream.run()

      # Mesmo processo que enviou os progressos: a ordem das mensagens é preservada.
      Operacoes.concluir(id)
    rescue
      e -> Operacoes.falhar(id, Exception.message(e))
    end
  end

  # Converte uma linha bruta em {:ok, linha, sku, delta} ou {:error, linha, motivo}.
  defp interpretar(linha, numero) do
    case linha |> String.trim() |> String.split(";") do
      [sku, tipo, quantidade] when sku != "" ->
        case delta(tipo, quantidade) do
          {:ok, valor} -> {:ok, numero, sku, valor}
          {:error, motivo} -> {:error, numero, motivo}
        end

      _ ->
        {:error, numero, :formato_invalido}
    end
  end

  defp delta("entrada", quantidade), do: quantidade_inteira(quantidade, 1)
  defp delta("saida", quantidade), do: quantidade_inteira(quantidade, -1)
  defp delta(_tipo, _quantidade), do: {:error, :tipo_invalido}

  defp quantidade_inteira(texto, sinal) do
    case Integer.parse(texto) do
      {n, ""} when n > 0 -> {:ok, sinal * n}
      _ -> {:error, :quantidade_invalida}
    end
  end
end
