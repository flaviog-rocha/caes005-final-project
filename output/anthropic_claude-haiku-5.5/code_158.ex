# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de entregas e rastreamento, atendendo somente aos seguintes requisitos:
#
# 1. Impedir que uma mesma operação seja executada mais de uma vez.
# 2. Impedir atualizações parcialmente concluídas.
# 3. Quando fizer sentido para o domínio, considerar a comunicação com um banco de dados fictício e, opcionalmente, o uso de Ecto.Migration, sem tornar isso obrigatório.
# 4. Implementar novas tentativas após falhas temporárias.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# Arquivo único e autocontido: gerenciamento de entregas e rastreamento.
# Requisitos atendidos:
#   1. Idempotência: cada chave de operação é executada no máximo uma vez;
#      repetições devolvem o resultado original.
#   2. Atomicidade: cada transação é aplicada por completo ou não é aplicada.
#   3. Banco fictício em memória (GenServer). Não há dependência de Ecto.
#      Um Ecto.Migration equivalente poderia criar as tabelas `entregas`,
#      `historico` e `operacoes_idempotentes` (com índice único na chave),
#      mas isso é opcional e não é usado aqui.
#   4. Novas tentativas com backoff exponencial e jitter para falhas transitórias.
#
# Execução: `elixir entregas.exs`

defmodule Entregas.BancoFicticio do
  @moduledoc """
  Banco de dados fictício em memória.

  Toda leitura ou escrita ocorre dentro de uma transação. A função recebe o estado
  atual e devolve `{:ok, resultado, novo_estado}` ou `{:error, motivo}`.
  Em caso de sucesso, o novo estado substitui o antigo de uma só vez.
  Em caso de erro, o estado antigo é mantido, portanto não há atualização parcial.
  """

  use GenServer

  # ---------------------------------------------------------------------------
  # API pública
  # ---------------------------------------------------------------------------

  def start_link(opcoes \\ []) do
    nome = Keyword.get(opcoes, :name, __MODULE__)
    GenServer.start_link(__MODULE__, :ok, name: nome)
  end

  @doc "Executa `fun` atomicamente sobre o estado do banco."
  def transacao(fun, servidor \\ __MODULE__) when is_function(fun, 1) do
    GenServer.call(servidor, {:transacao, fun})
  end

  @doc """
  Simula falhas transitórias: as próximas `quantidade` transações são rejeitadas
  antes de qualquer alteração, como um banco temporariamente indisponível.
  """
  def injetar_falhas_transitorias(quantidade, servidor \\ __MODULE__) do
    GenServer.call(servidor, {:injetar_falhas, quantidade})
  end

  # ---------------------------------------------------------------------------
  # Callbacks do GenServer
  # ---------------------------------------------------------------------------

  @impl true
  def init(:ok) do
    estado = %{
      entregas: %{},
      historico: [],
      operacoes: %{},
      falhas_pendentes: 0
    }

    {:ok, estado}
  end

  @impl true
  def handle_call({:injetar_falhas, quantidade}, _de, estado) do
    {:reply, :ok, %{estado | falhas_pendentes: quantidade}}
  end

  # Falha transitória simulada: rejeita antes de executar, sem alterar dados.
  def handle_call({:transacao, _fun}, _de, %{falhas_pendentes: n} = estado) when n > 0 do
    resposta = {:error, {:transitorio, :banco_indisponivel}}
    {:reply, resposta, %{estado | falhas_pendentes: n - 1}}
  end

  def handle_call({:transacao, fun}, _de, estado) do
    # Exceções são tratadas como erros permanentes. O estado não é alterado.
    resposta =
      try do
        fun.(estado)
      rescue
        excecao -> {:error, {:excecao, excecao}}
      end

    case resposta do
      {:ok, resultado, novo_estado} ->
        # Commit: o novo estado substitui o antigo somente aqui.
        {:reply, {:ok, resultado}, novo_estado}

      {:error, motivo} ->
        # Rollback implícito: o estado antigo é mantido.
        {:reply, {:error, motivo}, estado}

      invalido ->
        {:reply, {:error, {:retorno_invalido, invalido}}, estado}
    end
  end
end

defmodule Entregas.Retentativa do
  @moduledoc """
  Repete operações que falharam por motivos transitórios, usando backoff
  exponencial com jitter. Somente `{:error, {:transitorio, _}}` gera nova tentativa.
  """

  @padroes [tentativas: 4, atraso_base_ms: 20, atraso_max_ms: 500]

  def executar(operacao, opcoes \\ []) when is_function(operacao, 0) do
    config = Keyword.merge(@padroes, opcoes)
    tentar(operacao, 1, config)
  end

  defp tentar(operacao, numero, config) do
    case operacao.() do
      {:error, {:transitorio, _}} = erro ->
        if numero < config[:tentativas] do
          Process.sleep(atraso(numero, config))
          tentar(operacao, numero + 1, config)
        else
          erro
        end

      resultado ->
        resultado
    end
  end

  # Backoff exponencial limitado, com jitter para evitar tentativas sincronizadas.
  defp atraso(numero, config) do
    exponencial = config[:atraso_base_ms] * Integer.pow(2, numero - 1)
    limitado = min(exponencial, config[:atraso_max_ms])
    limitado + :rand.uniform(max(config[:atraso_base_ms], 1))
  end
end

defmodule Entregas do
  @moduledoc """
  Regras de domínio para entregas e rastreamento.

  Operações de escrita recebem uma `chave` de idempotência. Essa chave é gravada
  na mesma transação da alteração, então a operação é aplicada no máximo uma vez.
  """

  alias Entregas.{BancoFicticio, Retentativa}

  # Máquina de estados: status atual => próximos status permitidos.
  @transicoes %{
    postada: [:em_transito, :cancelada],
    em_transito: [:saiu_para_entrega, :cancelada],
    saiu_para_entrega: [:entregue, :cancelada],
    entregue: [],
    cancelada: []
  }

  # ---------------------------------------------------------------------------
  # Comandos (escrita idempotente)
  # ---------------------------------------------------------------------------

  @doc "Registra uma nova entrega. `attrs` deve conter `:codigo` e pode conter `:destinatario`."
  def registrar_entrega(chave, %{codigo: codigo} = attrs) when is_binary(chave) and chave != "" do
    payload = {:registrar, attrs}

    executar_idempotente(chave, payload, fn estado ->
      if Map.has_key?(estado.entregas, codigo) do
        {:error, :codigo_ja_existe}
      else
        entrega = %{
          codigo: codigo,
          destinatario: Map.get(attrs, :destinatario),
          status: :postada
        }

        novo_estado = %{
          estado
          | entregas: Map.put(estado.entregas, codigo, entrega),
            historico: [{codigo, :postada} | estado.historico]
        }

        {:ok, entrega, novo_estado}
      end
    end)
  end

  @doc "Atualiza o status de uma única entrega."
  def atualizar_status(chave, codigo, novo_status) when is_binary(chave) do
    atualizar_lote(chave, [{codigo, novo_status}])
  end

  @doc """
  Atualiza várias entregas em uma única operação.
  Ou todas as mudanças são aplicadas, ou nenhuma é.
  """
  def atualizar_lote(chave, mudancas) when is_binary(chave) and is_list(mudancas) do
    payload = {:lote, mudancas}

    executar_idempotente(chave, payload, fn estado ->
      aplicar_lote(estado, mudancas)
    end)
  end

  # ---------------------------------------------------------------------------
  # Consultas
  # ---------------------------------------------------------------------------

  @doc "Retorna a entrega e a linha do tempo de eventos do código informado."
  def rastrear(codigo) do
    Retentativa.executar(fn ->
      BancoFicticio.transacao(fn estado ->
        case buscar(estado, codigo) do
          {:ok, entrega} ->
            eventos =
              estado.historico
              |> Enum.filter(fn {c, _status} -> c == codigo end)
              |> Enum.reverse()

            {:ok, %{entrega: entrega, eventos: eventos}, estado}

          erro ->
            erro
        end
      end)
    end)
  end

  # ---------------------------------------------------------------------------
  # Núcleo: idempotência + atomicidade + retentativa
  # ---------------------------------------------------------------------------

  defp executar_idempotente(chave, payload, operacao) do
    Retentativa.executar(fn ->
      BancoFicticio.transacao(fn estado ->
        case Map.fetch(estado.operacoes, chave) do
          # Já executada: devolve o resultado original sem repetir efeitos.
          {:ok, {^payload, resultado}} ->
            {:ok, resultado, estado}

          # Mesma chave com conteúdo diferente: possível erro do chamador.
          {:ok, {_outro_payload, _resultado}} ->
            {:error, :chave_reutilizada_com_payload_diferente}

          :error ->
            case operacao.(estado) do
              {:ok, resultado, novo_estado} ->
                # A chave é gravada na mesma transação da alteração.
                operacoes = Map.put(novo_estado.operacoes, chave, {payload, resultado})
                {:ok, resultado, %{novo_estado | operacoes: operacoes}}

              {:error, _motivo} = erro ->
                erro
            end
        end
      end)
    end)
  end

  # ---------------------------------------------------------------------------
  # Regras internas
  # ---------------------------------------------------------------------------

  defp aplicar_lote(estado, mudancas) do
    resultado =
      Enum.reduce_while(mudancas, {:ok, [], estado}, fn {codigo, status}, {:ok, feitas, acc} ->
        case aplicar_mudanca(acc, codigo, status) do
          {:ok, entrega, novo} -> {:cont, {:ok, [entrega | feitas], novo}}
          {:error, _motivo} = erro -> {:halt, erro}
        end
      end)

    case resultado do
      {:ok, feitas, novo_estado} -> {:ok, Enum.reverse(feitas), novo_estado}
      {:error, _motivo} = erro -> erro
    end
  end

  defp aplicar_mudanca(estado, codigo, novo_status) do
    with {:ok, entrega} <- buscar(estado, codigo),
         :ok <- validar_transicao(entrega.status, novo_status) do
      entrega_atualizada = %{entrega | status: novo_status}

      novo_estado = %{
        estado
        | entregas: Map.put(estado.entregas, codigo, entrega_atualizada),
          historico: [{codigo, novo_status} | estado.historico]
      }

      {:ok, entrega_atualizada, novo_estado}
    end
  end

  defp buscar(estado, codigo) do
    case Map.fetch(estado.entregas, codigo) do
      {:ok, entrega} -> {:ok, entrega}
      :error -> {:error, :nao_encontrada}
    end
  end

  defp validar_transicao(atual, novo) do
    if novo in Map.get(@transicoes, atual, []) do
      :ok
    else
      {:error, {:transicao_invalida, atual, novo}}
    end
  end
end

# ---------------------------------------------------------------------------
# Demonstração de uso
# ---------------------------------------------------------------------------

{:ok, _pid} = Entregas.BancoFicticio.start_link()

# 1. Idempotência: a mesma chave executada duas vezes gera apenas um registro.
{:ok, _} = Entregas.registrar_entrega("op-1", %{codigo: "BR001", destinatario: "Ana"})
{:ok, _} = Entregas.registrar_entrega("op-1", %{codigo: "BR001", destinatario: "Ana"})

# 2. Retentativa: falhas transitórias injetadas são superadas automaticamente.
Entregas.BancoFicticio.injetar_falhas_transitorias(2)
{:ok, _} = Entregas.atualizar_status("op-2", "BR001", :em_transito)

# 3. Atomicidade: o lote falha na segunda mudança e nenhuma é aplicada.
resultado_lote =
  Entregas.atualizar_lote("op-3", [
    {"BR001", :saiu_para_entrega},
    {"BR001", :postada}
  ])

IO.inspect(resultado_lote, label: "lote inválido")
IO.inspect(Entregas.rastrear("BR001"), label: "rastreamento")
