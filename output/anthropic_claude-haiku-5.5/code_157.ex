# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de entregas e rastreamento, atendendo somente aos seguintes requisitos:
#
# 1. Impedir que uma mesma operação seja executada mais de uma vez.
# 2. Impedir atualizações parcialmente concluídas.
# 3. Quando fizer sentido para o domínio, considerar a comunicação com um banco de dados fictício e, opcionalmente, o uso de Ecto.Migration, sem tornar isso obrigatório.
# 4. Implementar novas tentativas após falhas temporárias.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# Arquivo único: gerenciamento de entregas e rastreamento em Elixir.
#
# Garantias:
#   1. Idempotência: cada operação recebe uma chave. A primeira execução é
#      registrada junto com seu efeito. Repetições com a mesma chave e mesmo
#      payload devolvem o resultado já gravado, sem reexecutar. Reuso da chave
#      com payload diferente é recusado.
#   2. Atomicidade: o estado é imutável. A operação trabalha sobre um snapshot
#      e só é gravada num commit compare-and-swap (CAS) que verifica a versão.
#      Não há escrita parcial: ou tudo é aplicado (efeito + chave de
#      idempotência) ou nada é.
#   3. Banco fictício em memória (Agent). A forma do estado segue um esquema
#      versionado e pode ser trocada por Ecto.Repo/Ecto.Migration sem alterar
#      a lógica de domínio.
#   4. Retentativas: erros temporários (conflito de versão, serviço
#      indisponível) são reexecutados com backoff exponencial e jitter. Erros
#      permanentes (dados inválidos, transição proibida) retornam imediatamente.
#
# Exemplo de uso:
#   {:ok, banco} = Entregas.iniciar()
#   {:ok, entrega} = Entregas.registrar_entrega(banco, "req-1",
#                      %{destinatario: "Ana", endereco: "Rua A, 10"})
#   {:ok, _} = Entregas.registrar_evento(banco, "req-2", entrega.id, :em_transito)
#   {:ok, _} = Entregas.registrar_evento(banco, "req-2", entrega.id, :em_transito) # não reexecuta

defmodule Entregas.Banco do
  @moduledoc """
  Banco de dados fictício em memória, implementado com Agent.

  O estado é um mapa com:
    * `:versao`        - contador usado para controle de concorrência (CAS)
    * `:esquema`       - versão do esquema (análogo a schema_migrations do Ecto)
    * `:seq`           - gerador sequencial de IDs de entregas
    * `:entregas`      - %{id => entrega}
    * `:rastreamentos` - %{entrega_id => [evento]}
    * `:idempotencia`  - %{chave => {impressao_digital, resultado}}

  Para usar Ecto, substitua este módulo por um Repo cujas operações de commit
  sejam executadas dentro de `Repo.transaction/1`, e crie as tabelas com
  `Ecto.Migration` (entregas, rastreamentos e idempotencia).
  """

  use Agent

  @esquema_atual 1

  @spec start_link(keyword()) :: Agent.on_start()
  def start_link(opts \\ []) do
    Agent.start_link(fn -> estado_inicial() end, Keyword.take(opts, [:name]))
  end

  defp estado_inicial do
    %{
      versao: 0,
      esquema: @esquema_atual,
      seq: 0,
      entregas: %{},
      rastreamentos: %{},
      idempotencia: %{}
    }
  end

  @doc "Retorna uma cópia imutável do estado atual."
  @spec snapshot(pid() | atom()) :: map()
  def snapshot(banco), do: Agent.get(banco, & &1)

  @doc """
  Aplica `novo_estado` somente se a versão atual ainda for `versao_esperada`.
  A comparação e a troca ocorrem dentro do Agent, de forma atômica.
  """
  @spec commit(pid() | atom(), non_neg_integer(), map()) ::
          {:ok, map()} | {:error, :conflito}
  def commit(banco, versao_esperada, novo_estado) do
    Agent.get_and_update(banco, fn atual ->
      if atual.versao == versao_esperada do
        nova = %{novo_estado | versao: versao_esperada + 1}
        {{:ok, nova}, nova}
      else
        {{:error, :conflito}, atual}
      end
    end)
  end
end

defmodule Entregas.Transacao do
  @moduledoc """
  Executa uma função de negócio como transação all-or-nothing.

  A função recebe um snapshot e retorna:
    * `{:ok, resultado, novo_estado}`   - efeito a ser commitado
    * `{:replay, resultado}`            - operação já executada (sem escrita)
    * `{:error, motivo}`                - erro permanente (sem retentativa)
    * `{:erro_temporario, motivo}`      - erro transitório (com retentativa)

  Exceções são capturadas e tratadas como erro permanente. Como nenhuma escrita
  ocorre antes do commit, nada fica parcialmente aplicado.
  """

  alias Entregas.Banco

  @spec executar(pid() | atom(), (map() -> tuple())) ::
          {:ok, term()} | {:error, term()} | {:error, :retryable, term()}
  def executar(banco, fun) do
    snap = Banco.snapshot(banco)

    try do
      case fun.(snap) do
        {:ok, resultado, novo_estado} ->
          case Banco.commit(banco, snap.versao, novo_estado) do
            {:ok, _} -> {:ok, resultado}
            {:error, :conflito} -> {:error, :retryable, :conflito}
          end

        {:replay, resultado} ->
          {:ok, resultado}

        {:error, motivo} ->
          {:error, motivo}

        {:erro_temporario, motivo} ->
          {:error, :retryable, motivo}
      end
    rescue
      e -> {:error, {:excecao, Exception.message(e)}}
    end
  end
end

defmodule Entregas.Retry do
  @moduledoc """
  Retentativas com backoff exponencial e jitter para erros temporários.

  Só reexecuta resultados `{:error, :retryable, motivo}`. Opções:
    * `:max_tentativas`   - total de tentativas (padrão 5)
    * `:backoff_base_ms`  - base do backoff em ms (padrão 20)
    * `:dormir`           - função de espera (padrão Process.sleep/1; útil em testes)
  """

  @spec com_retentativas((-> term()), keyword()) :: term()
  def com_retentativas(fun, opts \\ []) do
    cfg = %{
      max: Keyword.get(opts, :max_tentativas, 5),
      base: Keyword.get(opts, :backoff_base_ms, 20),
      dormir: Keyword.get(opts, :dormir, &Process.sleep/1)
    }

    tentar(fun, 1, cfg)
  end

  defp tentar(fun, n, %{max: max} = cfg) do
    case fun.() do
      {:error, :retryable, _motivo} when n < max ->
        cfg.dormir.(atraso(n, cfg.base))
        tentar(fun, n + 1, cfg)

      {:error, :retryable, motivo} ->
        {:error, {:falha_temporaria_esgotada, motivo}}

      resultado ->
        resultado
    end
  end

  defp atraso(n, base) do
    base * Integer.pow(2, n - 1) + :rand.uniform(base)
  end
end

defmodule Entregas do
  @moduledoc """
  API de gerenciamento de entregas e rastreamento.

  Estados de uma entrega e transições permitidas:
    pendente -> em_transito | cancelada
    em_transito -> entregue | falha
    falha -> em_transito | cancelada
    entregue, cancelada -> (terminais)
  """

  alias Entregas.{Banco, Retry, Transacao}

  @transicoes %{
    pendente: [:em_transito, :cancelada],
    em_transito: [:entregue, :falha],
    falha: [:em_transito, :cancelada],
    entregue: [],
    cancelada: []
  }

  @doc "Inicia o banco fictício. Aceita `name:` nas opções."
  def iniciar(opts \\ []), do: Banco.start_link(opts)

  @doc """
  Registra uma nova entrega de forma idempotente.

  `dados` deve conter `:destinatario` e `:endereco` (strings não vazias).
  """
  def registrar_entrega(banco, chave, dados, opts \\ []) when is_binary(chave) do
    fingerprint = {:registrar, dados}

    executar_idempotente(banco, chave, fingerprint, opts, fn snap ->
      with :ok <- validar_dados(dados) do
        id = snap.seq + 1

        entrega = %{
          id: id,
          destinatario: dados.destinatario,
          endereco: dados.endereco,
          status: :pendente
        }

        estado = %{
          snap
          | seq: id,
            entregas: Map.put(snap.entregas, id, entrega),
            rastreamentos: Map.put(snap.rastreamentos, id, [])
        }

        {:ok, entrega, estado}
      end
    end)
  end

  @doc """
  Registra um evento de rastreamento e atualiza o status da entrega de forma
  atômica (evento e status são gravados juntos ou não são gravados).

  Opções:
    * `:notificador` - `fn chave, {id, status} -> :ok | {:error, :indisponivel} | {:error, motivo} end`
      Chamado antes do commit. Pode falhar temporariamente; nesse caso a
      operação é reexecutada. Recebe a chave para permitir deduplicação externa.
  """
  def registrar_evento(banco, chave, entrega_id, novo_status, opts \\ [])
      when is_binary(chave) do
    notificador = Keyword.get(opts, :notificador, fn _chave, _info -> :ok end)
    fingerprint = {:evento, entrega_id, novo_status}

    executar_idempotente(banco, chave, fingerprint, opts, fn snap ->
      with {:ok, entrega} <- buscar_entrega(snap, entrega_id),
           :ok <- validar_transicao(entrega.status, novo_status),
           :ok <- notificar(notificador, chave, {entrega_id, novo_status}) do
        evento = %{status: novo_status, ordem: length(historico_de(snap, entrega_id)) + 1}

        estado = %{
          snap
          | entregas:
              Map.put(snap.entregas, entrega_id, %{entrega | status: novo_status}),
            rastreamentos:
              Map.update!(snap.rastreamentos, entrega_id, &(&1 ++ [evento]))
        }

        {:ok, %{entrega | status: novo_status}, estado}
      end
    end)
  end

  @doc "Consulta uma entrega pelo ID (somente leitura)."
  def consultar(banco, entrega_id) do
    case Banco.snapshot(banco).entregas do
      %{^entrega_id => entrega} -> {:ok, entrega}
      _ -> {:error, :nao_encontrada}
    end
  end

  @doc "Retorna o histórico de rastreamento de uma entrega (somente leitura)."
  def historico(banco, entrega_id) do
    snap = Banco.snapshot(banco)

    case Map.fetch(snap.entregas, entrega_id) do
      {:ok, _} -> {:ok, historico_de(snap, entrega_id)}
      :error -> {:error, :nao_encontrada}
    end
  end

  # --------------------------------------------------------------------------
  # Núcleo de idempotência + atomicidade + retentativas
  # --------------------------------------------------------------------------

  # A chave de idempotência é gravada no mesmo commit que o efeito da operação.
  # Assim, se o commit tiver sucesso, a chave e o efeito existem; se falhar, nenhum dos dois existe.
  defp executar_idempotente(banco, chave, fingerprint, opts, operacao) do
    Retry.com_retentativas(
      fn ->
        Transacao.executar(banco, fn snap ->
          case Map.fetch(snap.idempotencia, chave) do
            {:ok, {^fingerprint, resultado}} ->
              # Mesma operação já executada: devolve o resultado gravado.
              {:replay, resultado}

            {:ok, _outra} ->
              # Mesma chave, payload diferente: recusado.
              {:error, :chave_reutilizada}

            :error ->
              case operacao.(snap) do
                {:ok, resultado, estado} ->
                  idem = Map.put(estado.idempotencia, chave, {fingerprint, resultado})
                  {:ok, resultado, %{estado | idempotencia: idem}}

                outro ->
                  outro
              end
          end
        end)
      end,
      opts
    )
  end

  # --------------------------------------------------------------------------
  # Regras de domínio
  # --------------------------------------------------------------------------

  defp validar_dados(%{destinatario: d, endereco: e})
       when is_binary(d) and is_binary(e) and d != "" and e != "",
       do: :ok

  defp validar_dados(_), do: {:error, :dados_invalidos}

  defp buscar_entrega(snap, id) do
    case Map.fetch(snap.entregas, id) do
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

  defp notificar(notificador, chave, info) do
    case notificador.(chave, info) do
      :ok -> :ok
      {:error, :indisponivel} -> {:erro_temporario, :notificador_indisponivel}
      {:error, motivo} -> {:error, motivo}
    end
  end

  defp historico_de(snap, entrega_id) do
    Map.get(snap.rastreamentos, entrega_id, [])
  end
end
