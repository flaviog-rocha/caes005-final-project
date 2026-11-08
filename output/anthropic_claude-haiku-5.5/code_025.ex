# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de entregas e rastreamento, atendendo somente aos seguintes requisitos:
#
# 1. Fazer uso de macros.
# 2. Manter um histórico das operações realizadas.
# 3. Permitir consultar o estado atual das operações em andamento.
# 4. Permitir que diferentes entidades sejam processadas independentemente.
# 5. Permitir limitar a quantidade de operações simultâneas.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - Logger
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule Entregas do
  @moduledoc """
  Gerenciamento de entregas e rastreamento baseado em processos.

  - Cada operação executa em um processo isolado, então entidades diferentes
    são processadas de forma independente (a falha de uma não afeta as demais).
  - A concorrência é limitada pela opção `:limite`; operações excedentes ficam
    em uma fila de pendentes.
  - O histórico de operações finalizadas e o estado das operações em andamento
    podem ser consultados a qualquer momento.
  - As etapas das operações são instrumentadas pelas macros de `Entregas.Macros`.
  """

  use GenServer
  require Logger

  @limite_padrao 4

  # ---------------------------------------------------------------------------
  # API pública
  # ---------------------------------------------------------------------------

  @doc "Inicia o gerenciador. Opções: `:limite` (concorrência máxima) e `:name`."
  def start_link(opts \\ []) do
    nome = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: nome)
  end

  @doc "Submete a execução de `modulo.funcao(args...)` para uma entidade. Retorna `{:ok, id}`."
  def submeter(servidor, entidade, modulo, funcao, args \\ []) do
    GenServer.call(servidor, {:submeter, entidade, modulo, funcao, args})
  end

  @doc "Consulta o estado atual de uma operação pelo id."
  def status(servidor, id) do
    GenServer.call(servidor, {:status, id})
  end

  @doc "Lista as operações pendentes e em execução."
  def em_andamento(servidor) do
    GenServer.call(servidor, :em_andamento)
  end

  @doc "Retorna o histórico de operações finalizadas, da mais antiga para a mais recente."
  def historico(servidor) do
    GenServer.call(servidor, :historico)
  end

  # ---------------------------------------------------------------------------
  # Callbacks do GenServer
  # ---------------------------------------------------------------------------

  @impl true
  def init(opts) do
    limite = Keyword.get(opts, :limite, @limite_padrao)
    Logger.info("Gerenciador de entregas iniciado com limite de #{limite} operações simultâneas")

    {:ok,
     %{
       limite: limite,
       # id => operação (todas: pendentes, em execução e finalizadas)
       ops: %{},
       # fila FIFO de ids de operações pendentes
       fila: :queue.new(),
       # id => referência do monitor (somente operações em execução)
       ativas: %{},
       # operações finalizadas (ordem inversa)
       historico: [],
       contador: 0
     }}
  end

  @impl true
  def handle_call({:submeter, entidade, modulo, funcao, args}, _from, estado) do
    id = estado.contador + 1

    op = %{
      id: id,
      entidade: entidade,
      modulo: modulo,
      funcao: funcao,
      args: args,
      status: :pendente,
      etapas: [],
      submetida_em: agora(),
      iniciada_em: nil,
      finalizada_em: nil,
      resultado: nil,
      pid: nil
    }

    estado = %{
      estado
      | contador: id,
        ops: Map.put(estado.ops, id, op),
        fila: :queue.in(id, estado.fila)
    }

    Logger.info("Operação #{id} enfileirada para entidade #{inspect(entidade)}")
    {:reply, {:ok, id}, despachar(estado)}
  end

  def handle_call({:status, id}, _from, estado) do
    resposta =
      case Map.fetch(estado.ops, id) do
        {:ok, op} -> {:ok, resumo(op)}
        :error -> {:error, :nao_encontrada}
      end

    {:reply, resposta, estado}
  end

  def handle_call(:em_andamento, _from, estado) do
    lista =
      estado.ops
      |> Map.values()
      |> Enum.filter(&(&1.status in [:pendente, :executando]))
      |> Enum.sort_by(& &1.id)
      |> Enum.map(&resumo/1)

    {:reply, lista, estado}
  end

  def handle_call(:historico, _from, estado) do
    {:reply, estado.historico |> Enum.reverse() |> Enum.map(&resumo/1), estado}
  end

  # Evento de etapa enviado pelo processo worker (via macro `etapa`).
  @impl true
  def handle_info({:evento, id, etapa}, estado) do
    ops = Map.update!(estado.ops, id, fn op -> %{op | etapas: [etapa | op.etapas]} end)
    {:noreply, %{estado | ops: ops}}
  end

  # Operação concluída com sucesso.
  def handle_info({:concluida, id, {:ok, valor}}, estado) do
    {:noreply, finalizar(estado, id, :concluida, valor)}
  end

  # Operação concluída com erro capturado no worker.
  def handle_info({:concluida, id, {:erro, motivo}}, estado) do
    {:noreply, finalizar(estado, id, :falhou, motivo)}
  end

  # Worker terminou de forma inesperada (sem enviar resultado).
  def handle_info({:DOWN, ref, :process, _pid, motivo}, estado) do
    case Enum.find(estado.ativas, fn {_id, r} -> r == ref end) do
      {id, _ref} -> {:noreply, finalizar(estado, id, :falhou, {:saida_inesperada, motivo})}
      nil -> {:noreply, estado}
    end
  end

  # ---------------------------------------------------------------------------
  # Funções privadas
  # ---------------------------------------------------------------------------

  # Inicia operações pendentes enquanto houver vagas no limite de concorrência.
  defp despachar(estado) do
    if map_size(estado.ativas) < estado.limite and not :queue.is_empty(estado.fila) do
      {{:value, id}, fila} = :queue.out(estado.fila)
      op = Map.fetch!(estado.ops, id)
      gerenciador = self()

      {pid, ref} = spawn_monitor(fn -> executar_operacao(gerenciador, op) end)

      op = %{op | status: :executando, pid: pid, iniciada_em: agora()}
      Logger.info("Operação #{id} iniciada para entidade #{inspect(op.entidade)}")

      estado = %{
        estado
        | fila: fila,
          ops: Map.put(estado.ops, id, op),
          ativas: Map.put(estado.ativas, id, ref)
      }

      despachar(estado)
    else
      estado
    end
  end

  # Executa a operação no processo worker, com contexto de rastreamento.
  defp executar_operacao(gerenciador, op) do
    Process.put(:entregas_ctx, %{gerenciador: gerenciador, op_id: op.id})

    resultado =
      try do
        {:ok, apply(op.modulo, op.funcao, op.args)}
      catch
        tipo, motivo -> {:erro, {tipo, motivo}}
      end

    send(gerenciador, {:concluida, op.id, resultado})
  end

  # Marca a operação como finalizada, move para o histórico e libera a vaga.
  defp finalizar(estado, id, status, valor) do
    op = Map.fetch!(estado.ops, id)
    op = %{op | status: status, resultado: valor, finalizada_em: agora(), pid: nil}

    {ref, ativas} = Map.pop(estado.ativas, id)
    if ref, do: Process.demonitor(ref, [:flush])

    Logger.info("Operação #{id} (#{inspect(op.entidade)}) finalizada com status #{status}")

    estado = %{
      estado
      | ops: Map.put(estado.ops, id, op),
        ativas: ativas,
        historico: [op | estado.historico]
    }

    despachar(estado)
  end

  defp resumo(op) do
    %{
      id: op.id,
      entidade: op.entidade,
      operacao: {op.modulo, op.funcao},
      status: op.status,
      submetida_em: op.submetida_em,
      iniciada_em: op.iniciada_em,
      finalizada_em: op.finalizada_em,
      resultado: op.resultado,
      etapas: Enum.reverse(op.etapas)
    }
  end

  defp agora, do: DateTime.utc_now()
end

defmodule Entregas.Macros do
  @moduledoc """
  Macros para definição de operações rastreáveis.

  - `defoperacao/2`: define uma função cujo corpo é executado como operação rastreada.
  - `etapa/2`: envolve um bloco de código como etapa instrumentada (log e duração).
  """

  require Logger

  @doc """
  Define uma função pública que executa seu corpo como uma operação rastreada.

      defoperacao transportar(pacote, destino) do
        ...
      end
  """
  defmacro defoperacao(assinatura, do: corpo) do
    nome = assinatura |> elem(0) |> Atom.to_string()

    quote do
      def unquote(assinatura) do
        Entregas.Macros.__etapa__(unquote(nome), fn -> unquote(corpo) end)
      end
    end
  end

  @doc """
  Executa um bloco como etapa nomeada, registrando início, fim, duração e erros.

      etapa "transporte" do
        ...
      end
  """
  defmacro etapa(nome, do: bloco) do
    quote do
      Entregas.Macros.__etapa__(unquote(nome), fn -> unquote(bloco) end)
    end
  end

  @doc false
  def __etapa__(nome, fun) do
    ctx = Process.get(:entregas_ctx)
    inicio = System.monotonic_time(:millisecond)

    Logger.info("Etapa '#{nome}' iniciada#{contexto(ctx)}")

    try do
      resultado = fun.()
      registrar(ctx, nome, :ok, inicio)
      resultado
    catch
      tipo, motivo ->
        Logger.error("Etapa '#{nome}' falhou#{contexto(ctx)}: #{inspect({tipo, motivo})}")
        registrar(ctx, nome, :erro, inicio)
        :erlang.raise(tipo, motivo, __STACKTRACE__)
    end
  end

  defp registrar(ctx, nome, status, inicio) do
    duracao = System.monotonic_time(:millisecond) - inicio
    Logger.info("Etapa '#{nome}' finalizada (#{status}) em #{duracao} ms#{contexto(ctx)}")

    case ctx do
      %{gerenciador: gerenciador, op_id: id} ->
        evento = %{nome: nome, status: status, duracao_ms: duracao, em: DateTime.utc_now()}
        send(gerenciador, {:evento, id, evento})

      _ ->
        :ok
    end
  end

  defp contexto(%{op_id: id}), do: " (operação #{id})"
  defp contexto(_), do: ""
end

defmodule Entregas.Operacoes do
  @moduledoc "Operações de entrega definidas com as macros do módulo `Entregas.Macros`."

  import Entregas.Macros
  require Logger

  defoperacao transportar(pacote, destino) do
    etapa "coleta" do
      Process.sleep(100)
      {:coletado, pacote}
    end

    etapa "transporte" do
      Process.sleep(200)
      :ok
    end

    etapa "entrega" do
      if destino == "invalido", do: raise(ArgumentError, "destino inválido: #{destino}")
      Logger.info("Pacote #{pacote} entregue em #{destino}")
      {:entregue, pacote, destino}
    end
  end
end

defmodule Entregas.Demo do
  @moduledoc "Exemplo de uso do gerenciador de entregas."

  def executar do
    {:ok, _pid} = Entregas.start_link(limite: 2)

    entregas = [
      {"pedido-1", "Centro"},
      {"pedido-2", "Zona Norte"},
      {"pedido-3", "invalido"},
      {"pedido-4", "Zona Sul"}
    ]

    ids =
      for {pedido, destino} <- entregas do
        {:ok, id} =
          Entregas.submeter(Entregas, pedido, Entregas.Operacoes, :transportar, [pedido, destino])

        id
      end

    IO.inspect(Entregas.em_andamento(Entregas), label: "em andamento")
    Process.sleep(2_500)
    IO.inspect(Entregas.status(Entregas, hd(ids)), label: "status da primeira operação")
    IO.inspect(Entregas.historico(Entregas), label: "histórico")
    :ok
  end
end
