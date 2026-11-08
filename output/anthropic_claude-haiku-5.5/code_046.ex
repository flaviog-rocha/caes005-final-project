# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de agendamento de compromissos, atendendo somente aos seguintes requisitos:
#
# 1. Executar determinadas operações periodicamente.
# 2. Permitir a execução de operações simultâneas.
# 3. Impedir atualizações parcialmente concluídas.
# 4. Implementar novas tentativas após falhas temporárias.
# 5. Permitir que diferentes entidades sejam processadas independentemente.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - DETS
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# agendamento.exs
#
# Sistema de agendamento de compromissos com persistência em DETS.
# Executar com: elixir agendamento.exs

defmodule Agendamento do
  @moduledoc """
  Agendador de compromissos com:

  1. Execução periódica: um tick a cada `tick_ms` procura compromissos vencidos.
  2. Concorrência: cada entidade é processada em uma Task supervisionada,
     e entidades diferentes são executadas simultaneamente.
  3. Atomicidade: cada transição de estado é gravada como um único objeto DETS
     (`:dets.insert/2`) seguido de `:dets.sync/1`. Uma entidade só tem uma
     execução em andamento por vez, e o estado "executando" possui um lease
     que permite recuperar compromissos interrompidos por queda do sistema.
  4. Novas tentativas: falhas transitórias são reagendadas com backoff
     exponencial e jitter, até `@max_tentativas`.
  5. Independência: cada entidade tem sua própria fila e falhas são isoladas
     por Task, sem derrubar o agendador nem as outras entidades.
  """

  use GenServer

  @tick_ms 500
  @lease_ms 10_000
  @max_tentativas 4
  @backoff_base_ms 200

  # ---------------------------------------------------------------------------
  # API pública
  # ---------------------------------------------------------------------------

  @doc "Inicia o agendador. Opções: :arquivo (obrigatória), :handler (obrigatória), :tabela, :tick_ms, :name."
  def start_link(opts) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc "Agenda um compromisso para uma entidade em `executar_em_ms` (timestamp Unix em ms)."
  def agendar(server \\ __MODULE__, entidade, payload, executar_em_ms) do
    GenServer.call(server, {:agendar, entidade, payload, executar_em_ms})
  end

  @doc "Consulta um compromisso pelo id."
  def consultar(server \\ __MODULE__, id) do
    GenServer.call(server, {:consultar, id})
  end

  # ---------------------------------------------------------------------------
  # Callbacks do GenServer
  # ---------------------------------------------------------------------------

  @impl true
  def init(opts) do
    arquivo = opts |> Keyword.fetch!(:arquivo) |> to_charlist()
    handler = Keyword.fetch!(opts, :handler)
    tabela = Keyword.get(opts, :tabela, :agendamento)
    tick = Keyword.get(opts, :tick_ms, @tick_ms)

    # DETS é aberta e acessada somente pelo GenServer, o que serializa as escritas.
    {:ok, dets} = :dets.open_file(tabela, file: arquivo, type: :set)
    {:ok, sup} = Task.Supervisor.start_link()
    agendar_tick(tick)

    {:ok,
     %{
       dets: dets,
       sup: sup,
       handler: handler,
       tick: tick,
       # Conjunto de entidades com execução em andamento (exclusão mútua por entidade).
       ocupadas: MapSet.new(),
       # Mapa %{ref_da_task => {entidade, compromisso_em_execucao}}.
       tarefas: %{}
     }}
  end

  @impl true
  def terminate(_motivo, state) do
    :dets.close(state.dets)
    :ok
  end

  @impl true
  def handle_call({:agendar, entidade, payload, em_ms}, _from, state) do
    id = {System.os_time(:nanosecond), :erlang.unique_integer([:positive])}

    compromisso = %{
      id: id,
      entidade: entidade,
      payload: payload,
      status: :agendado,
      executar_em: em_ms,
      tentativas: 0,
      lease_ate: nil,
      ultimo_erro: nil,
      resultado: nil
    }

    :ok = gravar(state.dets, compromisso)
    {:reply, {:ok, id}, state}
  end

  def handle_call({:consultar, id}, _from, state) do
    resposta =
      case :dets.lookup(state.dets, id) do
        [{^id, compromisso}] -> {:ok, compromisso}
        [] -> {:error, :nao_encontrado}
      end

    {:reply, resposta, state}
  end

  # Tick periódico: busca compromissos vencidos e inicia uma execução por entidade.
  @impl true
  def handle_info(:tick, state) do
    agora = agora_ms()

    state =
      state.dets
      |> vencidos(agora)
      |> Enum.group_by(& &1.entidade)
      |> Enum.reduce(state, fn {entidade, compromissos}, acc ->
        if MapSet.member?(acc.ocupadas, entidade) do
          # Entidade já possui execução em andamento: preserva a ordem.
          acc
        else
          [mais_antigo | _] = Enum.sort_by(compromissos, & &1.executar_em)
          iniciar(acc, entidade, mais_antigo, agora)
        end
      end)

    agendar_tick(state.tick)
    {:noreply, state}
  end

  # Resultado normal de uma Task.
  def handle_info({ref, resultado}, state) when is_reference(ref) do
    {:noreply, concluir(state, ref, resultado)}
  end

  # Task que terminou abruptamente (falha não capturada).
  def handle_info({:DOWN, ref, :process, _pid, motivo}, state) do
    {:noreply, concluir(state, ref, {:erro, :transiente, {:queda, motivo}})}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  # ---------------------------------------------------------------------------
  # Funções privadas
  # ---------------------------------------------------------------------------

  defp agendar_tick(intervalo) do
    Process.send_after(self(), :tick, intervalo)
  end

  defp agora_ms, do: System.os_time(:millisecond)

  # Compromissos agendados já vencidos ou em execução com lease expirado
  # (recuperação após queda do processo).
  defp vencidos(dets, agora) do
    :dets.foldl(
      fn {_id, compromisso}, acc ->
        if vencido?(compromisso, agora), do: [compromisso | acc], else: acc
      end,
      [],
      dets
    )
  end

  defp vencido?(%{status: :agendado, executar_em: t}, agora), do: t <= agora
  defp vencido?(%{status: :executando, lease_ate: t}, agora), do: t < agora
  defp vencido?(_, _), do: false

  defp iniciar(state, entidade, compromisso, agora) do
    # Claim persistido antes da execução: um único objeto DETS.
    em_execucao = %{compromisso | status: :executando, lease_ate: agora + @lease_ms}
    :ok = gravar(state.dets, em_execucao)

    handler = state.handler

    tarefa =
      Task.Supervisor.async_nolink(state.sup, fn ->
        executar_seguro(handler, em_execucao)
      end)

    %{
      state
      | ocupadas: MapSet.put(state.ocupadas, entidade),
        tarefas: Map.put(state.tarefas, tarefa.ref, {entidade, em_execucao})
    }
  end

  defp concluir(state, ref, resultado) do
    case Map.pop(state.tarefas, ref) do
      {nil, _} ->
        state

      {{entidade, compromisso}, tarefas} ->
        Process.demonitor(ref, [:flush])
        atualizado = aplicar_resultado(compromisso, resultado, agora_ms())
        # Uma única escrita atômica por transição de estado.
        :ok = gravar(state.dets, atualizado)

        %{
          state
          | tarefas: tarefas,
            ocupadas: MapSet.delete(state.ocupadas, entidade)
        }
    end
  end

  defp aplicar_resultado(compromisso, {:ok, resultado}, _agora) do
    %{compromisso | status: :concluido, resultado: resultado, lease_ate: nil}
  end

  defp aplicar_resultado(compromisso, {:erro, :permanente, motivo}, _agora) do
    %{compromisso | status: :falhou, ultimo_erro: motivo, lease_ate: nil}
  end

  defp aplicar_resultado(compromisso, {:erro, :transiente, motivo}, agora) do
    tentativas = compromisso.tentativas + 1

    base = %{
      compromisso
      | tentativas: tentativas,
        ultimo_erro: motivo,
        lease_ate: nil
    }

    if tentativas < @max_tentativas do
      %{base | status: :agendado, executar_em: agora + backoff(tentativas)}
    else
      %{base | status: :falhou}
    end
  end

  # Retorno desconhecido do handler é tratado como falha permanente.
  defp aplicar_resultado(compromisso, outro, agora) do
    aplicar_resultado(compromisso, {:erro, :permanente, {:retorno_invalido, outro}}, agora)
  end

  # Backoff exponencial com jitter: base * 2^(n-1) + aleatório em [0, base).
  defp backoff(n) do
    @backoff_base_ms * Integer.pow(2, n - 1) + :rand.uniform(@backoff_base_ms)
  end

  # Executa o handler isolando exceções e saídas, convertendo-as em falha transitória.
  defp executar_seguro(handler, compromisso) do
    try do
      handler.(compromisso)
    rescue
      e -> {:erro, :transiente, {:excecao, Exception.message(e)}}
    catch
      tipo, valor -> {:erro, :transiente, {tipo, valor}}
    end
  end

  defp gravar(dets, compromisso) do
    :ok = :dets.insert(dets, {compromisso.id, compromisso})
    :ok = :dets.sync(dets)
  end

  # ---------------------------------------------------------------------------
  # Demonstração (executada apenas quando o arquivo é rodado como script)
  # ---------------------------------------------------------------------------

  @doc false
  def demo do
    arquivo = Path.join(System.tmp_dir!(), "agendamento_demo.dets")
    File.rm(arquivo)

    handler = fn %{entidade: entidade, payload: payload, tentativas: tentativas} ->
      cond do
        # Falha permanente para uma entidade específica (isolada das demais).
        entidade == "cliente:2" ->
          {:erro, :permanente, :dados_invalidos}

        # Falha transitória na primeira tentativa, sucesso depois.
        tentativas == 0 and :rand.uniform() < 0.6 ->
          {:erro, :transiente, :timeout}

        true ->
          Process.sleep(300)
          {:ok, "#{payload} processado"}
      end
    end

    {:ok, pid} =
      start_link(arquivo: arquivo, handler: handler, tabela: :demo, tick_ms: 100)

    agora = agora_ms()

    ids =
      for {entidade, i} <- Enum.with_index(["cliente:1", "cliente:2", "cliente:3"]),
          j <- 1..2 do
        {:ok, id} = agendar(entidade, "compromisso #{i}.#{j}", agora + j * 50)
        id
      end

    Process.sleep(4_000)

    Enum.each(ids, fn id ->
      {:ok, c} = consultar(id)
      IO.puts("#{c.entidade} | #{c.payload} | #{c.status} | tentativas=#{c.tentativas} | resultado=#{inspect(c.resultado)} | erro=#{inspect(c.ultimo_erro)}")
    end)

    GenServer.stop(pid)
    File.rm(arquivo)
  end
end

if Path.extname(__ENV__.file) == ".exs", do: Agendamento.demo()
