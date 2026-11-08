# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de usuários e sessões, atendendo somente aos seguintes requisitos:
#
# 1. Permitir a execução de operações simultâneas.
# 2. Manter um histórico das operações realizadas.
# 3. Permitir limitar a quantidade de operações simultâneas.
# 4. Detectar encerramentos inesperados de processos.
# 5. Impedir que uma mesma operação seja executada mais de uma vez.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - DynamicSupervisor
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# Arquivo: gerenciador.ex
# Gerenciamento de operações concorrentes de usuários e sessões.
#
# Requisitos atendidos:
#   1. Operações simultâneas: cada operação roda em seu próprio processo (Task linkada ao DynamicSupervisor).
#   2. Histórico: todas as submissões (iniciadas, rejeitadas, concluídas, falhas e encerramentos) são registradas.
#   3. Limite de simultaneidade: definido via `max_children` do DynamicSupervisor.
#   4. Encerramentos inesperados: detectados por monitores (`Process.monitor/1`), inclusive em caso de kill externo.
#   5. Idempotência: um mesmo `id` de operação não é executado mais de uma vez.
#
# Exemplo de uso:
#
#     {:ok, _} = Gerenciador.start_link(limite_simultaneo: 3)
#     Gerenciador.submeter({:sessao, 42, :login}, fn -> :timer.sleep(1000); :ok end)
#     Gerenciador.submeter({:sessao, 42, :login}, fn -> :ok end)  # {:error, :duplicada}
#     Gerenciador.status({:sessao, 42, :login})
#     Gerenciador.historico()

defmodule Gerenciador do
  @moduledoc """
  Supervisor de topo e API pública do gerenciador de operações.

  A árvore de supervisão contém:
    * um `DynamicSupervisor` que executa as operações e impõe o limite de simultaneidade;
    * um `Gerenciador.Controlador` (GenServer) que mantém o histórico, o controle de
      duplicidade e os monitores dos processos em execução.
  """

  use Supervisor

  @supervisor_operacoes Gerenciador.OperacoesSupervisor

  @doc """
  Inicia o gerenciador.

  Opções:
    * `:limite_simultaneo` - número máximo de operações executando ao mesmo tempo (padrão: 10).
  """
  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    limite = Keyword.get(opts, :limite_simultaneo, 10)

    children = [
      # `max_children` faz o DynamicSupervisor recusar novos filhos ao atingir o limite
      {DynamicSupervisor,
       name: @supervisor_operacoes, strategy: :one_for_one, max_children: limite},
      {Gerenciador.Controlador, supervisor: @supervisor_operacoes}
    ]

    # Se o controlador cair, o DynamicSupervisor é reiniciado junto, evitando operações órfãs
    Supervisor.init(children, strategy: :one_for_all)
  end

  @doc """
  Submete uma operação identificada por `id`. `fun` deve ser uma função de aridade 0.

  Retorna:
    * `{:ok, id}` - operação aceita e iniciada;
    * `{:error, :duplicada}` - operação com esse `id` já foi aceita anteriormente;
    * `{:error, :limite_atingido}` - limite de simultaneidade atingido.
  """
  def submeter(id, fun) when is_function(fun, 0) do
    GenServer.call(Gerenciador.Controlador, {:submeter, id, fun})
  end

  @doc "Retorna o histórico completo, da operação mais antiga para a mais recente."
  def historico do
    GenServer.call(Gerenciador.Controlador, :historico)
  end

  @doc "Retorna a lista de ids das operações em execução."
  def em_execucao do
    GenServer.call(Gerenciador.Controlador, :em_execucao)
  end

  @doc """
  Retorna o estado atual de uma operação:
  `:executando`, o último status registrado no histórico ou `:desconhecida`.
  """
  def status(id) do
    GenServer.call(Gerenciador.Controlador, {:status, id})
  end
end

defmodule Gerenciador.Controlador do
  @moduledoc false
  # Mantém o estado do gerenciador: operações em execução, ids já utilizados e histórico.
  # Todas as decisões (duplicidade, limite, registro) são serializadas aqui,
  # garantindo atomicidade mesmo com submissões concorrentes.

  use GenServer
  require Logger

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    estado = %{
      supervisor: Keyword.fetch!(opts, :supervisor),
      # id => {pid, ref_do_monitor}
      em_execucao: %{},
      # ids já aceitos (em execução ou finalizados) — impedem reexecução
      ids_usados: MapSet.new(),
      # entradas do histórico, mais recente primeiro
      historico: []
    }

    {:ok, estado}
  end

  @impl true
  def handle_call({:submeter, id, fun}, _from, estado) do
    if MapSet.member?(estado.ids_usados, id) do
      {:reply, {:error, :duplicada}, registrar(estado, id, :rejeitada, :duplicada)}
    else
      iniciar_operacao(estado, id, fun)
    end
  end

  def handle_call(:historico, _from, estado) do
    {:reply, Enum.reverse(estado.historico), estado}
  end

  def handle_call(:em_execucao, _from, estado) do
    {:reply, Map.keys(estado.em_execucao), estado}
  end

  def handle_call({:status, id}, _from, estado) do
    resposta =
      if Map.has_key?(estado.em_execucao, id) do
        :executando
      else
        case Enum.find(estado.historico, &(&1.id == id)) do
          nil -> :desconhecida
          %{status: status} -> status
        end
      end

    {:reply, resposta, estado}
  end

  @impl true
  # Resultado reportado pela própria operação (terminou normalmente, com valor ou erro capturado)
  def handle_info({:concluida, id, resultado}, estado) do
    case Map.fetch(estado.em_execucao, id) do
      {:ok, _} ->
        {status, detalhe} =
          case resultado do
            {:ok, valor} -> {:concluida, valor}
            {:erro, motivo} -> {:falhou, motivo}
          end

        {:noreply, finalizar(estado, id, status, detalhe)}

      :error ->
        {:noreply, estado}
    end
  end

  # Encerramento do processo da operação sem que o resultado tenha sido reportado:
  # crash não capturado, kill externo, exit inesperado etc.
  def handle_info({:DOWN, ref, :process, _pid, motivo}, estado) do
    case Enum.find(estado.em_execucao, fn {_id, {_pid, r}} -> r == ref end) do
      {id, _} ->
        Logger.warning(
          "Operação #{inspect(id)} encerrou inesperadamente: #{inspect(motivo)}"
        )

        {:noreply, finalizar(estado, id, :encerrada_inesperadamente, motivo)}

      nil ->
        {:noreply, estado}
    end
  end

  def handle_info(_msg, estado), do: {:noreply, estado}

  # Helpers privados

  defp iniciar_operacao(estado, id, fun) do
    spec = {Gerenciador.Operacao, {id, fun, self()}}

    case DynamicSupervisor.start_child(estado.supervisor, spec) do
      {:ok, pid} ->
        ref = Process.monitor(pid)

        estado = %{
          estado
          | em_execucao: Map.put(estado.em_execucao, id, {pid, ref}),
            ids_usados: MapSet.put(estado.ids_usados, id)
        }

        {:reply, {:ok, id}, registrar(estado, id, :iniciada, nil)}

      {:error, :max_children} ->
        {:reply, {:error, :limite_atingido},
         registrar(estado, id, :rejeitada, :limite_atingido)}

      {:error, motivo} ->
        {:reply, {:error, motivo}, registrar(estado, id, :rejeitada, motivo)}
    end
  end

  defp finalizar(estado, id, status, detalhe) do
    {{_pid, ref}, em_execucao} = Map.pop(estado.em_execucao, id)
    # Remove o monitor e descarta eventual mensagem :DOWN já na caixa de entrada
    Process.demonitor(ref, [:flush])

    %{estado | em_execucao: em_execucao}
    |> registrar(id, status, detalhe)
  end

  defp registrar(estado, id, status, detalhe) do
    entrada = %{
      id: id,
      status: status,
      detalhe: detalhe,
      em: DateTime.utc_now()
    }

    %{estado | historico: [entrada | estado.historico]}
  end
end

defmodule Gerenciador.Operacao do
  @moduledoc false
  # Processo que executa uma operação. Não é reiniciado automaticamente:
  # uma falha é registrada como encerramento inesperado e não é repetida,
  # preservando a garantia de execução única.

  def child_spec({_id, _fun, _controlador} = arg) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [arg]},
      restart: :temporary
    }
  end

  def start_link({id, fun, controlador}) do
    Task.start_link(fn ->
      resultado = executar(fun)
      send(controlador, {:concluida, id, resultado})
    end)
  end

  defp executar(fun) do
    {:ok, fun.()}
  rescue
    e -> {:erro, Exception.message(e)}
  catch
    tipo, motivo -> {:erro, {tipo, motivo}}
  end
end
