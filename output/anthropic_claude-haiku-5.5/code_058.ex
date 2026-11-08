# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de transações financeiras, atendendo somente aos seguintes requisitos:
#
# 1. Persistir os dados para recuperação após reinicializações.
# 2. Impedir que uma mesma operação seja executada mais de uma vez.
# 3. Permitir consultar o estado atual das operações em andamento.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - ETS
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# Arquivo único: gerenciador de transações financeiras idempotente e persistente.
#
# Requisitos atendidos:
#   1. Persistência: a tabela ETS é salva em disco (ets:tab2file) a cada mudança de
#      estado, via escrita em arquivo temporário seguida de renomeação atômica, e
#      recarregada (ets:file2tab) na inicialização.
#   2. Idempotência: cada operação é identificada por um `op_id`. Um `op_id` já
#      registrado nunca é executado novamente, independentemente do seu status.
#   3. Consulta: o estado das operações pode ser consultado a qualquer momento
#      diretamente na ETS (leitura concorrente, sem passar pelo GenServer).
#
# Ciclo de vida de uma operação:
#   :running     -> operação registrada e em execução
#   :completed   -> finalizada com sucesso
#   :failed      -> finalizada com erro (não é reexecutada)
#   :interrupted -> estava :running quando o sistema reiniciou; NÃO é reexecutada
#                   automaticamente para evitar dupla execução financeira; requer
#                   reconciliação externa.
#
# Uso:
#   {:ok, _} = FinTxManager.start_link(data_dir: "priv/tx_data")
#   FinTxManager.execute("pgto-123", %{valor: 100}, fn p -> {:ok, p.valor} end)
#   FinTxManager.execute("pgto-123", %{valor: 100}, fn _ -> {:ok, :x} end)
#   #=> {:duplicate, :completed, %{...}}
#   FinTxManager.status("pgto-123")
#   FinTxManager.in_progress()

defmodule FinTxManager do
  @moduledoc """
  Gerenciador de transações com idempotência, persistência em disco e consulta
  de estado, baseado em ETS.
  """

  use GenServer

  # Nome fixo da tabela ETS (uma instância do gerenciador por nó).
  @table :fin_tx_manager_ops
  @file_name "fin_tx_manager.ets"

  # ---------------------------------------------------------------------------
  # API pública
  # ---------------------------------------------------------------------------

  @doc """
  Inicia o gerenciador. Opções:
    * `:data_dir` - diretório onde o arquivo de persistência é salvo (padrão "data")
    * `:name`     - nome do processo (padrão `FinTxManager`)
  """
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))
  end

  @doc """
  Executa `fun` (aridade 1, recebe `payload`) para a operação `op_id`, no máximo uma vez.

  Retorna:
    * `{:ok, value}` / `{:error, reason}` - resultado da primeira execução
    * `{:duplicate, status, data}` - a operação já existe (qualquer status); não é executada
  """
  @spec execute(GenServer.server(), term(), term(), (term() -> {:ok, term()} | {:error, term()})) ::
          {:ok, term()} | {:error, term()} | {:duplicate, atom(), map()}
  def execute(server \\ __MODULE__, op_id, payload, fun) when is_function(fun, 1) do
    GenServer.call(server, {:begin, op_id, payload, fun}, :infinity)
  end

  @doc """
  Consulta o estado de uma operação. Leitura direta na ETS, sem bloquear o GenServer.
  """
  @spec status(term()) :: {:ok, atom(), map()} | {:error, :not_found}
  def status(op_id) do
    case :ets.lookup(@table, op_id) do
      [{^op_id, status, data}] -> {:ok, status, data}
      [] -> {:error, :not_found}
    end
  end

  @doc """
  Lista as operações atualmente em andamento (status `:running`).
  """
  @spec in_progress() :: [{term(), map()}]
  def in_progress do
    :ets.match_object(@table, {:_, :running, :_})
    |> Enum.map(fn {op_id, :running, data} -> {op_id, data} end)
  end

  # ---------------------------------------------------------------------------
  # Callbacks do GenServer
  # ---------------------------------------------------------------------------

  @impl true
  def init(opts) do
    dir = Keyword.get(opts, :data_dir, "data")
    File.mkdir_p!(dir)
    path = Path.join(dir, @file_name)

    load_or_create_table(path)
    recover_interrupted()
    :ok = persist(path)

    # `tasks` mapeia a referência da Task para {op_id, from} do chamador.
    {:ok, %{path: path, tasks: %{}}}
  end

  @impl true
  def handle_call({:begin, op_id, payload, fun}, from, state) do
    case :ets.lookup(@table, op_id) do
      [{^op_id, status, data}] ->
        # Idempotência: operação já registrada não é executada novamente.
        {:reply, {:duplicate, status, data}, state}

      [] ->
        now = now()
        data = %{payload: payload, result: nil, started_at: now, updated_at: now}

        # Write-ahead: registra como :running e persiste ANTES de executar.
        # Se a persistência falhar, o processo cai e nada foi executado.
        :ets.insert(@table, {op_id, :running, data})
        :ok = persist(state.path)

        task = Task.async(fn -> run_safely(fun, payload) end)
        tasks = Map.put(state.tasks, task.ref, {op_id, from})

        {:noreply, %{state | tasks: tasks}}
    end
  end

  @impl true
  def handle_info({ref, result}, %{tasks: tasks} = state) when is_reference(ref) and is_map_key(tasks, ref) do
    # Resultado normal da Task: remove o monitor para não receber :DOWN.
    Process.demonitor(ref, [:flush])
    {op_id, from} = Map.fetch!(tasks, ref)
    state = %{state | tasks: Map.delete(tasks, ref)}
    finish(op_id, from, result, state)
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, %{tasks: tasks} = state)
      when is_map_key(tasks, ref) do
    # Task caiu inesperadamente antes de reportar resultado.
    {op_id, from} = Map.fetch!(tasks, ref)
    state = %{state | tasks: Map.delete(tasks, ref)}
    finish(op_id, from, {:error, {:crashed, reason}}, state)
  end

  def handle_info(_msg, state), do: {:noreply, state}

  # ---------------------------------------------------------------------------
  # Funções privadas
  # ---------------------------------------------------------------------------

  # Conclui uma operação: atualiza o status na ETS, persiste e responde ao chamador.
  defp finish(op_id, from, result, state) do
    [{^op_id, :running, data}] = :ets.lookup(@table, op_id)

    {status, value} =
      case result do
        {:ok, v} -> {:completed, v}
        {:error, r} -> {:failed, r}
      end

    :ets.insert(@table, {op_id, status, %{data | result: value, updated_at: now()}})
    :ok = persist(state.path)

    GenServer.reply(from, result)
    {:noreply, state}
  end

  # Executa a função isolando exceções, para que a Task sempre reporte um resultado.
  defp run_safely(fun, payload) do
    try do
      case fun.(payload) do
        {:ok, _} = ok -> ok
        {:error, _} = err -> err
        other -> {:error, {:invalid_return, other}}
      end
    rescue
      e -> {:error, {:exception, Exception.message(e)}}
    catch
      kind, reason -> {:error, {kind, reason}}
    end
  end

  # Carrega a tabela do disco, se existir; caso contrário, cria uma nova.
  # A tabela é :protected: somente o GenServer escreve, qualquer processo lê.
  defp load_or_create_table(path) do
    if File.exists?(path) do
      {:ok, @table} = :ets.file2tab(String.to_charlist(path))
      @table
    else
      :ets.new(@table, [:named_table, :set, :protected, read_concurrency: true])
    end
  end

  # Operações que estavam :running antes de uma reinicialização tornam-se :interrupted.
  # Não são reexecutadas, evitando dupla execução de operações financeiras.
  defp recover_interrupted do
    for {op_id, :running, data} <- :ets.tab2list(@table) do
      :ets.insert(@table, {op_id, :interrupted, %{data | updated_at: now()}})
    end

    :ok
  end

  # Persistência atômica: grava em arquivo temporário e renomeia.
  defp persist(path) do
    tmp = path <> ".tmp"

    case :ets.tab2file(@table, String.to_charlist(tmp)) do
      :ok -> File.rename(tmp, path)
      {:error, reason} -> {:error, reason}
    end
  end

  defp now, do: System.system_time(:millisecond)
end
