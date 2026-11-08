# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de um sistema de cache, atendendo somente aos seguintes requisitos:
#
# 1. Impedir atualizações parcialmente concluídas.
# 2. Permitir limitar a quantidade de operações simultâneas.
# 3. Garantir a consistência dos dados durante operações concorrentes.
# 4. Detectar encerramentos inesperados de processos.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule Cache do
  @moduledoc """
  Cache em memória com:

  * Transações atômicas: alterações são aplicadas todas de uma vez ou nenhuma (sem atualizações parciais).
  * Controle de concorrência otimista: cada commit valida a versão lida, evitando perda de atualizações.
  * Limite de operações simultâneas via semáforo com fila de espera e timeout.
  * Detecção de encerramentos inesperados por meio de monitores: slots de processos que morrem são liberados.

  Uso básico:

      {:ok, _} = Cache.start_link(max_concurrency: 4)
      :ok = Cache.put(:a, 1)
      {:ok, 2} = Cache.transaction(fn data ->
        {:ok, [{:put, :a, data[:a] + 1}], data[:a] + 1}
      end)
  """

  use GenServer
  require Logger

  @default_max_concurrency 10
  @default_timeout 5_000
  @default_retries 5

  # ---------------------------------------------------------------------------
  # API pública
  # ---------------------------------------------------------------------------

  @doc "Inicia o cache. Opções: :name (padrão __MODULE__) e :max_concurrency."
  def start_link(opts \\ []) do
    {name, opts} = Keyword.pop(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc false
  def child_spec(opts) do
    %{
      id: Keyword.get(opts, :name, __MODULE__),
      start: {__MODULE__, :start_link, [opts]},
      type: :worker
    }
  end

  @doc "Lê um valor. Leitura não passa pelo semáforo, pois é uma operação atômica no servidor."
  def get(key, server \\ __MODULE__) do
    GenServer.call(server, {:get, key})
  end

  @doc "Insere ou atualiza uma chave como transação atômica de uma única operação."
  def put(key, value, server \\ __MODULE__) do
    transaction(fn _data -> {:ok, [{:put, key, value}], :ok} end, server: server)
  end

  @doc "Remove uma chave como transação atômica de uma única operação."
  def delete(key, server \\ __MODULE__) do
    transaction(fn _data -> {:ok, [{:delete, key}], :ok} end, server: server)
  end

  @doc """
  Executa uma transação.

  A função recebe um snapshot imutável dos dados e deve retornar:

    * `{:ok, changes, result}` onde `changes` é uma lista de `{:put, chave, valor}` ou `{:delete, chave}`
    * `{:error, motivo}` para abortar sem alterar nada

  Opções:

    * `:server` - processo do cache (padrão `Cache`)
    * `:timeout` - tempo máximo de espera por um slot de concorrência
    * `:retries` - número de novas tentativas em caso de conflito de versão

  Observação: a função é executada no processo chamador enquanto ele ocupa um slot.
  Chamar `transaction/2` de dentro da própria função pode causar bloqueio se o limite
  de concorrência estiver esgotado.
  """
  def transaction(fun, opts \\ []) when is_function(fun, 1) do
    server = Keyword.get(opts, :server, __MODULE__)
    timeout = Keyword.get(opts, :timeout, @default_timeout)
    retries = Keyword.get(opts, :retries, @default_retries)

    case acquire(server, timeout) do
      {:ok, ref} ->
        # `after` garante a liberação do slot mesmo em caso de exceção.
        try do
          run(fun, server, ref, retries)
        after
          release(server, ref)
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  # ---------------------------------------------------------------------------
  # Lógica do cliente
  # ---------------------------------------------------------------------------

  # Lê o snapshot, executa a função e tenta o commit validando a versão.
  # Como a função opera sobre dados imutáveis e o commit é uma única
  # chamada atômica, uma falha no meio não deixa alterações parciais.
  defp run(fun, server, ref, attempts_left) do
    {version, data} = GenServer.call(server, :snapshot)

    case safe_call(fun, data) do
      {:ok, changes, result} ->
        case GenServer.call(server, {:commit, ref, version, changes}) do
          {:ok, _new_version} ->
            {:ok, result}

          {:error, :conflict} when attempts_left > 0 ->
            # Backoff com jitter para reduzir nova colisão.
            Process.sleep(:rand.uniform(20))
            run(fun, server, ref, attempts_left - 1)

          {:error, reason} ->
            {:error, reason}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  # Executa a função do usuário isolando exceções e retornos inválidos.
  defp safe_call(fun, data) do
    case fun.(data) do
      {:ok, changes, result} when is_list(changes) -> {:ok, changes, result}
      {:error, reason} -> {:error, reason}
      other -> {:error, {:invalid_return, other}}
    end
  rescue
    e -> {:error, {:exception, Exception.message(e)}}
  catch
    kind, reason -> {:error, {kind, reason}}
  end

  # Solicita um slot. A espera é feita no cliente com timeout,
  # e o cancelamento é síncrono para não deixar slots órfãos.
  defp acquire(server, timeout) do
    ref = make_ref()
    GenServer.cast(server, {:acquire, ref, self()})

    receive do
      {^ref, :granted} ->
        {:ok, ref}
    after
      timeout ->
        # O call garante que, ao retornar, qualquer grant já enviado
        # esteja na caixa de correio e possa ser descartado.
        GenServer.call(server, {:cancel, ref})

        receive do
          {^ref, :granted} -> :ok
        after
          0 -> :ok
        end

        {:error, :timeout}
    end
  end

  defp release(server, ref) do
    GenServer.cast(server, {:release, ref})
  end

  # ---------------------------------------------------------------------------
  # Servidor
  # ---------------------------------------------------------------------------

  @impl true
  def init(opts) do
    state = %{
      data: %{},
      version: 0,
      max: Keyword.get(opts, :max_concurrency, @default_max_concurrency),
      # %{ref => {pid, monitor_ref}}: slots concedidos
      holders: %{},
      # fila de {ref, pid, monitor_ref}: pedidos aguardando slot
      waiting: :queue.new()
    }

    {:ok, state}
  end

  @impl true
  def handle_call({:get, key}, _from, state) do
    {:reply, Map.get(state.data, key), state}
  end

  def handle_call(:snapshot, _from, state) do
    {:reply, {state.version, state.data}, state}
  end

  def handle_call({:commit, ref, base_version, changes}, _from, state) do
    cond do
      not Map.has_key?(state.holders, ref) ->
        {:reply, {:error, :not_holder}, state}

      base_version != state.version ->
        # Outra transação foi confirmada após a leitura: conflito.
        {:reply, {:error, :conflict}, state}

      true ->
        case apply_changes(changes, state.data) do
          {:ok, new_data} ->
            new_state = %{state | data: new_data, version: state.version + 1}
            {:reply, {:ok, new_state.version}, new_state}

          {:error, reason} ->
            # Nenhuma alteração é aplicada quando há erro.
            {:reply, {:error, reason}, state}
        end
    end
  end

  def handle_call({:cancel, ref}, _from, state) do
    {:reply, :ok, drop(ref, state)}
  end

  @impl true
  def handle_cast({:acquire, ref, pid}, state) do
    # Monitor por pedido: detecta encerramento tanto de quem aguarda quanto de quem usa o slot.
    mref = Process.monitor(pid)

    state =
      if map_size(state.holders) < state.max do
        grant(state, ref, pid, mref)
      else
        %{state | waiting: :queue.in({ref, pid, mref}, state.waiting)}
      end

    {:noreply, state}
  end

  def handle_cast({:release, ref}, state) do
    {:noreply, drop(ref, state)}
  end

  @impl true
  def handle_info({:DOWN, mref, :process, pid, reason}, state) do
    case Enum.find(state.holders, fn {_ref, {_pid, m}} -> m == mref end) do
      {ref, _} ->
        # Liberações normais removem o monitor antes do DOWN,
        # então qualquer DOWN aqui é um encerramento inesperado.
        Logger.warning(
          "Cache: processo #{inspect(pid)} encerrou inesperadamente " <>
            "(#{inspect(reason)}) segurando o slot #{inspect(ref)}; slot liberado"
        )

        state =
          %{state | holders: Map.delete(state.holders, ref)}
          |> promote()

        {:noreply, state}

      nil ->
        # Processo que estava apenas na fila de espera.
        waiting = :queue.filter(fn {_r, _p, m} -> m != mref end, state.waiting)
        {:noreply, %{state | waiting: waiting}}
    end
  end

  def handle_info(_msg, state), do: {:noreply, state}

  # ---------------------------------------------------------------------------
  # Funções auxiliares do servidor
  # ---------------------------------------------------------------------------

  # Valida todas as operações antes de aplicar. Como os mapas são imutáveis,
  # qualquer erro descarta o resultado parcial e preserva o estado original.
  defp apply_changes(changes, data) do
    Enum.reduce_while(changes, {:ok, data}, fn
      {:put, key, value}, {:ok, acc} ->
        {:cont, {:ok, Map.put(acc, key, value)}}

      {:delete, key}, {:ok, acc} ->
        {:cont, {:ok, Map.delete(acc, key)}}

      op, _acc ->
        {:halt, {:error, {:invalid_op, op}}}
    end)
  end

  defp grant(state, ref, pid, mref) do
    send(pid, {ref, :granted})
    %{state | holders: Map.put(state.holders, ref, {pid, mref})}
  end

  # Libera um slot ou remove um pedido da fila, sempre removendo o monitor.
  defp drop(ref, state) do
    case Map.pop(state.holders, ref) do
      {{_pid, mref}, holders} ->
        Process.demonitor(mref, [:flush])
        promote(%{state | holders: holders})

      {nil, _} ->
        {removed, rest} =
          Enum.split_with(:queue.to_list(state.waiting), fn {r, _p, _m} -> r == ref end)

        Enum.each(removed, fn {_r, _p, m} -> Process.demonitor(m, [:flush]) end)
        %{state | waiting: :queue.from_list(rest)}
    end
  end

  # Concede slots disponíveis aos próximos pedidos da fila (FIFO).
  defp promote(state) do
    with true <- map_size(state.holders) < state.max,
         {{:value, {ref, pid, mref}}, rest} <- :queue.out(state.waiting) do
      state
      |> Map.put(:waiting, rest)
      |> grant(ref, pid, mref)
      |> promote()
    else
      _ -> state
    end
  end
end
