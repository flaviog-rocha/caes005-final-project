# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de um sistema de cache, atendendo somente aos seguintes requisitos:
#
# 1. Impedir atualizações parcialmente concluídas.
# 2. Permitir limitar a quantidade de operações simultâneas.
# 3. Garantir a consistência dos dados durante operações concorrentes.
# 4. Detectar encerramentos inesperados de processos.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule CacheTransacional do
  @moduledoc """
  Cache em memória com transações atômicas, controle de concorrência e
  detecção de encerramento inesperado de processos.

  Garantias:

    * Atomicidade: as escritas de uma transação são aplicadas de uma só vez
      no commit, ou não são aplicadas. Não há estados parcialmente atualizados.
    * Limite de concorrência: no máximo `:max_concurrency` transações ativas
      ao mesmo tempo. As demais aguardam em fila, com timeout.
    * Consistência: leituras são validadas por versão no commit (controle
      otimista). Se algum dado lido foi alterado por outra transação, o commit
      é rejeitado com `{:error, :conflict}`.
    * Detecção de falhas: o servidor monitora o processo dono de cada
      transação. Se ele terminar antes do commit, a transação é abortada e a
      vaga liberada automaticamente.

  Uso:

      {:ok, _} = CacheTransacional.start_link(name: :cache, max_concurrency: 5)

      CacheTransacional.transaction(:cache, fn tx ->
        {:ok, saldo, tx} = CacheTransacional.get(tx, :saldo, 0)
        tx = CacheTransacional.put(tx, :saldo, saldo + 10)
        {:ok, :atualizado, tx}
      end)
  """

  use GenServer
  require Logger

  defmodule Tx do
    @moduledoc false
    # Estado local da transação. Escritas ficam em memória até o commit.
    defstruct [:id, :server, reads: %{}, writes: %{}]
  end

  # ---------------------------------------------------------------------------
  # API pública
  # ---------------------------------------------------------------------------

  @doc "Inicia o cache. Aceita `:name` e `:max_concurrency` (padrão 10)."
  def start_link(opts \\ []) do
    {gen_opts, init_opts} = Keyword.split(opts, [:name])
    GenServer.start_link(__MODULE__, init_opts, gen_opts)
  end

  @doc """
  Abre uma transação, ocupando uma vaga de concorrência.
  Bloqueia até obter vaga ou até o `:timeout` (padrão 5000 ms).
  """
  def begin_tx(server, opts \\ []) do
    timeout = Keyword.get(opts, :timeout, 5_000)
    # O id é gerado pelo cliente para permitir cancelamento em caso de timeout.
    id = make_ref()

    try do
      :ok = GenServer.call(server, {:begin, id, self()}, timeout)
      {:ok, %Tx{id: id, server: server}}
    catch
      :exit, {:timeout, _} ->
        # Remove a solicitação da fila ou libera a vaga caso já tenha sido concedida.
        GenServer.cast(server, {:cancel, id})
        {:error, :timeout}
    end
  end

  @doc """
  Lê uma chave dentro da transação. Retorna `{:ok, valor, tx}`.
  Escritas pendentes da própria transação têm precedência (leitura das próprias escritas).
  """
  def get(%Tx{} = tx, key, default \\ nil) do
    case Map.fetch(tx.writes, key) do
      {:ok, {:put, value}} ->
        {:ok, value, tx}

      {:ok, :delete} ->
        {:ok, default, tx}

      :error ->
        case GenServer.call(tx.server, {:read, tx.id, self(), key}) do
          {:ok, {:found, value}, version} ->
            {:ok, value, %{tx | reads: Map.put_new(tx.reads, key, version)}}

          {:ok, :missing, version} ->
            {:ok, default, %{tx | reads: Map.put_new(tx.reads, key, version)}}

          {:error, reason} ->
            {:error, reason}
        end
    end
  end

  @doc "Stages a write. Only applied on commit."
  def put(%Tx{} = tx, key, value) do
    %{tx | writes: Map.put(tx.writes, key, {:put, value})}
  end

  @doc "Stages a deletion. Only applied on commit."
  def delete(%Tx{} = tx, key) do
    %{tx | writes: Map.put(tx.writes, key, :delete)}
  end

  @doc """
  Confirma a transação. Valida as leituras e aplica todas as escritas
  atomicamente. Retorna `:ok`, `{:error, :conflict}` ou `{:error, :tx_invalid}`.
  A vaga é sempre liberada ao final.
  """
  def commit(%Tx{} = tx) do
    GenServer.call(tx.server, {:commit, tx.id, self(), tx.reads, tx.writes})
  end

  @doc "Descarta a transação e libera a vaga. Não altera o cache."
  def rollback(%Tx{} = tx) do
    GenServer.cast(tx.server, {:release, tx.id, self()})
    :ok
  end

  @doc """
  Executa `fun` dentro de uma transação. `fun` recebe a transação e deve
  retornar `{:ok, resultado, tx}`, `{:error, motivo}` ou `{:abort, motivo}`.
  Em caso de exceção, a transação é descartada e a exceção é propagada.
  """
  def transaction(server, fun, opts \\ []) when is_function(fun, 1) do
    with {:ok, tx} <- begin_tx(server, opts) do
      run(tx, fun)
    end
  end

  defp run(tx, fun) do
    try do
      case fun.(tx) do
        {:ok, result, %Tx{} = final} ->
          if final.id == tx.id do
            case commit(final) do
              :ok -> {:ok, result}
              error -> error
            end
          else
            rollback(tx)
            {:error, :tx_id_mismatch}
          end

        {:error, reason} ->
          rollback(tx)
          {:error, reason}

        {:abort, reason} ->
          rollback(tx)
          {:error, reason}

        other ->
          rollback(tx)
          {:error, {:invalid_return, other}}
      end
    catch
      kind, reason ->
        rollback(tx)
        :erlang.raise(kind, reason, __STACKTRACE__)
    end
  end

  # ---------------------------------------------------------------------------
  # Callbacks do GenServer
  # ---------------------------------------------------------------------------

  @impl true
  def init(opts) do
    state = %{
      # key => {valor, versão}
      data: %{},
      # Relógio lógico global; cada commit com escritas recebe uma nova versão.
      clock: 0,
      max: Keyword.get(opts, :max_concurrency, 10),
      # id => %{pid: pid, mon: monitor_ref}
      active: %{},
      # Fila de {id, from, pid, monitor_ref} aguardando vaga
      waiting: :queue.new()
    }

    {:ok, state}
  end

  @impl true
  def handle_call({:begin, id, pid}, from, state) do
    # Monitora o dono desde a solicitação, para tratar encerramento inesperado
    # tanto na fila quanto com a transação ativa.
    mon = Process.monitor(pid)

    if map_size(state.active) < state.max do
      {:reply, :ok, activate(state, id, pid, mon)}
    else
      waiting = :queue.in({id, from, pid, mon}, state.waiting)
      {:noreply, %{state | waiting: waiting}}
    end
  end

  def handle_call({:read, id, pid, key}, _from, state) do
    if owner?(state, id, pid) do
      reply =
        case Map.fetch(state.data, key) do
          {:ok, {value, version}} -> {:ok, {:found, value}, version}
          :error -> {:ok, :missing, 0}
        end

      {:reply, reply, state}
    else
      {:reply, {:error, :tx_invalid}, state}
    end
  end

  def handle_call({:commit, id, pid, reads, writes}, _from, state) do
    cond do
      not owner?(state, id, pid) ->
        {:reply, {:error, :tx_invalid}, state}

      not reads_valid?(state.data, reads) ->
        {:reply, {:error, :conflict}, release(state, id)}

      true ->
        version = state.clock + 1
        # O novo estado é calculado por completo antes de substituir o atual:
        # ou todas as escritas são aplicadas, ou nenhuma é.
        new_data = apply_writes(state.data, writes, version)

        state =
          %{state | data: new_data, clock: version}
          |> release(id)

        {:reply, :ok, state}
    end
  end

  @impl true
  def handle_cast({:release, id, pid}, state) do
    if owner?(state, id, pid) do
      {:noreply, release(state, id)}
    else
      {:noreply, state}
    end
  end

  def handle_cast({:cancel, id}, state) do
    {:noreply, release(state, id)}
  end

  @impl true
  def handle_info({:DOWN, mon, :process, pid, reason}, state) do
    case Enum.find(state.active, fn {_id, info} -> info.mon == mon end) do
      {id, _info} ->
        if reason != :normal do
          Logger.warning(
            "Transação #{inspect(id)} abortada: processo #{inspect(pid)} " <>
              "terminou inesperadamente (#{inspect(reason)})"
          )
        end

        {:noreply, release(state, id)}

      nil ->
        # O processo pode estar apenas aguardando na fila.
        {:noreply, drop_waiting(state, fn {_id, _from, _pid, m} -> m == mon end)}
    end
  end

  def handle_info(_msg, state), do: {:noreply, state}

  # ---------------------------------------------------------------------------
  # Funções auxiliares (puras ou de manipulação de estado)
  # ---------------------------------------------------------------------------

  defp owner?(state, id, pid) do
    match?(%{pid: ^pid}, Map.get(state.active, id))
  end

  defp activate(state, id, pid, mon) do
    %{state | active: Map.put(state.active, id, %{pid: pid, mon: mon})}
  end

  # Libera a vaga de uma transação ativa ou remove uma solicitação da fila.
  defp release(state, id) do
    case Map.pop(state.active, id) do
      {%{mon: mon}, active} ->
        Process.demonitor(mon, [:flush])
        promote(%{state | active: active})

      {nil, _} ->
        drop_waiting(state, fn {wid, _from, _pid, _mon} -> wid == id end)
    end
  end

  # Concede vagas livres às solicitações da fila, em ordem de chegada.
  defp promote(state) do
    if map_size(state.active) < state.max do
      case :queue.out(state.waiting) do
        {{:value, {id, from, pid, mon}}, rest} ->
          GenServer.reply(from, :ok)
          promote(activate(%{state | waiting: rest}, id, pid, mon))

        {:empty, _} ->
          state
      end
    else
      state
    end
  end

  defp drop_waiting(state, predicate) do
    {removed, kept} =
      state.waiting
      |> :queue.to_list()
      |> Enum.split_with(predicate)

    Enum.each(removed, fn {_id, _from, _pid, mon} -> Process.demonitor(mon, [:flush]) end)

    %{state | waiting: :queue.from_list(kept)}
  end

  # Verifica se nenhum dado lido foi alterado desde a leitura (controle otimista).
  defp reads_valid?(data, reads) do
    Enum.all?(reads, fn {key, version} -> current_version(data, key) == version end)
  end

  defp current_version(data, key) do
    case Map.fetch(data, key) do
      {:ok, {_value, version}} -> version
      :error -> 0
    end
  end

  # Função pura: retorna um novo mapa de dados com todas as escritas aplicadas.
  defp apply_writes(data, writes, version) do
    Enum.reduce(writes, data, fn
      {key, {:put, value}}, acc -> Map.put(acc, key, {value, version})
      {key, :delete}, acc -> Map.delete(acc, key)
    end)
  end
end
