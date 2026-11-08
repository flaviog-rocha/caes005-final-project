# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de estoque de uma loja, atendendo somente aos seguintes requisitos:
#
# 1. Processar grandes volumes de dados sem carregá-los integralmente na memória.
# 2. Permitir consultar o estado atual das operações em andamento.
# 3. Manter um histórico das operações realizadas.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# Gerenciamento de estoque com processamento em streaming, consulta de operações
# em andamento e histórico de operações. Usa apenas a biblioteca padrão do Elixir.
#
# Uso (no iex):
#   {:ok, _} = Estoque.start_link()
#   {:ok, _} = Estoque.adjust("SKU-1", 10)
#   {:ok, id} = Estoque.start_import("movimentos.csv", chunk_size: 500)
#   Estoque.operation_status(id)
#   Estoque.running_operations()
#   Estoque.history(10)
#   Estoque.stock("SKU-1")
#
# Formato do arquivo de movimentos: uma linha por movimento, "sku;delta"
# (delta positivo = entrada, negativo = saída). Ex.: "ABC-123;-4"

defmodule Estoque do
  @moduledoc """
  Gerenciamento de estoque com:

    * Processamento em streaming: o arquivo é lido linha a linha e agrupado
      em lotes (`Stream.chunk_every/2`), de modo que apenas um lote por vez
      fique em memória. O estado do estoque guarda somente um saldo por SKU.
    * Consulta de operações em andamento: cada importação registra progresso
      (processadas, aplicadas, inválidas, rejeitadas) que pode ser consultado
      a qualquer momento.
    * Histórico: operações finalizadas (importações e ajustes) são mantidas em
      uma lista limitada, da mais recente para a mais antiga.

  O estado é mantido por um único `GenServer`, o que serializa as
  atualizações e garante consistência entre saldo, progresso e histórico.
  """

  use GenServer

  @default_chunk_size 1_000
  @default_history_limit 200

  # ---------------------------------------------------------------------------
  # API pública
  # ---------------------------------------------------------------------------

  @doc "Inicia o processo de estoque registrado com o nome do módulo."
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Inicia a importação assíncrona de um arquivo de movimentos.
  Retorna `{:ok, id}` imediatamente; o processamento ocorre em background.
  """
  def start_import(path, opts \\ []) do
    chunk_size = Keyword.get(opts, :chunk_size, @default_chunk_size)

    {:ok, id} = GenServer.call(__MODULE__, {:begin, :import, %{source: path}})

    # A tarefa é desvinculada do chamador; falhas são tratadas dentro dela
    # e reportadas ao GenServer.
    {:ok, _pid} = Task.start(fn -> run_import(id, path, chunk_size) end)

    {:ok, id}
  end

  @doc """
  Aplica um ajuste direto de estoque para um SKU.
  Retorna `{:ok, novo_saldo}` ou `{:error, :estoque_insuficiente}`.
  """
  def adjust(sku, delta) when is_binary(sku) and is_integer(delta) do
    GenServer.call(__MODULE__, {:adjust, sku, delta})
  end

  @doc "Retorna o saldo atual de um SKU (0 se não existir)."
  def stock(sku) when is_binary(sku) do
    GenServer.call(__MODULE__, {:stock, sku})
  end

  @doc """
  Consulta o estado de uma operação pelo id, procurando primeiro entre as
  operações em andamento e depois no histórico.
  """
  def operation_status(id) do
    GenServer.call(__MODULE__, {:operation_status, id})
  end

  @doc "Lista as operações em andamento, da mais antiga para a mais recente."
  def running_operations do
    GenServer.call(__MODULE__, :running_operations)
  end

  @doc "Retorna as últimas `limit` operações finalizadas (mais recente primeiro)."
  def history(limit \\ 50) when is_integer(limit) and limit > 0 do
    GenServer.call(__MODULE__, {:history, limit})
  end

  # ---------------------------------------------------------------------------
  # Callbacks do GenServer
  # ---------------------------------------------------------------------------

  @impl true
  def init(opts) do
    state = %{
      # %{sku => saldo}
      stock: %{},
      # %{id => operação em andamento}
      running: %{},
      # lista de operações finalizadas, mais recente primeiro
      history: [],
      history_limit: Keyword.get(opts, :history_limit, @default_history_limit)
    }

    {:ok, state}
  end

  @impl true
  def handle_call({:begin, type, meta}, _from, state) do
    id = :erlang.unique_integer([:positive, :monotonic])

    op =
      Map.merge(
        %{
          id: id,
          type: type,
          status: :running,
          processed: 0,
          applied: 0,
          invalid: 0,
          rejected: 0,
          started_at: now(),
          finished_at: nil,
          reason: nil
        },
        meta
      )

    {:reply, {:ok, id}, put_in(state.running[id], op)}
  end

  # Aplica um lote de movimentos de uma importação em andamento.
  def handle_call({:apply_batch, id, entries}, _from, state) do
    case Map.fetch(state.running, id) do
      :error ->
        {:reply, {:error, :not_running}, state}

      {:ok, op} ->
        {stock, counters} =
          Enum.reduce(entries, {state.stock, %{applied: 0, invalid: 0, rejected: 0}}, &apply_entry/2)

        op = %{
          op
          | processed: op.processed + length(entries),
            applied: op.applied + counters.applied,
            invalid: op.invalid + counters.invalid,
            rejected: op.rejected + counters.rejected
        }

        new_state = %{state | stock: stock, running: Map.put(state.running, id, op)}
        {:reply, :ok, new_state}
    end
  end

  # Finaliza uma operação em andamento e move-a para o histórico.
  def handle_call({:finish, id, result}, _from, state) do
    case Map.pop(state.running, id) do
      {nil, _} ->
        {:reply, {:error, :not_running}, state}

      {op, running} ->
        op =
          case result do
            :ok -> %{op | status: :done, finished_at: now()}
            {:error, reason} -> %{op | status: :failed, reason: reason, finished_at: now()}
          end

        new_state = %{state | running: running} |> record(op)
        {:reply, :ok, new_state}
    end
  end

  def handle_call({:adjust, sku, delta}, _from, state) do
    current = Map.get(state.stock, sku, 0)
    new_balance = current + delta

    base = %{
      id: :erlang.unique_integer([:positive, :monotonic]),
      type: :adjust,
      sku: sku,
      delta: delta,
      started_at: now(),
      finished_at: now()
    }

    if new_balance < 0 do
      op = Map.merge(base, %{status: :rejected, reason: :estoque_insuficiente})
      {:reply, {:error, :estoque_insuficiente}, record(state, op)}
    else
      op = Map.merge(base, %{status: :done, balance: new_balance})
      new_state = %{state | stock: Map.put(state.stock, sku, new_balance)} |> record(op)
      {:reply, {:ok, new_balance}, new_state}
    end
  end

  def handle_call({:stock, sku}, _from, state) do
    {:reply, Map.get(state.stock, sku, 0), state}
  end

  def handle_call({:operation_status, id}, _from, state) do
    reply =
      case Map.fetch(state.running, id) do
        {:ok, op} ->
          {:ok, op}

        :error ->
          case Enum.find(state.history, &(&1.id == id)) do
            nil -> {:error, :not_found}
            op -> {:ok, op}
          end
      end

    {:reply, reply, state}
  end

  def handle_call(:running_operations, _from, state) do
    ops =
      state.running
      |> Map.values()
      |> Enum.sort_by(& &1.started_at)

    {:reply, ops, state}
  end

  def handle_call({:history, limit}, _from, state) do
    {:reply, Enum.take(state.history, limit), state}
  end

  # ---------------------------------------------------------------------------
  # Processamento em background (streaming)
  # ---------------------------------------------------------------------------

  # Lê o arquivo sob demanda, agrupa em lotes e envia cada lote ao GenServer.
  # Em nenhum momento o arquivo inteiro é carregado na memória.
  defp run_import(id, path, chunk_size) do
    result =
      try do
        path
        |> File.stream!()
        |> Stream.map(&String.trim/1)
        |> Stream.reject(&(&1 == ""))
        |> Stream.chunk_every(chunk_size)
        |> Enum.each(fn lines ->
          entries = Enum.map(lines, &parse_line/1)
          :ok = GenServer.call(__MODULE__, {:apply_batch, id, entries}, :infinity)
        end)

        :ok
      rescue
        e -> {:error, Exception.message(e)}
      catch
        kind, reason -> {:error, {kind, reason}}
      end

    GenServer.call(__MODULE__, {:finish, id, result}, :infinity)
  end

  # Converte "sku;delta" em {:ok, sku, delta}; qualquer outra coisa vira :error.
  defp parse_line(line) do
    with [sku, qty] when sku != "" <- String.split(line, ";", parts: 2),
         {n, ""} <- Integer.parse(String.trim(qty)) do
      {:ok, String.trim(sku), n}
    else
      _ -> :error
    end
  end

  # Aplica uma entrada ao acumulador {saldos, contadores} sem alterar nada
  # fora dele. Saídas que deixariam o saldo negativo são rejeitadas.
  defp apply_entry(:error, {stock, c}) do
    {stock, %{c | invalid: c.invalid + 1}}
  end

  defp apply_entry({:ok, sku, delta}, {stock, c}) do
    new_balance = Map.get(stock, sku, 0) + delta

    if new_balance < 0 do
      {stock, %{c | rejected: c.rejected + 1}}
    else
      {Map.put(stock, sku, new_balance), %{c | applied: c.applied + 1}}
    end
  end

  # ---------------------------------------------------------------------------
  # Utilidades internas
  # ---------------------------------------------------------------------------

  # Insere uma operação finalizada no início do histórico, respeitando o limite.
  defp record(state, op) do
    history = Enum.take([op | state.history], state.history_limit)
    %{state | history: history}
  end

  defp now, do: System.system_time(:millisecond)
end
