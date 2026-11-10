# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de gerenciamento de notificações, atendendo somente aos seguintes requisitos:
#
# 1. Permitir cadastrar, consultar, atualizar e remover registros.
# 2. Gerar notificações quando determinadas condições forem atingidas.
# 3. Diferenciar erros de validação de falhas operacionais.
# 4. Processar grandes volumes de dados sem carregá-los integralmente na memória.
# 5. Implementar expiração automática de registros.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule NotificationSystem do
  @moduledoc """
  Sistema de gerenciamento de notificações usando ETS para armazenamento eficiente,
  Streams para processamento de memória constante e processos para expiração.
  """

  @table :notifications

  def start do
    :ets.new(@table, [:set, :public, :named_table, read_concurrency: true])
    :ok
  end

  # 1. CRUD
  def create(id, data, ttl_seconds \\ 3600) do
    if is_map(data) and Map.has_key?(data, :message) do
      expiry = System.system_time(:second) + ttl_seconds
      :ets.insert(@table, {id, data, expiry})
      {:ok, id}
    else
      {:error, :validation_failed, "Data must be a map containing :message"}
    end
  rescue
    e -> {:error, :operational_failure, e}
  end

  def get(id) do
    case :ets.lookup(@table, id) do
      [{^id, data, _}] -> {:ok, data}
      [] -> {:error, :not_found, "Notification not found"}
    end
  end

  def update(id, new_data) do
    case :ets.lookup(@table, id) do
      [{^id, _, expiry}] ->
        :ets.insert(@table, {id, new_data, expiry})
        {:ok, id}
      [] -> {:error, :not_found, "Cannot update non-existent record"}
    end
  end

  def delete(id), do: :ets.delete(@table, id)

  # 2. Geração de notificações (Regras de negócio)
  def trigger_if(id, predicate_fun) do
    case get(id) do
      {:ok, data} ->
        if predicate_fun.(data), do: send_notification(id, data), else: :noop
      error -> error
    end
  end

  defp send_notification(id, data), do: IO.puts("Notifying #{id}: #{data.message}")

  # 4. Processamento de grandes volumes via Stream (sem carregar tudo)
  def stream_all do
    Stream.resource(
      fn -> :ets.first(@table) end,
      fn
        :"$end_of_table" -> {:halt, nil}
        key ->
          case :ets.lookup(@table, key) do
            [{^key, data, _}] -> {[data], :ets.next(@table, key)}
            [] -> {[], :ets.next(@table, key)}
          end
      end,
      fn _ -> :ok end
    )
  end

  # 5. Expiração automática (Worker de limpeza)
  def start_janitor(interval_ms \\ 60_000) do
    spawn(fn ->
      loop_janitor(interval_ms)
    end)
  end

  defp loop_janitor(interval) do
    now = System.system_time(:second)
    :ets.foldl(fn {id, _, expiry}, _acc ->
      if now > expiry, do: :ets.delete(@table, id)
    end, nil, @table)
    
    :timer.sleep(interval)
    loop_janitor(interval)
  end
end
