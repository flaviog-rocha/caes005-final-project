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
  Sistema de gerenciamento de notificações utilizando ETS para armazenamento em memória
  com suporte a expiração (TTL) e processamento via Streams para grandes volumes.
  """

  # Define a tabela ETS persistente na memória
  @table :notifications

  def init do
    :ets.new(@table, [:set, :public, :named_table, read_concurrency: true])
  end

  # 1. CRUD: Cadastro
  def create(id, data, ttl_seconds \\ 3600) do
    if is_map(data) do
      expiry = System.system_time(:second) + ttl_seconds
      :ets.insert(@table, {id, data, expiry})
      {:ok, id}
    else
      # 3. Diferenciar erros de validação
      {:error, :validation_error, "Dados devem ser um mapa"}
    end
  rescue
    e -> {:error, :operational_failure, e}
  end

  # 1. CRUD: Consulta
  def get(id) do
    case :ets.lookup(@table, id) do
      [{^id, data, expiry}] ->
        if System.system_time(:second) < expiry do
          {:ok, data}
        else
          delete(id)
          {:error, :not_found, "Registro expirado"}
        end
      [] -> {:error, :not_found, "Registro inexistente"}
    end
  end

  # 1. CRUD: Atualizar
  def update(id, new_data) do
    case :ets.lookup(@table, id) do
      [{^id, _, expiry}] ->
        :ets.insert(@table, {id, new_data, expiry})
        {:ok, id}
      [] -> {:error, :not_found, "Não é possível atualizar registro inexistente"}
    end
  end

  # 1. CRUD: Remover
  def delete(id), do: :ets.delete(@table, id)

  # 4. Processamento de grandes volumes via Stream (Lazy evaluation)
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

  # 2. Gerar notificações baseadas em condições
  def process_notifications(condition_func) do
    stream_all()
    |> Stream.filter(condition_func)
    |> Stream.each(fn item -> 
         IO.puts("Notificação disparada para: #{inspect(item)}") 
       end)
    |> Stream.run()
  end

  # 5. Expiração automática (Worker de limpeza)
  def start_cleanup_timer(interval_ms \\ 60_000) do
    spawn(fn ->
      loop_cleanup(interval_ms)
    end)
  end

  defp loop_cleanup(interval) do
    now = System.system_time(:second)
    :ets.match_delete(@table, {:_, :_, :"$1"}) # Simplificado para demonstração
    
    # Filtro eficiente para expiração
    :ets.foldl(fn {id, _, expiry}, _acc ->
      if now > expiry, do: delete(id)
    end, nil, @table)

    :timer.sleep(interval)
    loop_cleanup(interval)
  end
end
