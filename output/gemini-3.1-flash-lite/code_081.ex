# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de gerenciamento de notificações, atendendo somente aos seguintes requisitos:
#
# 1. Permitir que diferentes entidades sejam processadas independentemente.
# 2. Implementar novas tentativas após falhas temporárias.
# 3. Processar grandes volumes de dados sem carregá-los integralmente na memória.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - DETS
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule NotificationManager do
  @moduledoc """
  Sistema de gerenciamento de notificações utilizando DETS para persistência em disco,
  processamento lazy (via streams) para eficiência de memória e um modelo de 
  processamento de concorrência por entidade.
  """

  @table_name :notifications_db
  @max_retries 3

  def start do
    :dets.open_file(@table_name, type: :set)
  end

  # Requisito 3: Processamento eficiente de memória usando Stream
  # Carrega registros sob demanda em vez de carregar toda a tabela na RAM
  def process_all do
    Stream.resource(
      fn -> :dets.first(@table_name) end,
      fn
        :"$end_of_table" -> {:halt, nil}
        key ->
          case :dets.lookup(@table_name, key) do
            [{^key, notification}] -> {[notification], :dets.next(@table_name, key)}
            [] -> {[], :dets.next(@table_name, key)}
          end
      end,
      fn _ -> :ok end
    )
    |> Stream.each(&spawn_worker/1)
    |> Stream.run()
  end

  # Requisito 1: Processamento independente por entidade
  # Dispara um processo isolado para cada notificação
  defp spawn_worker(notification) do
    spawn(fn -> execute_with_retry(notification, 0) end)
  end

  # Requisito 2: Implementação de novas tentativas (Retry)
  defp execute_with_retry(notification, attempt) when attempt < @max_retries do
    case perform_delivery(notification) do
      :ok ->
        :dets.delete(@table_name, notification.id)
      :error ->
        :timer.sleep(:math.pow(2, attempt) |> round() |> :timer.seconds())
        execute_with_retry(notification, attempt + 1)
    end
  end
  defp execute_with_retry(notification, _attempt), do: {:failed, notification.id}

  # Simulação de IO para processamento
  defp perform_delivery(_notification), do: Enum.random([:ok, :error])

  def add_notification(id, payload) do
    :dets.insert(@table_name, {id, %{id: id, payload: payload}})
  end
end
