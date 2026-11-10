# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de entregas e rastreamento, atendendo somente aos seguintes requisitos:
#
# 1. Validar os dados recebidos antes de realizar operações.
# 2. Executar determinadas operações periodicamente.
# 3. Gerar notificações quando determinadas condições forem atingidas.
# 4. Processar grandes volumes de dados sem carregá-los integralmente na memória.
# 5. Impedir que uma mesma operação seja executada mais de uma vez.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - Stream
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule DeliveryManager do
  @moduledoc """
  Sistema de gerenciamento de entregas utilizando processamento em stream
  para alta performance e baixo consumo de memória.
  """

  # 1. Validação de dados
  def validate_delivery(%{id: id, status: status, weight: weight}) when is_integer(id) and weight > 0 do
    {:ok, %{id: id, status: status, weight: weight}}
  end
  def validate_delivery(_), do: {:error, :invalid_data}

  # 4. Processamento de grandes volumes via Stream (Lazy evaluation)
  def process_deliveries(data_source) do
    data_source
    |> Stream.map(&validate_delivery/1)
    |> Stream.filter(fn {:ok, _} -> true; _ -> false end)
    |> Stream.map(fn {:ok, delivery} -> delivery end)
    |> Stream.each(&ensure_unique_execution/1)
    |> Stream.each(&trigger_notifications/1)
    |> Stream.run()
  end

  # 5. Impedir execução duplicada usando ETS como cache de controle (Idempotência)
  defp ensure_unique_execution(delivery) do
    :ets.new(:processed_deliveries, [:set, :public, :named_table]) rescue _ -> nil
    
    case :ets.insert_new(:processed_deliveries, {delivery.id, System.system_time()}) do
      true -> :ok
      false -> raise "Operação duplicada detectada: #{delivery.id}"
    end
  end

  # 3. Geração de notificações
  defp trigger_notifications(%{status: :urgent} = delivery) do
    IO.puts("Notificação enviada: Entrega #{delivery.id} requer atenção imediata!")
  end
  defp trigger_notifications(_), do: :ok

  # 2. Operações periódicas utilizando um GenServer (Loop recursivo)
  def start_periodic_task(interval_ms) do
    spawn(fn -> loop_task(interval_ms) end)
  end

  defp loop_task(interval_ms) do
    IO.puts("Executando verificação periódica de sistema...")
    # Lógica de processamento periódico aqui
    Process.sleep(interval_ms)
    loop_task(interval_ms)
  end
end

# Exemplo de uso:
# dados = [%{id: 1, status: :normal, weight: 10}, %{id: 2, status: :urgent, weight: 5}]
# DeliveryManager.process_deliveries(dados)
# DeliveryManager.start_periodic_task(60_000)
