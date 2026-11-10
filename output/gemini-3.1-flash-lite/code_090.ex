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
  def validate_delivery(%{id: id, status: status, destination: dest}) when is_binary(id) and is_binary(dest) do
    {:ok, %{id: id, status: status, destination: dest, processed_at: System.system_time(:second)}}
  end
  def validate_delivery(_), do: {:error, :invalid_payload}

  # 5. Impedir execução duplicada (usando Agent como cache de idempotência)
  def start_idempotency_check do
    Agent.start_link(fn -> MapSet.new() end, name: __MODULE__.IdempotencyTable)
  end

  defp already_processed?(id) do
    Agent.get_and_update(__MODULE__.IdempotencyTable, fn set ->
      {MapSet.member?(set, id), MapSet.put(set, id)}
    end)
  end

  # 4. Processamento de grandes volumes (Stream)
  def process_deliveries(data_source) do
    data_source
    |> Stream.map(&validate_delivery/1)
    |> Stream.filter(fn {:ok, data} -> !already_processed?(data.id) end)
    |> Stream.each(&notify_if_critical/1)
    |> Stream.run()
  end

  # 3. Notificações baseadas em condições
  defp notify_if_critical({:ok, %{status: :delayed} = data}) do
    IO.puts("ALERTA: Entrega #{data.id} está atrasada para #{data.destination}")
  end
  defp notify_if_critical(_), do: :ok

  # 2. Operações periódicas (usando Task para agendamento)
  def schedule_cleanup(interval_ms) do
    Task.start_link(fn ->
      loop_cleanup(interval_ms)
    end)
  end

  defp loop_cleanup(interval) do
    :timer.sleep(interval)
    IO.puts("Executando limpeza periódica de cache...")
    Agent.update(__MODULE__.IdempotencyTable, fn _ -> MapSet.new() end)
    loop_cleanup(interval)
  end
end

# Exemplo de uso:
# DeliveryManager.start_idempotency_check()
# DeliveryManager.schedule_cleanup(60_000)

# Simulação de stream de dados massivo (ex: lendo de um arquivo ou socket)
# deliveries = Stream.repeatedly(fn -> %{id: "ID-#{:rand.uniform(100)}", status: :delayed, destination: "SP"} end)
# DeliveryManager.process_deliveries(Stream.take(deliveries, 1000))
