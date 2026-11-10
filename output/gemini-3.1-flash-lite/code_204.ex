# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de troca de mensagens entre usuários, atendendo somente aos seguintes requisitos:
#
# 1. Persistir os dados para recuperação após reinicializações.
# 2. Implementar novas tentativas após falhas temporárias.
# 3. Processar grandes volumes de dados sem carregá-los integralmente na memória.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# Para persistência simples e eficiente sem carregar tudo na memória, usamos :dets (Disk-based Erlang Term Storage).
# Para processamento em lote (streaming), usamos Stream para evitar sobrecarga de memória.
# Para novas tentativas, utilizamos uma estratégia de recursão com backoff exponencial.

defmodule Messenger do
  @table_name :messages_store
  @max_retries 3

  def start do
    :dets.open_file(@table_name, [type: :set, file: 'messages.dets'])
  end

  # Persistência com nova tentativa
  def save_message(user_id, message), do: retry_operation(fn -> :dets.insert(@table_name, {System.system_time(), user_id, message}) end)

  # Processamento de grandes volumes via Stream
  # O uso de :dets.match_spec permite ler o arquivo sem carregar toda a tabela na RAM
  def stream_messages do
    Stream.resource(
      fn -> :dets.match_spec(@table_name, [{{:"$1", :"$2", :"$3"}, [], [{{:"$1", :"$2", :"$3"}}]}]) end,
      fn 
        :"$end_of_table" -> {:halt, nil}
        continuation -> 
          case :dets.select(continuation) do
            {results, new_continuation} -> {results, new_continuation}
            :"$end_of_table" -> {:halt, nil}
          end
      end,
      fn _ -> :ok end
    )
  end

  defp retry_operation(func, attempt \\ 0) do
    try do
      func.()
    rescue
      _ ->
        if attempt < @max_retries do
          Process.sleep(100 * :math.pow(2, attempt) |> round())
          retry_operation(func, attempt + 1)
        else
          {:error, :max_retries_exceeded}
        end
    end
  end

  def close do
    :dets.close(@table_name)
  end
end

# Exemplo de uso:
# Messenger.start()
# Messenger.save_message("user_1", "Olá mundo")
# Messenger.stream_messages() |> Enum.each(fn {ts, u, m} -> IO.puts("#{u}: #{m}") end)
