# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de troca de mensagens entre usuários, atendendo somente aos seguintes requisitos:
#
# 1. Persistir os dados para recuperação após reinicializações.
# 2. Implementar novas tentativas após falhas temporárias.
# 3. Processar grandes volumes de dados sem carregá-los integralmente na memória.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# Para persistência simples e eficiente, utilizamos o módulo :dets (Disk-based Erlang Term Storage).
# Para processamento em streaming (sem carregar tudo na memória), usamos o módulo Stream.
# Para novas tentativas (retry), usamos um padrão recursivo com backoff exponencial.

defmodule MessagingSystem do
  @db_file "messages.dets"

  # Inicia o banco de dados persistente
  def start_db do
    :dets.open_file(:messages, [file: @db_file, type: :bag])
  end

  # Salva mensagem com lógica de retry
  def save_message(message, attempt \\ 1) do
    try do
      :dets.insert(:messages, {System.system_time(:microsecond), message})
    rescue
      _ ->
        if attempt <= 3 do
          Process.sleep(100 * attempt)
          save_message(message, attempt + 1)
        else
          {:error, :max_retries_exceeded}
        end
    end
  end

  # Processa mensagens usando Stream para evitar carga total em memória
  # O uso de :dets.match_spec_run permite leitura segmentada
  def process_messages(callback) do
    Stream.resource(
      fn -> :dets.match_spec_run(:messages, :ets.match_spec_compile([{{:_, :"$1"}, [], [:"$1"]}])) end,
      fn
        {results, continuation} -> {results, :dets.match_spec_run(:messages, continuation)}
        :"$end_of_table" -> {:halt, nil}
      end,
      fn _ -> :ok end
    )
    |> Stream.each(callback)
    |> Stream.run()
  end

  # Exemplo de uso:
  # MessagingSystem.start_db()
  # MessagingSystem.save_message("Olá mundo")
  # MessagingSystem.process_messages(fn msg -> IO.puts("Processando: #{msg}") end)
end
