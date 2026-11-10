# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de troca de mensagens entre usuários, atendendo somente aos seguintes requisitos:
#
# 1. Fazer uso de macros.
# 2. Permitir comunicação entre diferentes processos.
# 3. Garantir a consistência dos dados durante operações concorrentes.
# 4. Persistir os dados para recuperação após reinicializações.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - Task
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule MessagingSystem do
  @moduledoc """
  Sistema de mensagens com concorrência segura, persistência em ETS e macros.
  """

  # Macro para simplificar a criação de novas mensagens e envio entre processos
  defmacro send_msg(from, to, content) do
    quote do
      GenServer.cast(MessagingSystem.Server, {:send, unquote(from), unquote(to), unquote(content)})
    end
  end

  defmodule Server do
    use GenServer
    @table_name :messages_table
    @file_path "messages.dat"

    def start_link(_), do: GenServer.start_link(__MODULE__, [], name: __MODULE__)

    @impl true
    def init(_) do
      # Garantir consistência: ETS com proteção pública e persistência em disco
      table = :ets.new(@table_name, [:set, :named_table, :public, read_concurrency: true])
      load_from_disk(table)
      {:ok, table}
    end

    @impl true
    def handle_cast({:send, from, to, content}, state) do
      msg = %{from: from, to: to, content: content, timestamp: System.system_time(:second)}
      :ets.insert(@table_name, {System.unique_integer(), msg})
      persist_to_disk()
      {:noreply, state}
    end

    defp persist_to_disk do
      # Task para não bloquear o loop do GenServer durante a escrita em disco
      Task.start(fn ->
        data = :ets.tab2list(@table_name)
        :erlang.term_to_binary(data) |> then(&File.write!(@file_path, &1))
      end)
    end

    defp load_from_disk(table) do
      if File.exists?(@file_path) do
        File.read!(@file_path)
        |> :erlang.binary_to_term()
        |> Enum.each(&:ets.insert(table, &1))
      end
    end
  end

  # Demonstração de uso
  def run_example do
    {:ok, _} = MessagingSystem.Server.start_link([])

    # Uso da macro
    require MessagingSystem
    MessagingSystem.send_msg("Alice", "Bob", "Olá, mundo concorrente!")

    # Verificação da persistência e concorrência
    Task.start(fn ->
      MessagingSystem.send_msg("Bob", "Alice", "Mensagem recebida.")
    end)

    :timer.sleep(100)
    IO.inspect(:ets.tab2list(:messages_table), label: "Estado Atual das Mensagens")
  end
end
