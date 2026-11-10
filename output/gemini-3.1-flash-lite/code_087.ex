# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de troca de mensagens entre usuários, atendendo somente aos seguintes requisitos:
#
# 1. Detectar encerramentos inesperados de processos.
# 2. Impedir atualizações parcialmente concluídas.
# 3. Validar os dados recebidos antes de realizar operações.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - Process
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule MessagingSystem do
  @moduledoc """
  Sistema de troca de mensagens utilizando Processos Elixir, garantindo
  integridade de estado, validação e monitoramento de falhas.
  """

  use GenServer

  # --- API Pública ---

  def start_link(_), do: GenServer.start_link(__MODULE__, %{}, name: __MODULE__)

  def send_message(from, to, content) when is_binary(content) and byte_size(content) > 0 do
    GenServer.call(__MODULE__, {:send, from, to, content})
  end
  def send_message(_, _, _), do: {:error, :invalid_payload}

  # --- Callbacks do GenServer ---

  @impl true
  def init(state) do
    Process.flag(:trap_exit, true)
    {:ok, state}
  end

  @impl true
  def handle_call({:send, from, to, content}, _from, state) do
    # Requisito 2: Impedir atualizações parcialmente concluídas.
    # O estado só é alterado se ambas as operações forem bem-sucedidas.
    new_state =
      state
      |> Map.update(from, [content], &([content | &1]))
      |> Map.update(to, [content], &([content | &1]))

    {:reply, :ok, new_state}
  end

  # Requisito 1: Detectar encerramentos inesperados.
  @impl true
  def handle_info({:EXIT, pid, reason}, state) do
    IO.puts("Processo #{inspect(pid)} encerrou inesperadamente: #{inspect(reason)}")
    # Logica de recuperação de estado pode ser inserida aqui
    {:noreply, state}
  end

  # --- Exemplo de Uso e Validação ---

  def test_run do
    {:ok, _} = MessagingSystem.start_link([])

    # Validação (Requisito 3)
    IO.inspect(MessagingSystem.send_message("Alice", "Bob", "Olá!"))
    IO.inspect(MessagingSystem.send_message("Alice", "Bob", "")) # Deve falhar

    # Monitoramento
    spawn_link(fn -> exit(:boom) end)
    :timer.sleep(100)
  end
end
