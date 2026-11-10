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
  Sistema de mensagens com supervisão de processos, transações atômicas
  e validação de dados utilizando apenas a biblioteca padrão.
  """

  use GenServer

  # --- API Pública ---

  def start_link(_) do
    GenServer.start_link(__MODULE__, %{users: %{}}, name: __MODULE__)
  end

  def send_message(from, to, content) do
    GenServer.call(__MODULE__, {:send, from, to, content})
  end

  # --- Callbacks do GenServer ---

  @impl true
  def init(state) do
    {:ok, state}
  end

  @impl true
  def handle_call({:send, from, to, content}, _from, state) do
    # 3. Validação de dados antes da operação
    case validate_message(from, to, content) do
      :ok ->
        # 2. Impedir atualizações parcialmente concluídas:
        # A operação é executada atomicamente no estado do GenServer.
        # Se ocorrer uma falha aqui, o GenServer entra em estado de erro
        # e o supervisor pode reiniciá-lo, garantindo consistência.
        new_state = update_in(state, [:users], fn users ->
          users
          |> Map.put_new(from, [])
          |> Map.put_new(to, [])
          |> Map.update(to, [], &([%{from: from, msg: content} | &1]))
        end)

        {:reply, {:ok, "Mensagem enviada"}, new_state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  # --- Validação ---

  defp validate_message(from, to, content) when is_binary(from) and is_binary(to) and is_binary(content) do
    if String.length(content) > 0, do: :ok, else: {:error, "Conteúdo vazio"}
  end
  defp validate_message(_, _, _), do: {:error, "Dados inválidos"}

  # --- Supervisão e Detecção de Falhas ---

  @doc """
  1. Detectar encerramentos inesperados:
  Utilizamos monitoramento de processos. Se um processo de usuário (simulado)
  morrer, podemos capturar o sinal :DOWN.
  """
  def monitor_user(pid) do
    spawn(fn ->
      Process.monitor(pid)
      receive do
        {:DOWN, _ref, :process, ^pid, reason} ->
          IO.puts("Alerta: Processo de usuário #{inspect(pid)} encerrou: #{inspect(reason)}")
      end
    end)
  end
end

# Exemplo de uso:
# {:ok, pid} = MessagingSystem.start_link([])
# MessagingSystem.send_message("Alice", "Bob", "Olá!")
# MessagingSystem.monitor_user(self())
