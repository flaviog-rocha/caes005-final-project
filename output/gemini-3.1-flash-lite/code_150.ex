# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de pedidos de um e-commerce, atendendo somente aos seguintes requisitos:
#
# 1. Gerar notificações quando determinadas condições forem atingidas.
# 2. Detectar encerramentos inesperados de processos.
# 3. Persistir os dados para recuperação após reinicializações.
# 4. Validar os dados recebidos antes de realizar operações.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule OrderSystem do
  use GenServer

  # Requisito 3: Persistência simples em disco (arquivo binário)
  @storage_file "orders.ets"

  # API Pública
  def start_link(_), do: GenServer.start_link(__MODULE__, %{}, name: __MODULE__)

  def create_order(id, amount) do
    GenServer.call(__MODULE__, {:create, id, amount})
  end

  # Callbacks do GenServer
  @impl true
  def init(_) do
    # Requisito 2: Detectar encerramentos (Monitoramento)
    Process.flag(:trap_exit, true)
    
    # Recuperação de dados
    state = if File.exists?(@storage_file) do
      :erlang.binary_to_term(File.read!(@storage_file))
    else
      %{}
    end
    {:ok, state}
  end

  @impl true
  def handle_call({:create, id, amount}, _from, state) do
    # Requisito 4: Validação de dados
    case validate_order(id, amount) do
      :ok ->
        new_state = Map.put(state, id, amount)
        save_to_disk(new_state)
        
        # Requisito 1: Notificação por condição
        if amount > 1000, do: notify_admin(id, amount)
        
        {:reply, {:ok, "Pedido #{id} criado."}, new_state}
      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  # Requisito 2: Monitoramento de processos externos
  @impl true
  def handle_info({:EXIT, _pid, reason}, state) do
    IO.puts("Alerta: Processo monitorado encerrou: #{inspect(reason)}")
    {:noreply, state}
  end

  # Funções Auxiliares
  defp validate_order(id, amount) when is_binary(id) and amount > 0, do: :ok
  defp validate_order(_, _), do: {:error, "Dados inválidos."}

  defp save_to_disk(state) do
    File.write!(@storage_file, :erlang.term_to_binary(state))
  end

  defp notify_admin(id, amount) do
    IO.puts("Notificação: Pedido de alto valor detectado! ID: #{id}, Valor: #{amount}")
  end
end
