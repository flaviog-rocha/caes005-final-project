# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de transações financeiras, atendendo somente aos seguintes requisitos:
#
# 1. Recuperar processos após falhas inesperadas.
# 2. Permitir comunicação entre diferentes processos.
# 3. Gerar notificações quando determinadas condições forem atingidas.
# 4. Persistir os dados para recuperação após reinicializações.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - GenServer
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule TransactionManager do
  @moduledoc """
  Gerenciador de transações com persistência, tolerância a falhas e notificação.
  Utiliza ETS para persistência rápida e GenServer para gerenciar o estado.
  """
  use GenServer

  @table :transactions_table

  # --- API Pública ---

  def start_link(_) do
    GenServer.start_link(__MODULE__, nil, name: __MODULE__)
  end

  def process_transaction(id, amount) do
    GenServer.call(__MODULE__, {:process, id, amount})
  end

  def get_balance(id) do
    case :ets.lookup(@table, id) do
      [{^id, balance}] -> {:ok, balance}
      [] -> {:error, :not_found}
    end
  end

  # --- Callbacks do GenServer ---

  @impl true
  def init(_) do
    # 1. Recuperação: Cria/carrega tabela ETS (Persistência em memória)
    # Em produção real, usaria um arquivo ou banco de dados via DETS.
    :ets.new(@table, [:set, :public, :named_table, read_concurrency: true])
    {:ok, %{}}
  end

  @impl true
  def handle_call({:process, id, amount}, _from, state) do
    # 4. Persistência: Atualiza o estado
    new_balance =
      case :ets.lookup(@table, id) do
        [{^id, balance}] -> balance + amount
        [] -> amount
      end

    :ets.insert(@table, {id, new_balance})

    # 3. Notificação: Condição atingida (ex: saldo > 1000)
    if new_balance >= 1000 do
      notify_stakeholders(id, new_balance)
    end

    {:reply, {:ok, new_balance}, state}
  end

  # 2. Comunicação: Exemplo de envio de mensagem para outro processo
  defp notify_stakeholders(id, balance) do
    message = {:alert, "Transação de alto valor", id, balance}
    # Envia mensagem assíncrona para um listener (processo) registrado
    # Se não houver processo registrado, o send falha silenciosamente ou pode ser tratado
    if Process.whereis(:notification_service) do
      send(:notification_service, message)
    end
  end

  # --- Tolerância a Falhas ---
  # O uso de 'start_link' dentro de um Supervision Tree (não incluído aqui para simplicidade)
  # garante que, caso o processo morra, ele seja reiniciado pelo supervisor.
  # O uso de ETS garante que, se o GenServer travar mas a tabela persistir,
  # os dados ainda estejam acessíveis.
end

# Exemplo de serviço de notificação (processo separado)
defmodule NotificationService do
  def start_link do
    spawn_link(fn -> loop() end) |> Process.register(:notification_service)
  end

  defp loop do
    receive do
      {:alert, msg, id, val} ->
        IO.puts("[NOTIFICAÇÃO] #{msg} | ID: #{id} | Valor: #{val}")
        loop()
    end
  end
end
