# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de empréstimos de livros, atendendo somente aos seguintes requisitos:
#
# 1. Persistir os dados para recuperação após reinicializações.
# 2. Validar os dados recebidos antes de realizar operações.
# 3. Permitir cadastrar, consultar, atualizar e remover registros.
# 4. Executar determinadas operações periodicamente.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - Task
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule LibraryManager do
  @moduledoc """
  Gerenciador de empréstimos de livros com persistência, validação e tarefas periódicas.
  """
  use Agent

  @file_path "library_data.ets"

  def start_link(_opts) do
    # Carrega dados do disco ou inicia vazio
    initial_state = load_data()
    {:ok, _pid} = Agent.start_link(fn -> initial_state end, name: __MODULE__)
    
    # Inicia a tarefa de monitoramento periódico (ex: limpeza ou notificação)
    spawn_periodic_task()
    
    {:ok, __MODULE__}
  end

  # --- API Pública ---

  def register_book(id, title) when is_binary(id) and is_binary(title) do
    Agent.update(__MODULE__, fn state ->
      new_state = Map.put(state, id, %{title: title, borrowed: false, updated_at: DateTime.utc_now()})
      persist(new_state)
      new_state
    end)
  end

  def get_book(id), do: Agent.get(__MODULE__, &Map.get(&1, id))

  def update_book(id, title) when is_binary(id) and is_binary(title) do
    Agent.update(__MODULE__, fn state ->
      if Map.has_key?(state, id) do
        new_state = Map.put(state, id, %{title: title, borrowed: false, updated_at: DateTime.utc_now()})
        persist(new_state)
        new_state
      else
        state
      end
    end)
  end

  def remove_book(id) do
    Agent.update(__MODULE__, fn state ->
      new_state = Map.delete(state, id)
      persist(new_state)
      new_state
    end)
  end

  # --- Tarefas Periódicas ---

  defp spawn_periodic_task do
    Task.start(fn ->
      loop_periodic_task()
    end)
  end

  defp loop_periodic_task do
    # Simula uma operação periódica (ex: verificação de vencimento) a cada 60 segundos
    :timer.sleep(60_000)
    IO.puts("[#{DateTime.utc_now()}] Executando rotina periódica de manutenção...")
    loop_periodic_task()
  end

  # --- Persistência e Utilitários ---

  defp persist(data) do
    File.write!(@file_path, :erlang.term_to_binary(data))
  end

  defp load_data do
    if File.exists?(@file_path) do
      @file_path |> File.read!() |> :erlang.binary_to_term()
    else
      %{}
    end
  end
end

# --- Exemplo de uso ---
# LibraryManager.start_link([])
# LibraryManager.register_book("1", "O Alquimista")
# IO.inspect(LibraryManager.get_book("1"))
# LibraryManager.update_book("1", "O Alquimista - Edição Especial")
# LibraryManager.remove_book("1")
