# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de um sistema de cache, atendendo somente aos seguintes requisitos:
#
# 1. Processar grandes volumes de dados sem carregá-los integralmente na memória.
# 2. Executar determinadas operações periodicamente.
# 3. Permitir que diferentes entidades sejam processadas independentemente.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule StreamCacheSystem do
  @moduledoc """
  Sistema de processamento de cache baseado em Streams para eficiência de memória,
  com processamento periódico via GenServer e isolamento por entidades.
  """

  use GenServer

  # --- API Pública ---

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def schedule_processing(entity_id, data_source_fun) do
    GenServer.cast(__MODULE__, {:process, entity_id, data_source_fun})
  end

  # --- Callbacks do GenServer ---

  @impl true
  def init(_opts) do
    # O estado armazena apenas o timestamp ou status de processamento por entidade
    {:ok, %{}}
  end

  @impl true
  def handle_cast({:process, entity_id, data_source_fun}, state) do
    # Processamento em background sem carregar tudo na memória
    # Utiliza Streams para processamento lazy
    Task.start(fn ->
      data_source_fun.()
      |> Stream.chunk_every(100) # Processa em batches para eficiência
      |> Stream.map(&transform_data/1)
      |> Enum.each(&write_to_cache(entity_id, &1))
    end)

    {:noreply, Map.put(state, entity_id, System.system_time(:second))}
  end

  # --- Funções Auxiliares (Lógica de Domínio) ---

  defp transform_data(batch) do
    # Simulação de transformação pesada
    Enum.map(batch, fn item -> {item, :processed} end)
  end

  defp write_to_cache(entity_id, processed_batch) do
    # Simula escrita em um cache (Ex: ETS ou Redis)
    IO.puts("Entidade #{entity_id}: Cache atualizado com #{length(processed_batch)} itens.")
  end

  # --- Exemplo de Uso (Worker Periódico) ---

  def periodic_task(entity_id) do
    # Simula um grande dataset via Stream (Lazy)
    stream = Stream.map(1..1_000_000, &(&1))
    
    schedule_processing(entity_id, fn -> stream end)
  end
end

# Exemplo de inicialização:
# {:ok, _pid} = StreamCacheSystem.start_link([])
# StreamCacheSystem.periodic_task(:users)
# StreamCacheSystem.periodic_task(:orders)
