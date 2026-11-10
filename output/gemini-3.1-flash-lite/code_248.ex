# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de um sistema de cache, atendendo somente aos seguintes requisitos:
#
# 1. Processar grandes volumes de dados sem carregá-los integralmente na memória.
# 2. Executar determinadas operações periodicamente.
# 3. Permitir que diferentes entidades sejam processadas independentemente.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# Para atender aos requisitos de processamento de grandes volumes (streaming),
# execução periódica (GenServer com timers) e isolamento de entidades (processos via Registry),
# utilizamos Streams para processamento lazy e GenServer para isolamento.

defmodule CacheManager do
  use GenServer

  @doc """
  Inicia o sistema de cache para uma entidade específica.
  O uso do Registry garante o isolamento entre entidades.
  """
  def start_link(entity_id) do
    GenServer.start_link(__MODULE__, entity_id, name: via_tuple(entity_id))
  end

  def init(entity_id) do
    # Agendamento de tarefa periódica (ex: limpeza ou persistência a cada 60s)
    schedule_cleanup()
    {:ok, %{entity_id: entity_id, data: %{}}}
  end

  @doc """
  Processa um grande volume de dados de forma lazy.
  O Stream permite processar itens um a um sem carregar tudo na memória.
  """
  def process_large_dataset(entity_id, enumerable) do
    enumerable
    |> Stream.map(fn item -> transform_data(item) end)
    |> Stream.each(fn processed -> 
      GenServer.cast(via_tuple(entity_id), {:store, processed}) 
    end)
    |> Stream.run()
  end

  def handle_cast({:store, {key, value}}, state) do
    new_data = Map.put(state.data, key, value)
    {:noreply, %{state | data: new_data}}
  end

  def handle_info(:cleanup, state) do
    # Lógica de processamento periódico
    IO.puts("Executando tarefa periódica para: #{state.entity_id}")
    schedule_cleanup()
    {:noreply, state}
  end

  defp transform_data(item), do: {item.id, item.value}

  defp schedule_cleanup, do: Process.send_after(self(), :cleanup, 60_000)

  defp via_tuple(entity_id), do: {:via, Registry, {CacheRegistry, entity_id}}
end

# Exemplo de configuração do sistema
defmodule SupervisionTree do
  def start do
    # Registro para identificar processos pelo entity_id
    Registry.start_link(keys: :unique, name: CacheRegistry)
    
    # Exemplo de uso:
    # CacheManager.start_link("entidade_1")
    # data_stream = Stream.map(1..1_000_000, fn i -> %{id: i, value: "val_#{i}"} end)
    # CacheManager.process_large_dataset("entidade_1", data_stream)
  end
end
