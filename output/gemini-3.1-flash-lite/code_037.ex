# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o monitoramento de sensores, atendendo somente aos seguintes requisitos:
#
# 1. Impedir que uma mesma operação seja executada mais de uma vez.
# 2. Processar grandes volumes de dados sem carregá-los integralmente na memória.
# 3. Manter um histórico das operações realizadas.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - GenServer
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule SensorMonitor do
  @moduledoc """
  Monitor de sensores que processa fluxos de dados de forma idempotente,
  eficiente em memória e com persistência de histórico em estado.
  """
  use GenServer

  # API Pública

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, %{processed: MapSet.new(), history: []}, opts)
  end

  @doc """
  Processa um fluxo (Stream) de leituras. 
  Exemplo: SensorMonitor.process_stream(pid, Stream.map(1..1_000_000, &"id_#{&1}"))
  """
  def process_stream(pid, stream) do
    stream
    |> Stream.chunk_every(100) # Processamento em blocos para controle de memória
    |> Enum.each(fn batch ->
      GenServer.call(pid, {:process_batch, batch})
    end)
  end

  # Callbacks do GenServer

  @impl true
  def init(state), do: {:ok, state}

  @impl true
  def handle_call({:process_batch, batch}, _from, state) do
    # Filtra idempotência: apenas IDs que não foram processados anteriormente
    new_items = Enum.reject(batch, &MapSet.member?(state.processed, &1))

    if Enum.empty?(new_items) do
      {:reply, :ok, state}
    else
      # Atualiza estado: Mantém histórico e conjunto de IDs processados
      new_processed = Enum.reduce(new_items, state.processed, &MapSet.put(&2, &1))
      new_history = Enum.take(new_items ++ state.history, 1000) # Limita histórico para evitar estouro de memória

      {:reply, :ok, %{processed: new_processed, history: new_history}}
    end
  end

  @doc "Retorna o histórico das operações recentes."
  def get_history(pid), do: GenServer.call(pid, :get_history)

  @impl true
  def handle_call(:get_history, _from, state) do
    {:reply, state.history, state}
  end
end
