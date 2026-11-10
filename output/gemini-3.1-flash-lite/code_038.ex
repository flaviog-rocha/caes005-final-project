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
  Monitor de sensores que garante idempotência, processamento em streaming
  e persistência de histórico em memória (GenServer).
  """
  use GenServer

  # API Pública

  def start_link(_opts) do
    GenServer.start_link(__MODULE__, %{processed: MapSet.new(), history: []}, name: __MODULE__)
  end

  @doc """
  Processa um stream de dados para evitar estouro de memória.
  Impedir duplicidade através de um identificador único (id).
  """
  def process_stream(sensor_stream) do
    sensor_stream
    |> Stream.each(fn {id, data} ->
      GenServer.call(__MODULE__, {:process, id, data})
    end)
    |> Stream.run()
  end

  def get_history do
    GenServer.call(__MODULE__, :get_history)
  end

  # Callbacks do GenServer

  @impl true
  def init(state), do: {:ok, state}

  @impl true
  def handle_call({:process, id, data}, _from, %{processed: processed, history: history} = state) do
    if MapSet.member?(processed, id) do
      {:reply, {:error, :already_processed}, state}
    else
      new_state = %{
        processed: MapSet.put(processed, id),
        history: [{id, data, System.system_time(:second)} | history]
      }
      {:reply, :ok, new_state}
    end
  end

  @impl true
  def handle_call(:get_history, _from, state) do
    {:reply, state.history, state}
  end
end

# Exemplo de uso:
# {:ok, _pid} = SensorMonitor.start_link([])
# data_stream = Stream.map(1..1000, fn i -> {i, "reading_#{i}"} end)
# SensorMonitor.process_stream(data_stream)
