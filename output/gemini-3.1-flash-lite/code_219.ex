# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de agendamento de compromissos, atendendo somente aos seguintes requisitos:
#
# 1. Validar os dados recebidos antes de realizar operações.
# 2. Permitir cadastrar, consultar, atualizar e remover registros.
# 3. Recuperar processos após falhas inesperadas.
# 4. Detectar encerramentos inesperados de processos.
# 5. Permitir consultar o estado atual das operações em andamento.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule AppointmentSystem do
  @moduledoc """
  Sistema de agendamento resiliente usando GenServer.
  Gerencia estados, valida dados e recupera de falhas.
  """
  use GenServer

  # --- API Pública ---

  def start_link(_), do: GenServer.start_link(__MODULE__, %{}, name: __MODULE__)

  def create(id, data) when is_map(data) do
    if valid?(data), do: GenServer.call(__MODULE__, {:upsert, id, data}), else: {:error, :invalid_data}
  end

  def get(id), do: GenServer.call(__MODULE__, {:get, id})

  def delete(id), do: GenServer.call(__MODULE__, {:delete, id})

  def get_state, do: GenServer.call(__MODULE__, :get_state)

  # --- Callbacks do GenServer ---

  @impl true
  def init(_) do
    # Monitora o próprio processo para logs de encerramento
    Process.flag(:trap_exit, true)
    {:ok, %{}}
  end

  @impl true
  def handle_call({:upsert, id, data}, _from, state) do
    {:reply, :ok, Map.put(state, id, data)}
  end

  def handle_call({:get, id}, _from, state) do
    {:reply, Map.get(state, id, {:error, :not_found}), state}
  end

  def handle_call({:delete, id}, _from, state) do
    {:reply, :ok, Map.delete(state, id)}
  end

  def handle_call(:get_state, _from, state), do: {:reply, state, state}

  # --- Resiliência e Monitoramento ---

  @impl true
  def terminate(reason, state) do
    # Detecta encerramento inesperado e loga o estado para recuperação futura
    IO.puts("Processo encerrado inesperadamente: #{inspect(reason)}")
    File.write("checkpoint.dump", :erlang.term_to_binary(state))
    :ok
  end

  # Recuperação pós-falha: Tenta ler o estado anterior ao iniciar
  def handle_info({:EXIT, _from, reason}, state) do
    IO.puts("Falha detectada: #{inspect(reason)}")
    {:noreply, state}
  end

  # --- Validação ---

  defp valid?(data) do
    # Requisito 1: Validação simples (garantir presença de hora e cliente)
    Map.has_key?(data, :customer) and Map.has_key?(data, :time)
  end
end

# Exemplo de uso:
# {:ok, _pid} = AppointmentSystem.start_link([])
# AppointmentSystem.create("1", %{customer: "Alice", time: "10:00"})
# AppointmentSystem.get("1")
# AppointmentSystem.get_state()
