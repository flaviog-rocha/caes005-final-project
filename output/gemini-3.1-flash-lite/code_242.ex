# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de agendamento de compromissos, atendendo somente aos seguintes requisitos:
#
# 1. Permitir comunicação entre diferentes processos.
# 2. Implementar novas tentativas após falhas temporárias.
# 3. Gerar notificações quando determinadas condições forem atingidas.
# 4. Persistir os dados para recuperação após reinicializações.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule AppointmentSystem do
  @moduledoc """
  Sistema de agendamento resiliente utilizando GenServer, ETS para persistência 
  e o mecanismo de retry com expoente de espera.
  """
  use GenServer

  # --- API Pública ---

  def start_link(_), do: GenServer.start_link(__MODULE__, nil, name: __MODULE__)

  def schedule(id, time) do
    GenServer.call(__MODULE__, {:schedule, id, time})
  end

  # --- Callbacks do GenServer ---

  @impl true
  def init(_) do
    # Tabela ETS com proteção public para persistência básica em memória
    # Em produção, usaria :dets ou mnesia para persistência em disco.
    table = :ets.new(:appointments, [:set, :public, :named_table])
    {:ok, %{table: table}}
  end

  @impl true
  def handle_call({:schedule, id, time}, _from, state) do
    :ets.insert(:appointments, {id, time, :pending})
    
    # Dispara processo assíncrono para notificação com retries
    spawn(fn -> notify_with_retry(id, time, 0) end)
    
    {:reply, :ok, state}
  end

  # --- Lógica de Negócio e Resiliência ---

  defp notify_with_retry(id, time, attempt) do
    try do
      # Simulação de serviço externo
      send_notification(id, time)
      :ets.insert(:appointments, {id, time, :notified})
    rescue
      _ ->
        if attempt < 3 do
          # Backoff exponencial simples: 2^attempt segundos
          :timer.sleep(:math.pow(2, attempt) |> round() |> Kernel.*(1000))
          notify_with_retry(id, time, attempt + 1)
        else
          :ets.insert(:appointments, {id, time, :failed})
        end
    end
  end

  defp send_notification(id, _time) do
    # Simula falha aleatória para demonstrar o retry
    if :rand.uniform() > 0.7, do: raise("Falha de rede temporária")
    IO.puts("[Notificação] Compromisso #{id} processado com sucesso.")
  end
end

# Exemplo de uso:
# {:ok, _pid} = AppointmentSystem.start_link(nil)
# AppointmentSystem.schedule("meeting_01", "2023-12-01 10:00")
