# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o processamento de arquivos CSV, atendendo somente aos seguintes requisitos:
#
# 1. Executar determinadas operações periodicamente.
# 2. Detectar encerramentos inesperados de processos.
# 3. Permitir comunicação entre diferentes processos.
# 4. Diferenciar erros de validação de falhas operacionais.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule CsvProcessor do
  @moduledoc """
  Processador de CSV resiliente utilizando OTP e monitoramento de processos.
  """
  use GenServer
  require Logger

  # --- Tipos e Erros Customizados ---

  defmodule ValidationError, do: defexception [:message]
  defmodule OperationalFailure, do: defexception [:message]

  # --- API Pública ---

  def start_link(_opts) do
    GenServer.start_link(__MODULE__, %{files: ["data.csv"]}, name: __MODULE__)
  end

  def trigger_processing, do: GenServer.cast(__MODULE__, :process_files)

  # --- Callbacks do GenServer ---

  @impl true
  def init(state) do
    # 1. Executar periodicamente: Agenda o primeiro processamento
    schedule_work()
    {:ok, state}
  end

  @impl true
  def handle_info(:work, state) do
    trigger_processing()
    schedule_work()
    {:noreply, state}
  end

  @impl true
  def handle_cast(:process_files, state) do
    # 3. Comunicação entre processos: Spawn de worker para processar sem bloquear o loop
    Enum.each(state.files, fn file ->
      pid = spawn_link(fn -> process_csv(file) end)
      # 2. Detectar encerramentos inesperados: Monitoramento
      Process.monitor(pid)
    end)
    {:noreply, state}
  end

  @impl true
  def handle_info({:DOWN, _ref, :process, pid, reason}, state) do
    case reason do
      :normal -> Logger.info("Processo #{inspect(pid)} finalizou com sucesso.")
      _ -> Logger.error("Processo #{inspect(pid)} falhou inesperadamente: #{inspect(reason)}")
    end
    {:noreply, state}
  end

  # --- Lógica de Negócio ---

  defp process_csv(path) do
    # Simulação de leitura de arquivo
    try do
      # 4. Diferenciar erros de validação vs falhas operacionais
      data = File.read!(path)
      
      if String.contains?(data, "INVALID"), 
        do: raise(ValidationError, message: "Linha com formato inválido detectado")
      
      Logger.info("Arquivo #{path} processado.")
    rescue
      e in [File.Error] -> raise OperationalFailure, message: "Erro de IO: #{e.message}"
      e in [ValidationError] -> Logger.warn("Erro de validação: #{e.message}")
    end
  end

  defp schedule_work, do: Process.send_after(self(), :work, :timer.minutes(5))
end
