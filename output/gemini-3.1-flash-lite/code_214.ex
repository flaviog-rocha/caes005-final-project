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
  Processador de CSV resiliente utilizando OTP.
  - Periodicidade: GenServer com :timer.send_interval.
  - Monitoramento: Processo supervisor supervisionando o worker.
  - Comunicação: Registro de processos via Registry.
  - Erros: Distinção entre Validation (domínio) e Operational (falha de sistema).
  """

  use Supervisor
  use GenServer
  require Logger

  # --- Definição de Erros ---
  defmodule ValidationError, do: defexception [:message]
  defmodule OperationalError, do: defexception [:message]

  # --- Supervisor ---
  def start_link(_), do: Supervisor.start_link(__MODULE__, :ok, name: __MODULE__)

  @impl true
  def init(:ok) do
    # Registry para permitir comunicação nomeada entre processos
    children = [
      {Registry, keys: :unique, name: ProcessorRegistry},
      {Worker, name: {:via, Registry, {ProcessorRegistry, :csv_worker}}}
    ]
    Supervisor.init(children, strategy: :one_for_one)
  end

  # --- Worker (GenServer) ---
  defmodule Worker do
    use GenServer

    def start_link(opts), do: GenServer.start_link(__MODULE__, :ok, opts)

    @impl true
    def init(:ok) do
      # Requisito 1: Execução periódica (a cada 10 segundos)
      :timer.send_interval(10_000, :process_files)
      {:ok, %{files_processed: 0}}
    end

    @impl true
    def handle_info(:process_files, state) do
      # Simulação de processamento
      try do
        perform_csv_logic()
        {:noreply, %{state | files_processed: state.files_processed + 1}}
      rescue
        e in [ValidationError, OperationalError] ->
          Logger.error("Falha ao processar: #{inspect(e)}")
          {:noreply, state}
      end
    end

    defp perform_csv_logic do
      # Exemplo de diferenciação de erros
      case :rand.uniform(10) do
        1 -> raise ValidationError, message: "Linha inválida encontrada"
        2 -> raise OperationalError, message: "Sistema de arquivos indisponível"
        _ -> Logger.info("Processamento concluído com sucesso")
      end
    end

    # Requisito 3: Comunicação entre processos
    def handle_call(:get_status, _from, state) do
      {:reply, state, state}
    end
  end
end

# --- Início do sistema ---
# O código abaixo é apenas para fins de execução do exemplo
# Caso rode em um script, pode-se usar:
# CsvProcessor.start_link([])
# Process.sleep(:infinity)
