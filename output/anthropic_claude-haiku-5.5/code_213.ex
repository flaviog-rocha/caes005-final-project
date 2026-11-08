# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o processamento de arquivos CSV, atendendo somente aos seguintes requisitos:
#
# 1. Executar determinadas operações periodicamente.
# 2. Detectar encerramentos inesperados de processos.
# 3. Permitir comunicação entre diferentes processos.
# 4. Diferenciar erros de validação de falhas operacionais.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

#!/usr/bin/env elixir
# Processador de CSV com:
#   1. Execução periódica (agendamento via Process.send_after/3)
#   2. Detecção de encerramentos inesperados (Process.monitor/2 e supervisão)
#   3. Comunicação entre processos (mensagens e assinaturas)
#   4. Separação entre erros de validação e falhas operacionais
# Execução: elixir csv_processor.exs

defmodule CSVProcessor.Parser do
  @moduledoc """
  Funções puras de leitura e validação de arquivos CSV.

  Retornos:
    * `{:ok, rows}` quando o arquivo é válido
    * `{:error, {:validation, detalhe}}` quando o conteúdo do dado é inválido
    * `{:error, {:operational, detalhe}}` quando há falha de I/O ou de ambiente
  """

  @header ["id", "name", "amount"]

  @spec parse_file(Path.t()) :: {:ok, [map()]} | {:error, tuple()}
  def parse_file(path) do
    case File.read(path) do
      {:ok, content} ->
        parse_content(content)

      # Falha operacional: problema de I/O, não é culpa do dado
      {:error, reason} ->
        {:error, {:operational, {:read_failed, path, reason}}}
    end
  end

  @spec parse_content(String.t()) :: {:ok, [map()]} | {:error, tuple()}
  def parse_content(content) do
    lines =
      content
      |> String.split("\n", trim: true)
      |> Enum.map(&String.trim_trailing(&1, "\r"))
      |> Enum.reject(&(&1 == ""))

    case lines do
      [] ->
        {:error, {:validation, :empty_file}}

      [header | rows] ->
        case split_fields(header) do
          @header -> parse_rows(rows)
          other -> {:error, {:validation, {:invalid_header, other}}}
        end
    end
  end

  defp parse_rows(rows) do
    rows
    |> Enum.with_index(2)
    |> Enum.reduce_while({:ok, []}, fn {line, n}, {:ok, acc} ->
      case parse_row(line, n) do
        {:ok, row} -> {:cont, {:ok, [row | acc]}}
        {:error, _} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, acc} -> {:ok, Enum.reverse(acc)}
      error -> error
    end
  end

  defp parse_row(line, n) do
    case split_fields(line) do
      [id, name, amount] ->
        with {:ok, id} <- parse_id(id, n),
             {:ok, name} <- parse_name(name, n),
             {:ok, amount} <- parse_amount(amount, n) do
          {:ok, %{id: id, name: name, amount: amount}}
        end

      fields ->
        {:error, {:validation, {:line, n, {:wrong_field_count, length(fields)}}}}
    end
  end

  defp parse_id(raw, n) do
    case Integer.parse(raw) do
      {id, ""} when id > 0 -> {:ok, id}
      _ -> {:error, {:validation, {:line, n, {:invalid_id, raw}}}}
    end
  end

  defp parse_name(raw, n) do
    case String.trim(raw) do
      "" -> {:error, {:validation, {:line, n, :empty_name}}}
      name -> {:ok, name}
    end
  end

  defp parse_amount(raw, n) do
    case Float.parse(raw) do
      {amount, ""} -> {:ok, amount}
      _ -> {:error, {:validation, {:line, n, {:invalid_amount, raw}}}}
    end
  end

  defp split_fields(line) do
    line
    |> String.split(",")
    |> Enum.map(&String.trim/1)
  end
end

defmodule CSVProcessor.Worker do
  @moduledoc """
  Varre periodicamente um diretório, processa cada CSV em uma Task
  supervisionada e distribui os eventos para os processos assinantes.
  """

  use GenServer
  require Logger

  @name __MODULE__
  @task_sup CSVProcessor.TaskSupervisor

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: @name)
  end

  @doc "Inscreve o processo chamador para receber mensagens `{:csv_event, evento}`."
  def subscribe(server \\ @name) do
    GenServer.call(server, :subscribe)
  end

  @impl true
  def init(opts) do
    state = %{
      dir: Keyword.fetch!(opts, :dir),
      interval: Keyword.get(opts, :interval, 5_000),
      # ref do monitor => pid do assinante
      subscribers: %{},
      # ref da Task => caminho do arquivo em processamento
      running: %{},
      # arquivos concluídos com sucesso ou com erro de validação
      done: MapSet.new()
    }

    # Primeira varredura imediata; as seguintes são agendadas em handle_info/2
    send(self(), :scan)
    {:ok, state}
  end

  @impl true
  def handle_call(:subscribe, {pid, _tag}, state) do
    ref = Process.monitor(pid)
    {:reply, :ok, %{state | subscribers: Map.put(state.subscribers, ref, pid)}}
  end

  @impl true
  def handle_info(:scan, state) do
    schedule_scan(state.interval)

    in_flight = Map.values(state.running)

    pending =
      state.dir
      |> Path.join("*.csv")
      |> Path.wildcard()
      |> Enum.reject(&(MapSet.member?(state.done, &1) or Enum.member?(in_flight, &1)))

    {:noreply, Enum.reduce(pending, state, &start_task/2)}
  end

  # Resultado normal de uma Task
  def handle_info({ref, result}, state) when is_reference(ref) do
    case Map.pop(state.running, ref) do
      {nil, _} ->
        {:noreply, state}

      {path, running} ->
        # A Task terminou normalmente; removemos o monitor e a mensagem :DOWN pendente
        Process.demonitor(ref, [:flush])

        event = classify(path, result)
        broadcast(state.subscribers, event)

        done =
          case event do
            # Falhas operacionais não são marcadas como concluídas: serão retentadas
            {:operational_error, _, _} -> state.done
            _ -> MapSet.put(state.done, path)
          end

        {:noreply, %{state | running: running, done: done}}
    end
  end

  # Encerramento de assinante ou de Task
  def handle_info({:DOWN, ref, :process, _pid, reason}, state) do
    cond do
      Map.has_key?(state.subscribers, ref) ->
        {:noreply, %{state | subscribers: Map.delete(state.subscribers, ref)}}

      Map.has_key?(state.running, ref) ->
        {path, running} = Map.pop(state.running, ref)

        Logger.error("Task de #{Path.basename(path)} encerrou inesperadamente: #{inspect(reason)}")
        broadcast(state.subscribers, {:operational_failure, path, reason})

        {:noreply, %{state | running: running}}

      true ->
        {:noreply, state}
    end
  end

  def handle_info(_msg, state), do: {:noreply, state}

  defp start_task(path, state) do
    task = Task.Supervisor.async_nolink(@task_sup, fn -> run_job(path) end)
    %{state | running: Map.put(state.running, task.ref, path)}
  end

  # Converte exceções inesperadas em falha operacional
  defp run_job(path) do
    try do
      CSVProcessor.Parser.parse_file(path)
    rescue
      e -> {:error, {:operational, {:exception, Exception.message(e)}}}
    end
  end

  defp classify(path, {:ok, rows}), do: {:processed, path, rows}
  defp classify(path, {:error, {:validation, _} = reason}), do: {:validation_error, path, reason}
  defp classify(path, {:error, {:operational, _} = reason}), do: {:operational_error, path, reason}

  defp broadcast(subscribers, event) do
    Enum.each(Map.values(subscribers), &send(&1, {:csv_event, event}))
  end

  defp schedule_scan(interval) do
    Process.send_after(self(), :scan, interval)
  end
end

defmodule CSVProcessor.Listener do
  @moduledoc """
  Assina o Worker, monitora seu processo e reconecta automaticamente
  caso ele seja reiniciado pelo supervisor após uma falha.
  """

  use GenServer
  require Logger

  @name __MODULE__
  @retry_ms 500

  def start_link(_opts) do
    GenServer.start_link(__MODULE__, nil, name: @name)
  end

  @impl true
  def init(_) do
    send(self(), :connect)
    {:ok, %{worker_ref: nil}}
  end

  @impl true
  def handle_info(:connect, state) do
    case Process.whereis(CSVProcessor.Worker) do
      nil ->
        Process.send_after(self(), :connect, @retry_ms)
        {:noreply, state}

      pid ->
        ref = Process.monitor(pid)

        try do
          :ok = CSVProcessor.Worker.subscribe(pid)
          Logger.info("Conectado ao worker #{inspect(pid)}")
          {:noreply, %{state | worker_ref: ref}}
        catch
          :exit, reason ->
            # O worker morreu entre a localização e a assinatura
            Process.demonitor(ref, [:flush])
            Logger.warning("Falha ao assinar worker: #{inspect(reason)}")
            Process.send_after(self(), :connect, @retry_ms)
            {:noreply, state}
        end
    end
  end

  # Encerramento inesperado do worker detectado via monitor
  def handle_info({:DOWN, ref, :process, _pid, reason}, %{worker_ref: ref} = state) do
    Logger.error("Worker encerrou inesperadamente: #{inspect(reason)}. Reconectando...")
    send(self(), :connect)
    {:noreply, %{state | worker_ref: nil}}
  end

  def handle_info({:DOWN, _ref, :process, _pid, _reason}, state) do
    {:noreply, state}
  end

  def handle_info({:csv_event, event}, state) do
    log(event)
    {:noreply, state}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  defp log({:processed, path, rows}) do
    Logger.info("OK #{Path.basename(path)}: #{length(rows)} registro(s) processado(s)")
  end

  defp log({:validation_error, path, reason}) do
    Logger.warning("Erro de VALIDAÇÃO em #{Path.basename(path)}: #{inspect(reason)}")
  end

  defp log({:operational_error, path, reason}) do
    Logger.error("Falha OPERACIONAL em #{Path.basename(path)}: #{inspect(reason)}")
  end

  defp log({:operational_failure, path, reason}) do
    Logger.error("Processamento de #{Path.basename(path)} abortado: #{inspect(reason)}")
  end
end

defmodule CSVProcessor.Supervisor do
  @moduledoc """
  Supervisor raiz. Reinicia o Worker em caso de falha e mantém
  o Listener ativo para observar esses eventos.
  """

  use Supervisor

  def start_link(opts) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    children = [
      {Task.Supervisor, name: CSVProcessor.TaskSupervisor},
      {CSVProcessor.Worker, opts},
      CSVProcessor.Listener
    ]

    Supervisor.init(children, strategy: :one_for_one, max_restarts: 5, max_seconds: 10)
  end
end

# ---------------------------------------------------------------------------
# Demonstração
# ---------------------------------------------------------------------------

dir = Path.join(System.tmp_dir!(), "csv_demo_#{System.unique_integer([:positive])}")
File.mkdir_p!(dir)

# Arquivo válido
File.write!(Path.join(dir, "ok.csv"), "id,name,amount\n1,Ana,10.5\n2,Bruno,20\n")

# Arquivo com erros de validação (nome vazio, id negativo e valor inválido)
File.write!(Path.join(dir, "invalido.csv"), "id,name,amount\n1,,10\n-3,Carla,abc\n")

{:ok, _sup} = CSVProcessor.Supervisor.start_link(dir: dir, interval: 2_000)

# Aguarda algumas varreduras
Process.sleep(5_000)

# Simula encerramento inesperado do worker para demonstrar detecção e recuperação
case Process.whereis(CSVProcessor.Worker) do
  nil -> :ok
  pid -> Process.exit(pid, :kill)
end

# Arquivo novo após a recuperação: deve ser processado pelo worker reiniciado
File.write!(Path.join(dir, "novo.csv"), "id,name,amount\n3,Diego,99.9\n")

Process.sleep(5_000)
