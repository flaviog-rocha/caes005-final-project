# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de votação online, atendendo somente aos seguintes requisitos:
#
# 1. Processar grandes volumes de dados sem carregá-los integralmente na memória.
# 2. Recuperar processos após falhas inesperadas.
# 3. Detectar encerramentos inesperados de processos.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule Voting do
  @moduledoc """
  Sistema de votação com log append-only, índice de votantes em DETS e
  supervisão para reiniciar e monitorar o processo de votação.

  Inicie com `Voting.start_link(path: "votes.log")`. O índice DETS é
  reconstruído a partir do log durante a inicialização.
  """

  use Supervisor

  require Logger

  def start_link(opts \\ []) do
    supervisor_name = Keyword.get(opts, :supervisor_name)

    case supervisor_name do
      nil -> Supervisor.start_link(__MODULE__, opts)
      name -> Supervisor.start_link(__MODULE__, opts, name: name)
    end
  end

  def vote(server \\ Voting.Server, voter_id, choice) do
    GenServer.call(server, {:vote, voter_id, choice}, :infinity)
  end

  # Enum.reduce_while mantém o processamento incremental, sem materializar
  # a enumerable de votos na memória.
  def import_votes(server \\ Voting.Server, enumerable) do
    Enum.reduce_while(enumerable, {:ok, %{accepted: 0, duplicates: 0}}, fn
      {voter_id, choice}, {:ok, counts} ->
        case vote(server, voter_id, choice) do
          {:ok, :accepted} ->
            {:cont, {:ok, %{counts | accepted: counts.accepted + 1}}}

          {:error, :already_voted} ->
            {:cont, {:ok, %{counts | duplicates: counts.duplicates + 1}}}

          {:error, reason} ->
            {:halt, {:error, reason}}
        end

      _, _acc ->
        {:halt, {:error, :invalid_vote}}
    end)
  end

  def tally(server \\ Voting.Server) do
    GenServer.call(server, :tally, :infinity)
  end

  @impl true
  def init(opts) do
    path = Keyword.get(opts, :path, "votes.log")
    server_name = Keyword.get(opts, :server_name, Voting.Server)

    children = [
      {Voting.Server, path: path, name: server_name},
      {Voting.Watcher, server_name}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end

defmodule Voting.Server do
  use GenServer

  require Logger

  # O nome fixo da tabela DETS permite reconstruir o índice no reinício.
  # Esta implementação destina-se a uma instância de votação por nó.
  @table :voting_voters

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: Keyword.fetch!(opts, :name))
  end

  @impl true
  def init(opts) do
    path = Keyword.fetch!(opts, :path)
    :ok = File.mkdir_p(Path.dirname(path))

    {:ok, log_fd} = :file.open(String.to_charlist(path), [:append, :binary, :raw])

    dets_path = String.to_charlist(path <> ".voters.dets")
    {:ok, @table} = :dets.open_file(@table, file: dets_path, type: :set, repair: true)

    :ok = :dets.delete_all_objects(@table)
    tallies = replay_log(path)
    :ok = :dets.sync(@table)

    {:ok, %{log_fd: log_fd, tallies: tallies}}
  end

  @impl true
  def handle_call({:vote, voter_id, choice}, _from, state) do
    if valid_vote?(voter_id, choice) do
      case :dets.lookup(@table, voter_id) do
        [{^voter_id, _existing_choice}] ->
          {:reply, {:error, :already_voted}, state}

        [] ->
          persist_vote(voter_id, choice, state)
      end
    else
      {:reply, {:error, :invalid_vote}, state}
    end
  end

  def handle_call(:tally, _from, state) do
    {:reply, state.tallies, state}
  end

  @impl true
  def terminate(_reason, state) do
    _ = :file.close(state.log_fd)
    _ = :dets.close(@table)
    :ok
  end

  defp persist_vote(voter_id, choice, state) do
    record =
      {voter_id, choice}
      |> :erlang.term_to_binary([:deterministic])
      |> Base.encode64()
      |> Kernel.<>("\n")

    with :ok <- :file.write(state.log_fd, record),
         :ok <- :file.sync(state.log_fd),
         :ok <- :dets.insert(@table, {voter_id, choice}),
         :ok <- :dets.sync(@table) do
      tallies = Map.update(state.tallies, choice, 1, &(&1 + 1))
      {:reply, {:ok, :accepted}, %{state | tallies: tallies}}
    else
      {:error, reason} ->
        {:stop, {:persistence_failed, reason}, {:error, reason}, state}
    end
  end

  defp replay_log(path) do
    File.stream!(path, [], :line)
    |> Enum.reduce(%{}, fn line, tallies ->
      case decode_record(line) do
        {:ok, voter_id, choice} ->
          case :dets.lookup(@table, voter_id) do
            [] ->
              :ok = :dets.insert(@table, {voter_id, choice})
              Map.update(tallies, choice, 1, &(&1 + 1))

            [_already_seen] ->
              tallies
          end

        :error ->
          # Uma gravação interrompida pode deixar uma linha final incompleta.
          Logger.warning("Registro inválido ignorado durante a recuperação do log")
          tallies
      end
    end)
  end

  defp decode_record(line) do
    encoded =
      line
      |> String.trim_trailing("\n")
      |> String.trim_trailing("\r")

    with {:ok, binary} <- Base.decode64(encoded),
         {voter_id, choice} when is_binary(voter_id) and is_binary(choice) <-
           :erlang.binary_to_term(binary, [:safe]),
         true <- valid_vote?(voter_id, choice) do
      {:ok, voter_id, choice}
    else
      _ -> :error
    end
  rescue
    _ -> :error
  end

  defp valid_vote?(voter_id, choice) do
    is_binary(voter_id) and byte_size(voter_id) > 0 and
      is_binary(choice) and byte_size(choice) > 0
  end
end

defmodule Voting.Watcher do
  use GenServer

  require Logger

  @check_interval 100

  def start_link(server_name) do
    GenServer.start_link(__MODULE__, server_name)
  end

  @impl true
  def init(server_name) do
    send(self(), :watch)
    {:ok, %{server_name: server_name, pid: nil, monitor: nil}}
  end

  @impl true
  def handle_info(:watch, state) do
    case Process.whereis(state.server_name) do
      nil ->
        schedule_check()
        {:noreply, state}

      pid when pid == state.pid ->
        schedule_check()
        {:noreply, state}

      pid ->
        if state.monitor, do: Process.demonitor(state.monitor, [:flush])
        monitor = Process.monitor(pid)
        schedule_check()
        {:noreply, %{state | pid: pid, monitor: monitor}}
    end
  end

  def handle_info({:DOWN, monitor, :process, _pid, reason}, %{monitor: monitor} = state) do
    if reason in [:normal, :shutdown] do
      Logger.info("Processo de votação encerrado: #{inspect(reason)}")
    else
      Logger.warning("Encerramento inesperado do processo de votação: #{inspect(reason)}")
    end

    schedule_check()
    {:noreply, %{state | pid: nil, monitor: nil}}
  end

  def handle_info(_message, state), do: {:noreply, state}

  defp schedule_check do
    Process.send_after(self(), :watch, @check_interval)
  end
end
