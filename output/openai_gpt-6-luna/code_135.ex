# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de votação online, atendendo somente aos seguintes requisitos:
#
# 1. Processar grandes volumes de dados sem carregá-los integralmente na memória.
# 2. Recuperar processos após falhas inesperadas.
# 3. Detectar encerramentos inesperados de processos.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule OnlineVoting do
  @moduledoc """
  Sistema de votação com persistência em disco, importação por streaming e
  reinício supervisionado dos processos.
  """

  use Supervisor

  @engine OnlineVoting.Engine
  @store OnlineVoting.Store
  @watcher OnlineVoting.Watcher

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    data_file = Keyword.get(opts, :data_file, "data/votes.dets")

    children = [
      {@store, data_file: data_file},
      @engine,
      @watcher
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  @doc "Registra um voto. Cada eleitor pode votar apenas uma vez."
  def cast_vote(voter_id, candidate) do
    with :ok <- validate_value(voter_id, :voter_id),
         :ok <- validate_value(candidate, :candidate) do
      safe_call(fn -> GenServer.call(@engine, {:cast_vote, voter_id, candidate}) end)
    end
  end

  @doc "Retorna a contagem de votos por candidato."
  def tally do
    safe_call(fn -> GenServer.call(@engine, :tally, :infinity) end)
  end

  @doc """
  Importa linhas no formato `identificador_do_eleitor,candidato` sem carregar
  o arquivo inteiro na memória. Votos repetidos são ignorados.
  """
  def import_file(path, opts \\ []) do
    batch_size = Keyword.get(opts, :batch_size, 500)

    if not (is_integer(batch_size) and batch_size > 0) do
      {:error, :invalid_batch_size}
    else
      try do
        path
        |> File.stream!([], :line)
        |> Stream.map(&parse_line/1)
        |> Stream.chunk_every(batch_size)
        |> Enum.reduce_while({:ok, %{inserted: 0, duplicates: 0}}, fn batch, acc ->
          with {:ok, votes} <- validate_batch(batch),
               {:ok, result} <- safe_call(fn -> GenServer.call(@store, {:insert_batch, votes}, :infinity) end) do
            {:ok, totals} = acc

            {:cont,
             {:ok,
              %{
                inserted: totals.inserted + result.inserted,
                duplicates: totals.duplicates + result.duplicates
              }}}
          else
            {:error, reason} -> {:halt, {:error, reason}}
          end
        end)
      rescue
        error in File.Error -> {:error, error}
      end
    end
  end

  defp validate_value(value, _field) when is_binary(value) do
    if String.trim(value) == "", do: {:error, :empty_value}, else: :ok
  end

  defp validate_value(_value, field), do: {:error, {:invalid_value, field}}

  defp parse_line(line) do
    case String.split(String.trim(line), ",", parts: 2) do
      ["" ] ->
        :skip

      [voter_id, candidate] ->
        voter_id = String.trim(voter_id)
        candidate = String.trim(candidate)

        if voter_id == "" or candidate == "" do
          {:error, :invalid_row}
        else
          {:ok, {voter_id, candidate}}
        end

      _ ->
        {:error, :invalid_row}
    end
  end

  defp validate_batch(batch) do
    Enum.reduce_while(batch, {:ok, []}, fn
      :skip, {:ok, votes} ->
        {:cont, {:ok, votes}}

      {:ok, vote}, {:ok, votes} ->
        {:cont, {:ok, [vote | votes]}}

      {:error, reason}, _acc ->
        {:halt, {:error, reason}}
    end)
    |> case do
      {:ok, votes} -> {:ok, Enum.reverse(votes)}
      error -> error
    end
  end

  defp safe_call(fun) do
    fun.()
  catch
    :exit, reason -> {:error, {:process_unavailable, reason}}
  end
end

defmodule OnlineVoting.Store do
  use GenServer

  @table :online_voting_votes

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def init(opts) do
    path = Keyword.fetch!(opts, :data_file)
    File.mkdir_p!(Path.dirname(path))

    case :dets.open_file(@table, file: String.to_charlist(path), type: :set, repair: true) do
      {:ok, @table} -> {:ok, @table}
      {:error, reason} -> {:stop, {:dets_open_failed, reason}}
    end
  end

  def handle_call({:insert_batch, votes}, _from, table) do
    result =
      Enum.reduce(votes, %{inserted: 0, duplicates: 0}, fn {voter_id, candidate}, totals ->
        case :dets.insert_new(table, {voter_id, candidate}) do
          true -> %{totals | inserted: totals.inserted + 1}
          false -> %{totals | duplicates: totals.duplicates + 1}
        end
      end)

    case :dets.sync(table) do
      :ok -> {:reply, {:ok, result}, table}
      {:error, reason} -> {:stop, {:dets_sync_failed, reason}, {:error, reason}, table}
    end
  end

  def handle_call(:tally, _from, table) do
    totals =
      :dets.foldl(
        fn {_voter_id, candidate}, acc ->
          Map.update(acc, candidate, 1, &(&1 + 1))
        end,
        %{},
        table
      )

    {:reply, {:ok, totals}, table}
  end

  def terminate(_reason, table) do
    :dets.close(table)
  end
end

defmodule OnlineVoting.Engine do
  use GenServer

  def start_link(_opts \\ []) do
    GenServer.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  def init(:ok), do: {:ok, %{}}

  def handle_call({:cast_vote, voter_id, candidate}, _from, state) do
    reply =
      try do
        case GenServer.call(OnlineVoting.Store, {:insert_batch, [{voter_id, candidate}]}) do
          {:ok, %{inserted: 1}} -> :ok
          {:ok, %{duplicates: 1}} -> {:error, :already_voted}
          other -> other
        end
      catch
        :exit, reason -> {:error, {:store_unavailable, reason}}
      end

    {:reply, reply, state}
  end

  def handle_call(:tally, _from, state) do
    reply =
      try do
        GenServer.call(OnlineVoting.Store, :tally, :infinity)
      catch
        :exit, reason -> {:error, {:store_unavailable, reason}}
      end

    {:reply, reply, state}
  end
end

defmodule OnlineVoting.Watcher do
  use GenServer
  require Logger

  @engine OnlineVoting.Engine

  def start_link(_opts \\ []) do
    GenServer.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  def init(:ok) do
    {:ok, monitor_engine(%{ref: nil})}
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, %{ref: ref} = state) do
    unless reason == :normal or reason == :shutdown or match?({:shutdown, _}, reason) do
      Logger.warning("Processo de votação #{@engine} encerrado inesperadamente: #{inspect(reason)}")
    end

    Process.send_after(self(), :remonitor, 50)
    {:noreply, %{state | ref: nil}}
  end

  def handle_info(:remonitor, state) do
    {:noreply, monitor_engine(state)}
  end

  def handle_info(_message, state), do: {:noreply, state}

  defp monitor_engine(state) do
    case Process.whereis(@engine) do
      nil ->
        Process.send_after(self(), :remonitor, 50)
        state

      pid ->
        %{state | ref: Process.monitor(pid)}
    end
  end
end
