# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de votação online, atendendo somente aos seguintes requisitos:
#
# 1. Recuperar processos após falhas inesperadas.
# 2. Persistir os dados para recuperação após reinicializações.
# 3. Permitir que diferentes entidades sejam processadas independentemente.
# 4. Permitir a execução de operações simultâneas.
# 5. Diferenciar erros de validação de falhas operacionais.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule OnlineVoting do
  @moduledoc """
  Sistema de votação com processos supervisionados e persistência em disco.

  Inicie com `OnlineVoting.start_link(data_dir: "voting_data")`.
  """

  use Supervisor

  @registry OnlineVoting.Registry
  @dynamic_supervisor OnlineVoting.PollSupervisor
  @manager OnlineVoting.Manager

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    data_dir = Keyword.get(opts, :data_dir, "voting_data")

    children = [
      {Registry, keys: :unique, name: @registry},
      {DynamicSupervisor, strategy: :one_for_one, name: @dynamic_supervisor},
      {OnlineVoting.Manager, data_dir: data_dir}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  def create_poll(question, options) do
    case validate_poll(question, options) do
      :ok ->
        manager_call({:create_poll, question, options})

      {:error, reason} ->
        {:error, {:validation, reason}}
    end
  end

  def vote(id, option) when is_binary(id) and is_binary(option) do
    case find_poll(id) do
      {:ok, pid} -> poll_call(pid, {:vote, option})
      error -> error
    end
  end

  def vote(_id, _option), do: {:error, {:validation, :invalid_arguments}}

  def results(id) when is_binary(id) do
    case find_poll(id) do
      {:ok, pid} -> poll_call(pid, :results)
      error -> error
    end
  end

  def results(_id), do: {:error, {:validation, :invalid_id}}

  defp validate_poll(question, options) do
    cond do
      not is_binary(question) or String.trim(question) == "" ->
        {:error, :invalid_question}

      not is_list(options) or length(options) < 2 ->
        {:error, :at_least_two_options_required}

      not Enum.all?(options, &(is_binary(&1) and String.trim(&1) != "")) ->
        {:error, :invalid_option}

      length(Enum.uniq(options)) != length(options) ->
        {:error, :duplicate_options}

      true ->
        :ok
    end
  end

  defp find_poll(id) do
    if String.trim(id) == "" do
      {:error, {:validation, :invalid_id}}
    else
      try do
        case Registry.lookup(@registry, {:poll, id}) do
          [{pid, _value}] -> {:ok, pid}
          [] -> {:error, {:validation, :unknown_poll}}
        end
      catch
        :exit, reason -> {:error, {:operational, reason}}
      end
    end
  end

  defp poll_call(pid, message) do
    try do
      GenServer.call(pid, message)
    catch
      :exit, reason -> {:error, {:operational, reason}}
    end
  end

  defp manager_call(message) do
    try do
      GenServer.call(@manager, message)
    catch
      :exit, reason -> {:error, {:operational, reason}}
    end
  end
end

defmodule OnlineVoting.Manager do
  use GenServer

  @dynamic_supervisor OnlineVoting.PollSupervisor

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    data_dir = Keyword.fetch!(opts, :data_dir)

    case File.mkdir_p(data_dir) do
      :ok ->
        recover_polls(data_dir)
        {:ok, %{data_dir: data_dir}}

      {:error, reason} ->
        {:stop, {:operational, {:directory_creation_failed, reason}}}
    end
  end

  @impl true
  def handle_call({:create_poll, question, options}, _from, state) do
    id = new_id()
    data = %{
      id: id,
      question: question,
      options: options,
      votes: Map.new(options, &{&1, 0})
    }

    with :ok <- OnlineVoting.Store.persist(state.data_dir, data),
         {:ok, _pid} <- start_poll(id, state.data_dir) do
      {:reply, {:ok, id}, state}
    else
      {:error, reason} ->
        {:reply, {:error, {:operational, reason}}, state}
    end
  end

  defp recover_polls(data_dir) do
    data_dir
    |> OnlineVoting.Store.files()
    |> Enum.each(fn path ->
      case OnlineVoting.Store.read_path(path) do
        {:ok, %{id: id} = data} ->
          if OnlineVoting.Store.valid_data?(data) and
               OnlineVoting.Store.path(data_dir, id) == path do
            case start_poll(id, data_dir) do
              {:ok, _pid} -> :ok
              {:error, {:already_started, _pid}} -> :ok
              _error -> :ok
            end
          end

        _error ->
          :ok
      end
    end)
  end

  defp start_poll(id, data_dir) do
    DynamicSupervisor.start_child(
      @dynamic_supervisor,
      {OnlineVoting.Poll, {id, data_dir}}
    )
  end

  defp new_id do
    unique =
      {System.system_time(:nanosecond), System.unique_integer([:positive, :monotonic]), node()}

    unique
    |> :erlang.term_to_binary()
    |> Base.url_encode64(padding: false)
  end
end

defmodule OnlineVoting.Poll do
  use GenServer

  def start_link({id, data_dir}) do
    GenServer.start_link(
      __MODULE__,
      {id, data_dir},
      name: {:via, Registry, {OnlineVoting.Registry, {:poll, id}}}
    )
  end

  def child_spec({id, data_dir}) do
    %{
      id: {__MODULE__, id},
      start: {__MODULE__, :start_link, [{id, data_dir}]},
      restart: :permanent,
      shutdown: 5_000,
      type: :worker
    }
  end

  @impl true
  def init({id, data_dir}) do
    case OnlineVoting.Store.read(data_dir, id) do
      {:ok, data} ->
        {:ok, %{data: data, data_dir: data_dir}}

      {:error, reason} ->
        {:stop, {:operational, reason}}
    end
  end

  @impl true
  def handle_call(:results, _from, state) do
    data = state.data

    {:reply,
     {:ok,
      %{
        id: data.id,
        question: data.question,
        options: data.options,
        votes: data.votes,
        total_votes: Enum.sum(Map.values(data.votes))
      }}, state}
  end

  def handle_call({:vote, option}, _from, state) do
    data = state.data

    if option in data.options do
      updated = put_in(data, [:votes, option], Map.fetch!(data.votes, option) + 1)

      case OnlineVoting.Store.persist(state.data_dir, updated) do
        :ok ->
          {:reply, {:ok, updated.votes[option]}, %{state | data: updated}}

        {:error, reason} ->
          {:reply, {:error, {:operational, reason}}, state}
      end
    else
      {:reply, {:error, {:validation, :unknown_option}}, state}
    end
  end
end

defmodule OnlineVoting.Store do
  def path(data_dir, id) do
    filename = Base.url_encode64(id, padding: false) <> ".vote"
    Path.join(data_dir, filename)
  end

  def files(data_dir) do
    Path.wildcard(Path.join(data_dir, "*.vote"))
  end

  def persist(data_dir, data) do
    path = path(data_dir, data.id)
    temp_path = path <> "." <> Integer.to_string(System.unique_integer([:positive])) <> ".tmp"
    bytes = :erlang.term_to_binary(data, [:compressed])

    with :ok <- File.mkdir_p(data_dir),
         :ok <- write_synced(temp_path, bytes),
         :ok <- rename(temp_path, path) do
      :ok
    else
      {:error, reason} ->
        _ = File.rm(temp_path)
        {:error, {:persistence_failed, reason}}
    end
  end

  def read(data_dir, id) do
    read_path(path(data_dir, id))
  end

  def read_path(path) do
    with {:ok, bytes} <- File.read(path),
         {:ok, data} <- decode(bytes),
         true <- valid_data?(data) do
      {:ok, data}
    else
      {:error, reason} -> {:error, {:persistence_read_failed, reason}}
      false -> {:error, :invalid_persisted_data}
    end
  end

  def valid_data?(%{id: id, question: question, options: options, votes: votes}) do
    is_binary(id) and id != "" and
      is_binary(question) and String.trim(question) != "" and
      is_list(options) and length(options) >= 2 and
      Enum.all?(options, &(is_binary(&1) and String.trim(&1) != "")) and
      length(Enum.uniq(options)) == length(options) and
      is_map(votes) and
      Map.keys(votes) |> Enum.sort() == Enum.sort(options) and
      Enum.all?(Map.values(votes), &(is_integer(&1) and &1 >= 0))
  end

  def valid_data?(_), do: false

  defp write_synced(path, bytes) do
    case File.write(path, bytes, [:binary, :sync]) do
      :ok -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  defp rename(source, destination) do
    case File.rename(source, destination) do
      :ok -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  defp decode(bytes) do
    try do
      {:ok, :erlang.binary_to_term(bytes, [:safe])}
    rescue
      _ -> {:error, :invalid_persisted_data}
    end
  end
end
