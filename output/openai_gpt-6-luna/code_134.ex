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

defmodule Voting do
  @moduledoc """
  Sistema de votação persistente com processos independentes por votação.
  """

  def start_link(opts \\ []) do
    Voting.Supervisor.start_link(opts)
  end

  def create(question, options) do
    with {:ok, question} <- validate_text(question, :question),
         {:ok, options} <- validate_options(options),
         {:ok, id} <- generate_id(),
         data = %{id: id, question: question, options: Map.new(options, &{&1, 0})},
         :ok <- Voting.Store.insert_new(id, data),
         :ok <- ensure_poll_started(id) do
      {:ok, id}
    end
  end

  def vote(id, option) do
    with :ok <- validate_id(id),
         {:ok, option} <- validate_text(option, :option) do
      call_poll(id, {:vote, option})
    end
  end

  def results(id) do
    with :ok <- validate_id(id) do
      call_poll(id, :results)
    end
  end

  defp validate_text(value, field) when is_binary(value) do
    value = String.trim(value)

    if value == "" do
      validation_error({:empty, field})
    else
      {:ok, value}
    end
  end

  defp validate_text(_value, field), do: validation_error({:invalid_type, field})

  defp validate_options(options) when is_list(options) do
    if proper_list?(options) and length(options) >= 2 do
      validated =
        Enum.reduce_while(options, {:ok, []}, fn option, {:ok, acc} ->
          case validate_text(option, :option) do
            {:ok, value} -> {:cont, {:ok, [value | acc]}}
            error -> {:halt, error}
          end
        end)

      case validated do
        {:ok, values} ->
          values = Enum.reverse(values)

          if length(Enum.uniq(values)) == length(values) do
            {:ok, values}
          else
            validation_error(:duplicate_options)
          end

        error ->
          error
      end
    else
      validation_error(:at_least_two_options_required)
    end
  end

  defp validate_options(_options), do: validation_error(:options_must_be_a_list)

  defp validate_id(id) when is_binary(id) and byte_size(id) > 0, do: :ok
  defp validate_id(_id), do: validation_error(:invalid_poll_id)

  defp proper_list?([]), do: true
  defp proper_list?([_ | tail]), do: proper_list?(tail)
  defp proper_list?(_), do: false

  defp validation_error(reason), do: {:error, {:validation, reason}}

  defp generate_id do
    try do
      {:ok, Base.url_encode64(:crypto.strong_rand_bytes(16), padding: false)}
    rescue
      error -> {:error, {:operational, {:id_generation_failed, Exception.message(error)}}}
    catch
      kind, reason -> {:error, {:operational, {:id_generation_failed, kind, reason}}}
    end
  end

  defp ensure_poll_started(id) do
    case DynamicSupervisor.start_child(Voting.PollSupervisor, {Voting.Poll, id}) do
      {:ok, _pid} -> :ok
      {:error, {:already_started, _pid}} -> :ok
      {:error, reason} -> {:error, {:operational, {:poll_start_failed, reason}}}
    end
  rescue
    error -> {:error, {:operational, {:poll_start_failed, Exception.message(error)}}}
  catch
    kind, reason -> {:error, {:operational, {:poll_start_failed, kind, reason}}}
  end

  defp call_poll(id, request) do
    try do
      case Registry.lookup(Voting.Registry, id) do
        [{pid, _value}] ->
          safe_call(pid, request)

        [] ->
          case Voting.Store.get(id) do
            {:ok, _data} -> {:error, {:operational, :poll_process_unavailable}}
            {:error, :not_found} -> validation_error(:poll_not_found)
            {:error, _} = error -> error
          end
      end
    rescue
      error -> {:error, {:operational, {:registry_failure, Exception.message(error)}}}
    catch
      kind, reason -> {:error, {:operational, {:registry_failure, kind, reason}}}
    end
  end

  defp safe_call(pid, request) do
    GenServer.call(pid, request)
  rescue
    error -> {:error, {:operational, {:poll_call_failed, Exception.message(error)}}}
  catch
    :exit, reason -> {:error, {:operational, {:poll_call_failed, reason}}}
    kind, reason -> {:error, {:operational, {:poll_call_failed, kind, reason}}}
  end
end

defmodule Voting.Supervisor do
  use Supervisor

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    path =
      Keyword.get(
        opts,
        :path,
        Path.join(File.cwd!(), "voting_data.dets")
      )

    children = [
      {Voting.Store, path: path},
      {Registry, keys: :unique, name: Voting.Registry},
      {DynamicSupervisor, strategy: :one_for_one, name: Voting.PollSupervisor},
      Voting.Bootstrap
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end

defmodule Voting.Store do
  use GenServer

  @table :voting_persistent_store

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def get(id), do: safe_call({:get, id})
  def insert_new(id, data), do: safe_call({:insert_new, id, data})
  def put(id, data), do: safe_call({:put, id, data})
  def all_ids, do: safe_call(:all_ids)

  @impl true
  def init(opts) do
    path = Keyword.fetch!(opts, :path)

    with :ok <- File.mkdir_p(Path.dirname(path)),
         {:ok, _table} <-
           :dets.open_file(@table,
             file: String.to_charlist(path),
             type: :set,
             repair: true
           ) do
      {:ok, %{}}
    else
      {:error, reason} -> {:stop, {:operational, {:store_open_failed, reason}}}
    end
  end

  @impl true
  def handle_call({:get, id}, _from, state) do
    reply =
      guarded(fn ->
        case :dets.lookup(@table, id) do
          [{^id, data}] -> {:ok, data}
          [] -> {:error, :not_found}
        end
      end)

    {:reply, reply, state}
  end

  def handle_call({:insert_new, id, data}, _from, state) do
    reply =
      guarded(fn ->
        case :dets.insert_new(@table, {id, data}) do
          true -> sync_store()
          false -> {:error, {:operational, :id_collision}}
        end
      end)

    {:reply, reply, state}
  end

  def handle_call({:put, id, data}, _from, state) do
    reply =
      guarded(fn ->
        case :dets.insert(@table, {id, data}) do
          :ok -> sync_store()
          other -> {:error, {:operational, {:store_write_failed, other}}}
        end
      end)

    {:reply, reply, state}
  end

  def handle_call(:all_ids, _from, state) do
    reply =
      guarded(fn ->
        ids = :dets.foldl(fn {id, _data}, acc -> [id | acc] end, [], @table)
        {:ok, ids}
      end)

    {:reply, reply, state}
  end

  @impl true
  def terminate(_reason, _state) do
    :dets.close(@table)
    :ok
  end

  defp safe_call(request) do
    GenServer.call(__MODULE__, request)
  rescue
    error -> {:error, {:operational, {:store_call_failed, Exception.message(error)}}}
  catch
    :exit, reason -> {:error, {:operational, {:store_call_failed, reason}}}
    kind, reason -> {:error, {:operational, {:store_call_failed, kind, reason}}}
  end

  defp sync_store do
    case :dets.sync(@table) do
      :ok -> :ok
      {:error, reason} -> {:error, {:operational, {:store_sync_failed, reason}}}
      other -> {:error, {:operational, {:store_sync_failed, other}}}
    end
  end

  defp guarded(fun) do
    fun.()
  rescue
    error -> {:error, {:operational, {:store_failure, Exception.message(error)}}}
  catch
    kind, reason -> {:error, {:operational, {:store_failure, kind, reason}}}
  end
end

defmodule Voting.Poll do
  use GenServer

  def child_spec(id) do
    %{
      id: {__MODULE__, id},
      start: {__MODULE__, :start_link, [id]},
      restart: :permanent,
      shutdown: 5_000,
      type: :worker
    }
  end

  def start_link(id) do
    GenServer.start_link(__MODULE__, id, name: {:via, Registry, {Voting.Registry, id}})
  end

  @impl true
  def init(id) do
    case Voting.Store.get(id) do
      {:ok, _data} -> {:ok, %{id: id}}
      {:error, reason} -> {:stop, {:operational, {:poll_restore_failed, reason}}}
    end
  end

  @impl true
  def handle_call({:vote, option}, _from, state) do
    reply =
      with {:ok, data} <- Voting.Store.get(state.id),
           true <- Map.has_key?(data.options, option) do
        updated = update_in(data.options[option], &(&1 + 1))

        case Voting.Store.put(state.id, updated) do
          :ok -> {:ok, public_results(updated)}
          {:error, _} = error -> error
        end
      else
        {:error, :not_found} -> {:error, {:validation, :poll_not_found}}
        {:error, _} = error -> error
        false -> {:error, {:validation, :unknown_option}}
      end

    {:reply, reply, state}
  end

  def handle_call(:results, _from, state) do
    reply =
      case Voting.Store.get(state.id) do
        {:ok, data} -> {:ok, public_results(data)}
        {:error, :not_found} -> {:error, {:validation, :poll_not_found}}
        {:error, _} = error -> error
      end

    {:reply, reply, state}
  end

  defp public_results(data) do
    %{id: data.id, question: data.question, votes: data.options}
  end
end

defmodule Voting.Bootstrap do
  use GenServer

  def start_link(_opts \\ []) do
    GenServer.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  @impl true
  def init(:ok), do: {:ok, :ok, {:continue, :restore}}

  @impl true
  def handle_continue(:restore, state) do
    case Voting.Store.all_ids() do
      {:ok, ids} ->
        Enum.each(ids, fn id ->
          case DynamicSupervisor.start_child(Voting.PollSupervisor, {Voting.Poll, id}) do
            {:ok, _pid} -> :ok
            {:error, {:already_started, _pid}} -> :ok
            {:error, reason} -> exit({:poll_restore_failed, id, reason})
          end
        end)

        {:noreply, state}

      {:error, reason} ->
        exit({:poll_list_failed, reason})
    end
  end
end
