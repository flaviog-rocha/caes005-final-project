# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de votação online, atendendo somente aos seguintes requisitos:
#
# 1. Persistir os dados para recuperação após reinicializações.
# 2. Impedir que uma mesma operação seja executada mais de uma vez.
# 3. Processar grandes volumes de dados sem carregá-los integralmente na memória.
# 4. Fazer uso de macros.
# 5. Recuperar processos após falhas inesperadas.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule OnlineVoting.Macros do
  defmacro call(server, request) do
    quote do
      GenServer.call(unquote(server), unquote(request), :infinity)
    end
  end
end

defmodule OnlineVoting.Server do
  use GenServer

  @table __MODULE__

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    path = Keyword.fetch!(opts, :path)
    File.mkdir_p!(Path.dirname(path))

    case :dets.open_file(@table,
           file: String.to_charlist(path),
           type: :set,
           repair: true
         ) do
      {:ok, table} ->
        {:ok, %{table: table}}

      {:error, reason} ->
        {:stop, {:dets_open_failed, reason}}
    end
  end

  @impl true
  def handle_call({:vote, operation_id, choice}, _from, state)
      when is_binary(operation_id) and operation_id != "" and is_binary(choice) and choice != "" do
    key = {:operation, operation_id}

    case :dets.lookup(state.table, key) do
      [{^key, {:vote, existing_choice, _timestamp}}] ->
        {:reply, {:ok, :duplicate, existing_choice}, state}

      [] ->
        value = {:vote, choice, System.system_time(:millisecond)}

        case :dets.insert(state.table, {key, value}) do
          :ok ->
            case :dets.sync(state.table) do
              :ok -> {:reply, {:ok, :recorded}, state}
              {:error, reason} -> {:reply, {:error, {:sync_failed, reason}}, state}
            end

          {:error, reason} ->
            {:reply, {:error, {:insert_failed, reason}}, state}
        end
    end
  end

  def handle_call({:vote, _operation_id, _choice}, _from, state) do
    {:reply, {:error, :invalid_vote}, state}
  end

  def handle_call({:count, choice}, _from, state) when is_binary(choice) do
    count =
      :dets.foldl(
        fn
          {{:operation, _operation_id}, {:vote, ^choice, _timestamp}}, acc -> acc + 1
          _object, acc -> acc
        end,
        0,
        state.table
      )

    {:reply, {:ok, count}, state}
  end

  def handle_call(:count_all, _from, state) do
    counts =
      :dets.foldl(
        fn
          {{:operation, _operation_id}, {:vote, choice, _timestamp}}, acc ->
            Map.update(acc, choice, 1, &(&1 + 1))

          _object, acc ->
            acc
        end,
        %{},
        state.table
      )

    {:reply, {:ok, counts}, state}
  end

  @impl true
  def terminate(_reason, state) do
    :dets.close(state.table)
  end
end

defmodule OnlineVoting do
  use Supervisor
  require OnlineVoting.Macros

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    path = Keyword.get(opts, :path, Path.join(File.cwd!(), "online_voting.dets"))

    children = [
      {OnlineVoting.Server, path: path}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  def vote(operation_id, choice, server \\ OnlineVoting.Server) do
    OnlineVoting.Macros.call(server, {:vote, operation_id, choice})
  end

  def count(choice, server \\ OnlineVoting.Server) do
    OnlineVoting.Macros.call(server, {:count, choice})
  end

  def count_all(server \\ OnlineVoting.Server) do
    OnlineVoting.Macros.call(server, :count_all)
  end

  # Arquivo TSV: uma operação e uma opção por linha. Cada linha é processada
  # individualmente, sem carregar o arquivo inteiro na memória.
  def import_file(path, server \\ OnlineVoting.Server) do
    try do
      path
      |> File.stream!([], :line)
      |> Enum.reduce_while({0, 0, 0}, fn line, {line_number, recorded, duplicates} ->
        next_line = line_number + 1

        case parse_line(line) do
          {:ok, operation_id, choice} ->
            case vote(operation_id, choice, server) do
              {:ok, :recorded} ->
                {:cont, {next_line, recorded + 1, duplicates}}

              {:ok, :duplicate, _existing_choice} ->
                {:cont, {next_line, recorded, duplicates + 1}}

              {:error, reason} ->
                {:halt, {:error, next_line, reason, recorded, duplicates}}
            end

          {:error, reason} ->
            {:halt, {:error, next_line, reason, recorded, duplicates}}
        end
      end)
      |> case do
        {lines, recorded, duplicates} ->
          {:ok, %{lines: lines, recorded: recorded, duplicates: duplicates}}

        error ->
          error
      end
    rescue
      error in File.Error -> {:error, {:file_error, error.reason}}
    end
  end

  defp parse_line(line) do
    case String.split(String.trim(line), "\t", parts: 2) do
      [operation_id, choice] when operation_id != "" and choice != "" ->
        {:ok, operation_id, choice}

      _ ->
        {:error, :invalid_line}
    end
  end
end
