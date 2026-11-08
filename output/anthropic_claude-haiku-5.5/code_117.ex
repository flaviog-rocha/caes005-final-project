# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de empréstimos de livros, atendendo somente aos seguintes requisitos:
#
# 1. Persistir os dados para recuperação após reinicializações.
# 2. Validar os dados recebidos antes de realizar operações.
# 3. Permitir cadastrar, consultar, atualizar e remover registros.
# 4. Executar determinadas operações periodicamente.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - Task
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# Gerenciador de empréstimos de livros (arquivo único, apenas biblioteca padrão).
#
# Exemplo de uso:
#   {:ok, _pid} = LoanManager.start_link(path: "dados/emprestimos.bin", interval: :timer.minutes(1))
#   {:ok, loan} = LoanManager.create(%{book_title: "Dom Casmurro", borrower: "Ana", loan_days: 7})
#   {:ok, loan} = LoanManager.get(loan.id)
#   {:ok, loan} = LoanManager.update(loan.id, %{returned_on: Date.utc_today()})
#   :ok = LoanManager.delete(loan.id)
#   {:ok, loans} = LoanManager.list()

defmodule LoanManager do
  @moduledoc """
  Gerencia empréstimos de livros com:
  - persistência em disco (sobrevive a reinicializações),
  - validação de dados antes de qualquer operação,
  - CRUD de empréstimos,
  - atualização periódica de status (ex.: marcar empréstimos atrasados) executada por Task.
  """

  use GenServer
  require Logger

  @default_path "loan_manager_data.bin"
  @default_interval :timer.minutes(5)

  ## ---------------------------------------------------------------------------
  ## API pública
  ## ---------------------------------------------------------------------------

  # Inicia o gerenciador.
  # Opções:
  #   :name     - nome do processo (padrão: __MODULE__)
  #   :path     - arquivo de persistência (padrão: "loan_manager_data.bin")
  #   :interval - intervalo, em ms, da tarefa periódica de atualização de status
  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  # Cadastra um novo empréstimo. Retorna {:ok, loan} | {:error, motivo}.
  def create(attrs, server \\ __MODULE__), do: GenServer.call(server, {:create, attrs})

  # Consulta um empréstimo pelo id. Retorna {:ok, loan} | {:error, :not_found}.
  def get(id, server \\ __MODULE__), do: GenServer.call(server, {:get, id})

  # Lista todos os empréstimos ordenados por id. Retorna {:ok, [loan]}.
  def list(server \\ __MODULE__), do: GenServer.call(server, :list)

  # Atualiza campos de um empréstimo. Retorna {:ok, loan} | {:error, motivo}.
  def update(id, attrs, server \\ __MODULE__), do: GenServer.call(server, {:update, id, attrs})

  # Remove um empréstimo. Retorna :ok | {:error, :not_found}.
  def delete(id, server \\ __MODULE__), do: GenServer.call(server, {:delete, id})

  ## ---------------------------------------------------------------------------
  ## Callbacks do GenServer
  ## ---------------------------------------------------------------------------

  @impl true
  def init(opts) do
    path = Keyword.get(opts, :path, @default_path)
    interval = Keyword.get(opts, :interval, @default_interval)

    case LoanManager.Storage.load(path) do
      {:ok, data} ->
        # Agenda a tarefa periódica após a inicialização, fora do init/1.
        {:ok, %{path: path, interval: interval, data: data}, {:continue, :schedule}}

      {:error, reason} ->
        {:stop, {:load_failed, reason}}
    end
  end

  @impl true
  def handle_continue(:schedule, state) do
    server = self()
    # Task vinculada ao GenServer: é iniciada para executar operações periódicas.
    {:ok, _pid} = Task.start_link(fn -> periodic_loop(server, state.interval) end)
    {:noreply, state}
  end

  @impl true
  def handle_call({:create, attrs}, _from, state) do
    case LoanManager.Validator.validate_create(attrs) do
      {:ok, v} ->
        today = Date.utc_today()
        id = state.data.next_id

        loan = %{
          id: id,
          book_title: v.book_title,
          borrower: v.borrower,
          loaned_on: today,
          due_on: Date.add(today, v.loan_days),
          returned_on: nil,
          status: :active
        }

        data = %{
          state.data
          | next_id: id + 1,
            loans: Map.put(state.data.loans, id, loan)
        }

        commit(state, data, {:ok, loan})

      {:error, _} = error ->
        {:reply, error, state}
    end
  end

  def handle_call({:get, id}, _from, state) do
    reply =
      case Map.fetch(state.data.loans, id) do
        {:ok, loan} -> {:ok, loan}
        :error -> {:error, :not_found}
      end

    {:reply, reply, state}
  end

  def handle_call(:list, _from, state) do
    loans =
      state.data.loans
      |> Map.values()
      |> Enum.sort_by(& &1.id)

    {:reply, {:ok, loans}, state}
  end

  def handle_call({:update, id, attrs}, _from, state) do
    with {:ok, loan} <- fetch_loan(state.data, id),
         {:ok, changes} <- LoanManager.Validator.validate_update(attrs),
         merged = Map.merge(loan, changes),
         :ok <- LoanManager.Validator.validate_dates(merged) do
      updated = %{merged | status: status_for(merged, Date.utc_today())}
      data = %{state.data | loans: Map.put(state.data.loans, id, updated)}
      commit(state, data, {:ok, updated})
    else
      {:error, _} = error -> {:reply, error, state}
    end
  end

  def handle_call({:delete, id}, _from, state) do
    if Map.has_key?(state.data.loans, id) do
      data = %{state.data | loans: Map.delete(state.data.loans, id)}
      commit(state, data, :ok)
    else
      {:reply, {:error, :not_found}, state}
    end
  end

  # Operação periódica: recalcula o status de todos os empréstimos (ex.: :overdue).
  @impl true
  def handle_cast(:refresh_statuses, state) do
    today = Date.utc_today()

    loans =
      Map.new(state.data.loans, fn {id, loan} ->
        {id, %{loan | status: status_for(loan, today)}}
      end)

    if loans == state.data.loans do
      {:noreply, state}
    else
      data = %{state.data | loans: loans}

      case LoanManager.Storage.save(state.path, data) do
        :ok ->
          {:noreply, %{state | data: data}}

        {:error, reason} ->
          Logger.error("Falha ao persistir atualização de status: #{inspect(reason)}")
          {:noreply, state}
      end
    end
  end

  ## ---------------------------------------------------------------------------
  ## Funções privadas
  ## ---------------------------------------------------------------------------

  # Persiste os dados antes de responder. Só altera o estado se a gravação for bem-sucedida.
  defp commit(state, data, reply) do
    case LoanManager.Storage.save(state.path, data) do
      :ok -> {:reply, reply, %{state | data: data}}
      {:error, reason} -> {:reply, {:error, {:persistence_failed, reason}}, state}
    end
  end

  defp fetch_loan(data, id) do
    case Map.fetch(data.loans, id) do
      {:ok, loan} -> {:ok, loan}
      :error -> {:error, :not_found}
    end
  end

  defp status_for(%{returned_on: returned}, _today) when not is_nil(returned), do: :returned

  defp status_for(%{due_on: due}, today) do
    if Date.compare(today, due) == :gt, do: :overdue, else: :active
  end

  # Loop executado em Task. Envia a operação periódica ao GenServer e encerra
  # quando o processo dono deixar de existir.
  defp periodic_loop(server, interval) do
    Process.sleep(interval)

    if Process.alive?(server) do
      GenServer.cast(server, :refresh_statuses)
      periodic_loop(server, interval)
    end

    :ok
  end

  ## ---------------------------------------------------------------------------
  ## Validação
  ## ---------------------------------------------------------------------------
end

defmodule LoanManager.Validator do
  @moduledoc false

  @default_loan_days 14
  @create_fields [:book_title, :borrower, :loan_days]
  @update_fields [:book_title, :borrower, :due_on, :returned_on]

  # Valida os atributos de criação. Retorna {:ok, mapa_validado} | {:error, motivo}.
  def validate_create(attrs) when is_map(attrs) do
    with :ok <- check_known_keys(attrs, @create_fields),
         {:ok, title} <- required_string(attrs, :book_title),
         {:ok, borrower} <- required_string(attrs, :borrower),
         {:ok, days} <- loan_days(Map.get(attrs, :loan_days, @default_loan_days)) do
      {:ok, %{book_title: title, borrower: borrower, loan_days: days}}
    end
  end

  def validate_create(_), do: {:error, :invalid_attrs}

  # Valida os atributos de atualização (apenas campos presentes).
  def validate_update(attrs) when is_map(attrs) and map_size(attrs) == 0,
    do: {:error, :empty_update}

  def validate_update(attrs) when is_map(attrs) do
    with :ok <- check_known_keys(attrs, @update_fields) do
      Enum.reduce_while(attrs, {:ok, %{}}, fn {key, value}, {:ok, acc} ->
        case validate_field(key, value) do
          {:ok, valid} -> {:cont, {:ok, Map.put(acc, key, valid)}}
          {:error, _} = error -> {:halt, error}
        end
      end)
    end
  end

  def validate_update(_), do: {:error, :invalid_attrs}

  # Garante coerência entre datas após a aplicação das alterações.
  def validate_dates(%{loaned_on: loaned, due_on: due, returned_on: returned}) do
    cond do
      Date.compare(due, loaned) == :lt ->
        {:error, {:due_on, :before_loaned_on}}

      not is_nil(returned) and Date.compare(returned, loaned) == :lt ->
        {:error, {:returned_on, :before_loaned_on}}

      true ->
        :ok
    end
  end

  ## Funções privadas

  defp check_known_keys(attrs, allowed) do
    case Map.keys(attrs) -- allowed do
      [] -> :ok
      unknown -> {:error, {:unknown_fields, unknown}}
    end
  end

  defp required_string(attrs, key) do
    case Map.get(attrs, key) do
      nil ->
        {:error, {key, :required}}

      value when is_binary(value) ->
        trimmed = String.trim(value)

        cond do
          trimmed == "" -> {:error, {key, :blank}}
          String.length(trimmed) > 200 -> {:error, {key, :too_long}}
          true -> {:ok, trimmed}
        end

      _ ->
        {:error, {key, :not_a_string}}
    end
  end

  defp loan_days(days) when is_integer(days) and days in 1..365, do: {:ok, days}
  defp loan_days(_), do: {:error, {:loan_days, :out_of_range}}

  defp validate_field(key, value) when key in [:book_title, :borrower],
    do: required_string(%{key => value}, key)

  defp validate_field(:returned_on, nil), do: {:ok, nil}

  defp validate_field(key, value) when key in [:due_on, :returned_on], do: to_date(key, value)

  defp to_date(_key, %Date{} = date), do: {:ok, date}

  defp to_date(key, value) when is_binary(value) do
    case Date.from_iso8601(value) do
      {:ok, date} -> {:ok, date}
      {:error, _} -> {:error, {key, :invalid_date}}
    end
  end

  defp to_date(key, _), do: {:error, {key, :invalid_date}}
end

defmodule LoanManager.Storage do
  @moduledoc false

  # Carrega os dados do disco. Se o arquivo não existir, retorna estado inicial.
  def load(path) do
    case File.read(path) do
      {:ok, binary} ->
        try do
          {:ok, :erlang.binary_to_term(binary, [:safe])}
        rescue
          ArgumentError -> {:error, :corrupted_data}
        end

      {:error, :enoent} ->
        {:ok, %{next_id: 1, loans: %{}}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  # Grava de forma atômica: escreve em arquivo temporário e renomeia.
  def save(path, data) do
    tmp = path <> ".tmp"

    with :ok <- File.mkdir_p(Path.dirname(path)),
         :ok <- File.write(tmp, :erlang.term_to_binary(data)),
         :ok <- File.rename(tmp, path) do
      :ok
    end
  end
end
