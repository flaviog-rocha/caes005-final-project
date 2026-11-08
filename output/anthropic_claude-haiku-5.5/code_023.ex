# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de empréstimos de livros, atendendo somente aos seguintes requisitos:
#
# 1. Permitir que diferentes entidades sejam processadas independentemente.
# 2. Implementar expiração automática de registros.
# 3. Detectar encerramentos inesperados de processos.
# 4. Quando fizer sentido para o domínio, considerar a comunicação com um banco de dados fictício e, opcionalmente, o uso de Ecto.Migration, sem tornar isso obrigatório.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - DynamicSupervisor
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# gerenciador_emprestimos.exs
# Execução: elixir gerenciador_emprestimos.exs

# Entidade de empréstimo: estrutura de dados imutável que representa o registro persistível.
defmodule Library.Loan do
  @enforce_keys [:id, :book, :borrower, :due_at]
  defstruct [:id, :book, :borrower, :due_at, status: :active, events: []]

  @type status :: :active | :returned | :expired | :failed

  @type t :: %__MODULE__{
          id: pos_integer(),
          book: String.t(),
          borrower: String.t(),
          due_at: DateTime.t(),
          status: status(),
          events: [{DateTime.t(), term()}]
        }

  # Cria um novo empréstimo com identificador único e prazo de expiração.
  def new(book, borrower, duration_ms) do
    %__MODULE__{
      id: System.unique_integer([:positive, :monotonic]),
      book: book,
      borrower: borrower,
      due_at: DateTime.add(DateTime.utc_now(), duration_ms, :millisecond)
    }
  end

  # Registra um evento no histórico do empréstimo (append funcional).
  def add_event(%__MODULE__{} = loan, event) do
    %{loan | events: [{DateTime.utc_now(), event} | loan.events]}
  end
end

# Banco de dados fictício em memória.
# Pode ser substituído por um Ecto.Repo sem alterar o restante do código.
# Caso se opte por Ecto, a tabela "loans" poderia ser criada por uma migration como:
#   create table(:loans) do
#     add :book, :string, null: false
#     add :borrower, :string, null: false
#     add :due_at, :utc_datetime_usec, null: false
#     add :status, :string, null: false
#     add :events, :map, default: %{}
#   end
defmodule Library.Store do
  @moduledoc "Armazenamento fictício de empréstimos, implementado com Agent."
  use Agent

  def start_link(_opts) do
    Agent.start_link(fn -> %{} end, name: __MODULE__)
  end

  def get(id), do: Agent.get(__MODULE__, &Map.get(&1, id))

  def put(%Library.Loan{id: id} = loan) do
    Agent.update(__MODULE__, &Map.put(&1, id, loan))
  end

  def all, do: Agent.get(__MODULE__, &Map.values/1)
end

# Um processo por empréstimo. Cada entidade é processada de forma independente.
# restart: :transient faz com que o DynamicSupervisor reinicie o processo apenas
# em caso de falha; encerramentos :normal não geram reinício.
defmodule Library.LoanServer do
  @moduledoc "GenServer que controla o ciclo de vida de um único empréstimo."
  use GenServer, restart: :transient
  require Logger
  alias Library.{Loan, Store}

  def start_link(%Loan{id: id} = loan) do
    GenServer.start_link(__MODULE__, loan, name: via(id))
  end

  # Registro de nome por Registry, permitindo localizar o processo pelo id do empréstimo.
  def via(id), do: {:via, Registry, {Library.Registry, id}}

  def return(id), do: GenServer.call(via(id), :return)

  @impl true
  def init(%Loan{id: id} = arg) do
    # Permite que terminate/2 seja executado em encerramentos controlados pelo supervisor.
    Process.flag(:trap_exit, true)

    # Recupera o estado persistido. Em caso de reinício após falha,
    # o processo retoma o estado salvo no "banco" em vez do argumento original.
    loan = Store.get(id) || arg

    if loan.status == :active do
      Store.put(loan)
      schedule_expiry(loan)
      {:ok, loan}
    else
      # Empréstimo já finalizado: não há processo a manter.
      :ignore
    end
  end

  @impl true
  def handle_call(:return, _from, %Loan{status: :active} = loan) do
    loan = loan |> Loan.add_event(:returned) |> Map.put(:status, :returned)
    Store.put(loan)
    {:stop, :normal, :ok, loan}
  end

  def handle_call(:return, _from, loan) do
    {:reply, {:error, loan.status}, loan}
  end

  @impl true
  def handle_info(:expire, %Loan{} = loan) do
    # Expiração automática disparada pelo timer agendado em schedule_expiry/1.
    loan = loan |> Loan.add_event(:expired) |> Map.put(:status, :expired)
    Store.put(loan)
    {:stop, :normal, loan}
  end

  @impl true
  def terminate(reason, %Loan{} = loan) do
    # Registra o motivo do encerramento no histórico (quando o terminate é executado).
    loan = Loan.add_event(loan, {:terminated, reason})
    Store.put(loan)
    :ok
  end

  # Calcula o tempo restante até o vencimento e agenda a expiração.
  defp schedule_expiry(%Loan{due_at: due_at}) do
    remaining = max(DateTime.diff(due_at, DateTime.utc_now(), :millisecond), 0)
    Process.send_after(self(), :expire, remaining)
  end
end

# Gerenciador: monitora os processos de empréstimo e detecta encerramentos inesperados.
# Quando um processo cai com razão anormal, o DynamicSupervisor o reinicia (transient).
# O gerenciador verifica se houve reinício; se não houver, marca o empréstimo como :failed.
defmodule Library.Manager do
  @moduledoc "Coordena empréstimos e monitora falhas de processos."
  use GenServer
  require Logger
  alias Library.{Loan, LoanServer, Store}

  @recheck_ms 50
  @max_attempts 5

  def start_link(_opts) do
    GenServer.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  def borrow(book, borrower, duration_ms) do
    GenServer.call(__MODULE__, {:borrow, Loan.new(book, borrower, duration_ms)})
  end

  @impl true
  def init(:ok), do: {:ok, %{monitors: %{}}}

  @impl true
  def handle_call({:borrow, loan}, _from, state) do
    Store.put(loan)

    case DynamicSupervisor.start_child(Library.LoanSupervisor, {LoanServer, loan}) do
      {:ok, pid} when is_pid(pid) ->
        {:reply, {:ok, loan.id}, monitor(state, loan.id, pid)}

      {:ok, _} ->
        {:reply, {:error, :not_started}, state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_info({:DOWN, ref, :process, _pid, reason}, state) do
    case Map.pop(state.monitors, ref) do
      {nil, _} ->
        {:noreply, state}

      {id, monitors} ->
        state = %{state | monitors: monitors}

        if clean_exit?(reason) do
          {:noreply, state}
        else
          Logger.warning("Empréstimo #{id} encerrou inesperadamente: #{inspect(reason)}")
          Process.send_after(self(), {:check_restart, id, 1}, @recheck_ms)
          {:noreply, state}
        end
    end
  end

  def handle_info({:check_restart, id, attempt}, state) do
    alive =
      Library.Registry
      |> Registry.lookup(id)
      |> Enum.filter(fn {pid, _} -> Process.alive?(pid) end)

    case alive do
      [{pid, _}] ->
        Logger.info("Empréstimo #{id} reiniciado pelo supervisor (#{inspect(pid)})")
        {:noreply, monitor(state, id, pid)}

      [] when attempt < @max_attempts ->
        Process.send_after(self(), {:check_restart, id, attempt + 1}, @recheck_ms)
        {:noreply, state}

      [] ->
        mark_failed(id)
        {:noreply, state}
    end
  end

  defp monitor(state, id, pid) do
    ref = Process.monitor(pid)
    %{state | monitors: Map.put(state.monitors, ref, id)}
  end

  defp clean_exit?(:normal), do: true
  defp clean_exit?(:shutdown), do: true
  defp clean_exit?({:shutdown, _}), do: true
  defp clean_exit?(_), do: false

  # Marca como :failed apenas empréstimos que ainda estavam ativos.
  defp mark_failed(id) do
    case Store.get(id) do
      %Loan{status: :active} = loan ->
        Logger.error("Empréstimo #{id} não foi reiniciado; marcado como :failed")
        loan = loan |> Loan.add_event(:failed) |> Map.put(:status, :failed)
        Store.put(loan)

      _ ->
        :ok
    end
  end
end

# Fachada pública e árvore de supervisão.
defmodule Library do
  @moduledoc "API pública do sistema de empréstimos."
  alias Library.{Loan, LoanServer, Store, Manager}

  # Inicia a árvore de supervisão: armazenamento, registry, DynamicSupervisor e gerenciador.
  def start do
    children = [
      Store,
      {Registry, keys: :unique, name: Library.Registry},
      {DynamicSupervisor, strategy: :one_for_one, name: Library.LoanSupervisor},
      Manager
    ]

    Supervisor.start_link(children, strategy: :one_for_one, name: Library.Supervisor)
  end

  def borrow(book, borrower, duration_ms), do: Manager.borrow(book, borrower, duration_ms)

  def return(id) do
    LoanServer.return(id)
  catch
    :exit, _ -> {:error, :not_active}
  end

  def status(id), do: Store.get(id)
end

# Demonstração das funcionalidades.
defmodule Library.Demo do
  @moduledoc "Executa um cenário de exemplo."

  def run do
    {:ok, _} = Application.ensure_all_started(:logger)
    {:ok, _sup} = Library.start()

    {:ok, a} = Library.borrow("Dom Casmurro", "Ana", 60_000)
    {:ok, b} = Library.borrow("1984", "Bruno", 300)
    {:ok, c} = Library.borrow("O Hobbit", "Carla", 60_000)

    # Simula encerramento inesperado do processo do empréstimo "a".
    [{pid, _}] = Registry.lookup(Library.Registry, a)
    Process.exit(pid, :kill)
    Process.sleep(200)

    IO.inspect(Library.status(a).status, label: "após kill (esperado: :active)")

    IO.inspect(Library.return(c), label: "devolução de c")

    # Aguarda a expiração automática do empréstimo "b".
    Process.sleep(500)

    IO.inspect(Library.status(a).status, label: "a (esperado: :active)")
    IO.inspect(Library.status(b).status, label: "b (esperado: :expired)")
    IO.inspect(Library.status(c).status, label: "c (esperado: :returned)")
  end
end

Library.Demo.run()
