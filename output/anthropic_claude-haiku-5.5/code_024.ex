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

# arquivo: emprestimos.exs
# Execução: elixir emprestimos.exs
#
# Arquitetura:
#   Library.App (Supervisor raiz)
#     ├── Library.FakeDB            (Agent: banco de dados fictício em memória)
#     ├── Registry                  (nomeia um processo por empréstimo)
#     ├── Library.Watcher           (monitora processos de empréstimo e detecta quedas)
#     └── DynamicSupervisor         (supervisiona um processo Library.Loan por empréstimo)
#
# Cada empréstimo é um processo independente. Quando um processo morre de forma
# inesperada, o DynamicSupervisor o reinicia e o estado é recuperado do "banco".
# Empréstimos expiram automaticamente via Process.send_after/3.

Application.ensure_all_started(:logger)

defmodule Library.FakeDB do
  @moduledoc """
  Banco de dados fictício em memória, implementado com Agent.
  Simula persistência para permitir recuperação do estado após falhas.
  Pode ser substituído por um Ecto.Repo (com migrations) sem alterar os demais módulos.
  """
  use Agent

  def start_link(_opts \\ []) do
    Agent.start_link(fn -> %{} end, name: __MODULE__)
  end

  @doc "Retorna o registro associado ao id, ou nil."
  def get(id), do: Agent.get(__MODULE__, &Map.get(&1, id))

  @doc "Insere ou substitui um registro."
  def put(id, record), do: Agent.update(__MODULE__, &Map.put(&1, id, record))

  @doc "Atualiza um registro existente aplicando a função fornecida."
  def update(id, fun) do
    Agent.update(__MODULE__, fn map ->
      case Map.fetch(map, id) do
        {:ok, record} -> Map.put(map, id, fun.(record))
        :error -> map
      end
    end)
  end
end

defmodule Library.Watcher do
  @moduledoc """
  Detecta encerramentos inesperados de processos de empréstimo.
  Usa Process.monitor/1: ao receber {:DOWN, ...} com motivo anormal,
  registra a falha no banco. O DynamicSupervisor cuida da reinicialização.
  """
  use GenServer
  require Logger

  def start_link(_opts \\ []) do
    GenServer.start_link(__MODULE__, %{}, name: __MODULE__)
  end

  @doc "Passa a monitorar o processo de um empréstimo."
  def watch(pid, loan_id), do: GenServer.cast(__MODULE__, {:watch, pid, loan_id})

  @impl true
  def init(state), do: {:ok, state}

  @impl true
  def handle_cast({:watch, pid, loan_id}, state) do
    ref = Process.monitor(pid)
    {:noreply, Map.put(state, ref, loan_id)}
  end

  @impl true
  def handle_info({:DOWN, ref, :process, _pid, reason}, state) do
    {loan_id, state} = Map.pop(state, ref)

    cond do
      # Processo não estava sendo monitorado por este watcher
      is_nil(loan_id) ->
        :ok

      # Encerramento esperado (devolução, expiração ou desligamento normal)
      normal_exit?(reason) ->
        :ok

      # Encerramento inesperado: registra a falha para auditoria
      true ->
        Logger.warning("Empréstimo #{loan_id} encerrou inesperadamente: #{inspect(reason)}")

        Library.FakeDB.update(loan_id, fn rec ->
          %{rec | crashes: rec.crashes + 1, last_crash: reason}
        end)
    end

    {:noreply, state}
  end

  defp normal_exit?(:normal), do: true
  defp normal_exit?(:shutdown), do: true
  defp normal_exit?({:shutdown, _}), do: true
  defp normal_exit?(_), do: false
end

defmodule Library.Loan do
  @moduledoc """
  Processo independente que representa um empréstimo.
  - Expira automaticamente ao atingir `due_at` (encerra com :normal).
  - Restart :transient: é reiniciado apenas após encerramento anormal.
  - Na inicialização, recupera o estado persistido se o empréstimo estiver ativo.
  """
  use GenServer, restart: :transient
  require Logger

  def start_link(%{id: id} = attrs) do
    GenServer.start_link(__MODULE__, attrs, name: via(id))
  end

  @doc "Nome de registro do processo do empréstimo."
  def via(id), do: {:via, Registry, {Library.LoanRegistry, id}}

  @impl true
  def init(attrs) do
    record = restore_or_build(attrs)

    # Persiste o estado inicial/recuperado e registra o monitoramento
    Library.FakeDB.put(record.id, record)
    Library.Watcher.watch(self(), record.id)

    # Agenda a expiração automática
    delay = max(record.due_at - System.system_time(:millisecond), 0)
    Process.send_after(self(), :expire, delay)

    if record.crashes > 0 do
      Logger.info("Empréstimo #{record.id} recuperado após falha (#{record.crashes} ocorrência(s))")
    end

    {:ok, record}
  end

  @impl true
  def handle_call(:return, _from, state) do
    new_state = %{state | status: :returned}
    Library.FakeDB.put(new_state.id, new_state)
    {:stop, :normal, :ok, new_state}
  end

  @impl true
  def handle_info(:expire, state) do
    Logger.info("Empréstimo #{state.id} expirou")
    new_state = %{state | status: :expired}
    Library.FakeDB.put(new_state.id, new_state)
    {:stop, :normal, new_state}
  end

  # Recupera registro ativo do "banco"; caso contrário, cria um novo a partir dos atributos
  defp restore_or_build(%{id: id} = attrs) do
    case Library.FakeDB.get(id) do
      %{status: :active} = record ->
        record

      _ ->
        %{
          id: id,
          book: attrs.book,
          member: attrs.member,
          status: :active,
          due_at: attrs.due_at,
          crashes: 0,
          last_crash: nil
        }
    end
  end
end

defmodule Library.Loans do
  @moduledoc """
  API pública do gerenciamento de empréstimos.
  """

  @doc "Cria um novo empréstimo com duração em milissegundos."
  def borrow(id, book, member, duration_ms) do
    case Library.FakeDB.get(id) do
      %{status: :active} ->
        {:error, :already_active}

      _ ->
        attrs = %{
          id: id,
          book: book,
          member: member,
          due_at: System.system_time(:millisecond) + duration_ms
        }

        case DynamicSupervisor.start_child(Library.LoanSupervisor, {Library.Loan, attrs}) do
          {:ok, _pid} -> :ok
          {:error, {:already_started, _pid}} -> {:error, :already_active}
          {:error, reason} -> {:error, reason}
        end
    end
  end

  @doc "Devolve um livro, encerrando o processo do empréstimo."
  def return(id) do
    case Registry.lookup(Library.LoanRegistry, id) do
      [{pid, _}] ->
        try do
          GenServer.call(pid, :return)
        catch
          :exit, _ -> {:error, :not_active}
        end

      [] ->
        {:error, :not_active}
    end
  end

  @doc "Consulta o estado de um empréstimo no banco e se o processo está vivo."
  def status(id) do
    case Library.FakeDB.get(id) do
      nil ->
        {:error, :not_found}

      rec ->
        {:ok,
         %{
           status: rec.status,
           crashes: rec.crashes,
           due_at: rec.due_at,
           alive?: Registry.lookup(Library.LoanRegistry, id) != []
         }}
    end
  end
end

defmodule Library.App do
  @moduledoc """
  Supervisor raiz da aplicação.
  """
  use Supervisor

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    children = [
      Library.FakeDB,
      {Registry, keys: :unique, name: Library.LoanRegistry},
      Library.Watcher,
      {DynamicSupervisor, strategy: :one_for_one, name: Library.LoanSupervisor}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end

# ---------------------------------------------------------------------------
# Demonstração
# ---------------------------------------------------------------------------

{:ok, _} = Library.App.start_link()

# Empréstimo curto (expira em 3s) e empréstimo longo (expira em 10s)
:ok = Library.Loans.borrow("L1", "Dom Casmurro", "Ana", 3_000)
:ok = Library.Loans.borrow("L2", "1984", "Bruno", 10_000)

IO.inspect(Library.Loans.status("L1"), label: "L1 após empréstimo")
IO.inspect(Library.Loans.status("L2"), label: "L2 após empréstimo")

# Simula encerramento inesperado do processo do L2
[{pid_antes, _}] = Registry.lookup(Library.LoanRegistry, "L2")
Process.exit(pid_antes, :kill)
Process.sleep(100)

[{pid_depois, _}] = Registry.lookup(Library.LoanRegistry, "L2")
IO.inspect({pid_antes != pid_depois, Library.Loans.status("L2")},
  label: "L2 após queda (reiniciado?, estado)")

# Aguarda a expiração automática do L1
Process.sleep(3_000)
IO.inspect(Library.Loans.status("L1"), label: "L1 após expiração")

# Devolve o L2 normalmente
IO.inspect(Library.Loans.return("L2"), label: "Devolução de L2")
IO.inspect(Library.Loans.status("L2"), label: "L2 final")
