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

defmodule BookLoans.Loan do
  @moduledoc false

  defstruct [:id, :borrower, :book, :expires_at]
end

defmodule BookLoans.Store do
  @moduledoc false
  use GenServer

  # Armazenamento em memória que representa um banco de dados fictício.
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, %{}, Keyword.put_new(opts, :name, __MODULE__))
  end

  def insert_new(record), do: GenServer.call(__MODULE__, {:insert_new, record})
  def fetch(id), do: GenServer.call(__MODULE__, {:fetch, id})
  def delete(id), do: GenServer.call(__MODULE__, {:delete, id})
  def all, do: GenServer.call(__MODULE__, :all)

  @impl true
  def init(records), do: {:ok, records}

  @impl true
  def handle_call({:insert_new, record}, _from, records) do
    case Map.fetch(records, record.id) do
      :error ->
        {:reply, :ok, Map.put(records, record.id, record)}

      {:ok, existing} ->
        if existing.expires_at <= System.system_time(:millisecond) do
          {:reply, :ok, Map.put(records, record.id, record)}
        else
          {:reply, {:error, :already_exists}, records}
        end
    end
  end

  def handle_call({:fetch, id}, _from, records) do
    {:reply, Map.fetch(records, id), records}
  end

  def handle_call({:delete, id}, _from, records) do
    {:reply, :ok, Map.delete(records, id)}
  end

  def handle_call(:all, _from, records) do
    now = System.system_time(:millisecond)

    active =
      records
      |> Map.values()
      |> Enum.filter(&(&1.expires_at > now))

    {:reply, active, records}
  end
end

defmodule BookLoans.LoanWorker do
  @moduledoc false
  use GenServer

  @max_timer_delay 4_294_967_000

  def start_link(id) do
    GenServer.start_link(
      __MODULE__,
      id,
      name: {:via, Registry, {BookLoans.Registry, id}}
    )
  end

  @impl true
  def init(id) do
    case BookLoans.Store.fetch(id) do
      {:ok, loan} ->
        schedule_expiration(loan.expires_at)
        {:ok, loan}

      :error ->
        :ignore
    end
  end

  @impl true
  def handle_call(:get, _from, loan) do
    if expired?(loan) do
      BookLoans.Store.delete(loan.id)
      {:stop, :normal, {:error, :expired}, loan}
    else
      {:reply, {:ok, loan}, loan}
    end
  end

  def handle_call(:return, _from, loan) do
    if expired?(loan) do
      BookLoans.Store.delete(loan.id)
      {:stop, :normal, {:error, :expired}, loan}
    else
      BookLoans.Store.delete(loan.id)
      {:stop, :normal, :ok, loan}
    end
  end

  @impl true
  def handle_info(:expire, loan) do
    if expired?(loan) do
      BookLoans.Store.delete(loan.id)
      {:stop, :normal, loan}
    else
      schedule_expiration(loan.expires_at)
      {:noreply, loan}
    end
  end

  defp expired?(loan), do: loan.expires_at <= System.system_time(:millisecond)

  defp schedule_expiration(expires_at) do
    remaining = max(expires_at - System.system_time(:millisecond), 0)
    Process.send_after(self(), :expire, min(remaining, @max_timer_delay))
  end
end

defmodule BookLoans.Manager do
  @moduledoc false
  use GenServer
  require Logger

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, %{}, Keyword.put_new(opts, :name, __MODULE__))
  end

  def borrow(id, borrower, book, ttl_ms) do
    GenServer.call(__MODULE__, {:borrow, id, borrower, book, ttl_ms})
  end

  def get(id), do: GenServer.call(__MODULE__, {:get, id})
  def return(id), do: GenServer.call(__MODULE__, {:return, id})
  def list, do: BookLoans.Store.all()

  @impl true
  def init(state), do: {:ok, state}

  @impl true
  def handle_call({:borrow, id, borrower, book, ttl_ms}, _from, monitors) do
    if is_integer(ttl_ms) and ttl_ms > 0 do
      loan = %BookLoans.Loan{
        id: id,
        borrower: borrower,
        book: book,
        expires_at: System.system_time(:millisecond) + ttl_ms
      }

      case BookLoans.Store.insert_new(loan) do
        :ok ->
          case DynamicSupervisor.start_child(
                 BookLoans.LoanSupervisor,
                 {BookLoans.LoanWorker, id}
               ) do
            {:ok, pid} ->
              {:reply, {:ok, loan}, monitor_loan(id, pid, monitors)}

            {:error, reason} ->
              BookLoans.Store.delete(id)
              {:reply, {:error, reason}, monitors}
          end

        {:error, :already_exists} ->
          {:reply, {:error, :already_exists}, monitors}
      end
    else
      {:reply, {:error, :invalid_ttl}, monitors}
    end
  end

  def handle_call({:get, id}, _from, monitors) do
    reply =
      case Registry.lookup(BookLoans.Registry, id) do
        [{pid, _}] -> safe_worker_call(pid, :get)
        [] -> BookLoans.Store.fetch(id)
      end

    {:reply, reply, monitors}
  end

  def handle_call({:return, id}, _from, monitors) do
    reply =
      case Registry.lookup(BookLoans.Registry, id) do
        [{pid, _}] -> safe_worker_call(pid, :return)
        [] -> {:error, :not_found}
      end

    {:reply, reply, monitors}
  end

  @impl true
  def handle_info({:DOWN, ref, :process, _pid, reason}, monitors) do
    case Enum.find(monitors, fn {_id, monitor_ref} -> monitor_ref == ref end) do
      {id, ^ref} ->
        if unexpected?(reason) do
          Logger.error("Processo do empréstimo #{inspect(id)} encerrou inesperadamente: #{inspect(reason)}")
        end

        Process.send_after(self(), {:monitor_again, id}, 25)
        {:noreply, Map.delete(monitors, id)}

      nil ->
        {:noreply, monitors}
    end
  end

  def handle_info({:monitor_again, id}, monitors) do
    cond do
      Map.has_key?(monitors, id) ->
        {:noreply, monitors}

      match?({:ok, _}, BookLoans.Store.fetch(id)) ->
        case Registry.lookup(BookLoans.Registry, id) do
          [{pid, _}] ->
            {:noreply, monitor_loan(id, pid, monitors)}

          [] ->
            Process.send_after(self(), {:monitor_again, id}, 100)
            {:noreply, monitors}
        end

      true ->
        {:noreply, monitors}
    end
  end

  defp monitor_loan(id, pid, monitors) do
    Map.put(monitors, id, Process.monitor(pid))
  end

  defp safe_worker_call(pid, message) do
    try do
      GenServer.call(pid, message)
    catch
      :exit, _reason -> {:error, :not_found}
    end
  end

  defp unexpected?(:normal), do: false
  defp unexpected?(:shutdown), do: false
  defp unexpected?({:shutdown, _reason}), do: false
  defp unexpected?(_reason), do: true
end

defmodule BookLoans do
  @moduledoc """
  Gerenciamento de empréstimos independentes, com expiração automática.
  """

  def start_link(opts \\ []) do
    children = [
      {Registry, keys: :unique, name: BookLoans.Registry},
      BookLoans.Store,
      {DynamicSupervisor, strategy: :one_for_one, name: BookLoans.LoanSupervisor},
      BookLoans.Manager
    ]

    Supervisor.start_link(children, strategy: :one_for_one, name: BookLoans.Supervisor)
  end

  def borrow(id, borrower, book, ttl_ms \\ 86_400_000) do
    BookLoans.Manager.borrow(id, borrower, book, ttl_ms)
  end

  def get(id), do: BookLoans.Manager.get(id)
  def return(id), do: BookLoans.Manager.return(id)
  def list, do: BookLoans.Manager.list()
end
