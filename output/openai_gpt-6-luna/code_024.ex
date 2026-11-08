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

defmodule LibraryLoans do
  @moduledoc """
  Gerencia empréstimos em processos independentes por entidade.
  """

  @supervisor LibraryLoans.Supervisor
  @manager LibraryLoans.Manager

  def start_link(opts \\ []) do
    children = [
      {DynamicSupervisor,
       strategy: :one_for_one, name: LibraryLoans.DynamicSupervisor},
      {LibraryLoans.Manager, name: @manager}
    ]

    Supervisor.start_link(children, strategy: :one_for_one, name: @supervisor, **opts)
  end

  @doc """
  Registra um empréstimo. `ttl_ms` define por quanto tempo o registro permanece ativo.
  """
  def checkout(entity, loan_id, book, borrower, ttl_ms)
      when is_integer(ttl_ms) and ttl_ms > 0 do
    call_entity(entity, {:checkout, loan_id, book, borrower, ttl_ms})
  end

  def checkout(_entity, _loan_id, _book, _borrower, _ttl_ms),
    do: {:error, :invalid_ttl}

  def return_book(entity, loan_id),
    do: call_entity(entity, {:return_book, loan_id})

  def loan(entity, loan_id),
    do: call_entity(entity, {:loan, loan_id})

  def loans(entity),
    do: call_entity(entity, :loans)

  @doc """
  Retorna os encerramentos inesperados observados pelo gerenciador.
  """
  def crashes do
    GenServer.call(@manager, :crashes)
  end

  defp call_entity(entity, message, attempts \\ 1) do
    case GenServer.call(@manager, {:ensure_entity, entity}) do
      {:ok, pid} ->
        try do
          GenServer.call(pid, message)
        catch
          :exit, reason when attempts > 0 ->
            call_entity(entity, message, attempts - 1)

          :exit, reason ->
            {:error, {:process_unavailable, reason}}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end
end

defmodule LibraryLoans.Manager do
  @moduledoc false
  use GenServer

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: Keyword.fetch!(opts, :name))
  end

  @impl true
  def init(_opts) do
    {:ok, %{workers: %{}, crashes: []}}
  end

  @impl true
  def handle_call({:ensure_entity, entity}, _from, state) do
    case Map.get(state.workers, entity) do
      {pid, _ref} when is_pid(pid) ->
        if Process.alive?(pid) do
          {:reply, {:ok, pid}, state}
        else
          start_entity(entity, state)
        end

      nil ->
        start_entity(entity, state)
    end
  end

  def handle_call(:crashes, _from, state) do
    {:reply, Enum.reverse(state.crashes), state}
  end

  @impl true
  def handle_info({:DOWN, ref, :process, _pid, reason}, state) do
    {entity, workers} =
      Enum.reduce(state.workers, {nil, state.workers}, fn
        {entity, {_pid, ^ref}}, {_found, workers} ->
          {entity, Map.delete(workers, entity)}

        _, acc ->
          acc
      end)

    crashes =
      if entity != nil and unexpected?(reason) do
        [%{entity: entity, reason: reason, at: DateTime.utc_now()} | state.crashes]
      else
        state.crashes
      end

    {:noreply, %{state | workers: workers, crashes: crashes}}
  end

  defp start_entity(entity, state) do
    spec = %{
      id: {LibraryLoans.Entity, make_ref()},
      start: {LibraryLoans.Entity, :start_link, [entity]},
      restart: :temporary,
      shutdown: 5_000,
      type: :worker
    }

    case DynamicSupervisor.start_child(LibraryLoans.DynamicSupervisor, spec) do
      {:ok, pid} ->
        ref = Process.monitor(pid)
        workers = Map.put(state.workers, entity, {pid, ref})
        {:reply, {:ok, pid}, %{state | workers: workers}}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  defp unexpected?(:normal), do: false
  defp unexpected?(:shutdown), do: false
  defp unexpected?({:shutdown, _reason}), do: false
  defp unexpected?(_reason), do: true
end

defmodule LibraryLoans.Entity do
  @moduledoc false
  use GenServer

  def start_link(entity) do
    GenServer.start_link(__MODULE__, entity)
  end

  @impl true
  def init(entity) do
    # Cada entidade mantém seu próprio estado e seus próprios temporizadores.
    # Uma integração com banco de dados pode ser adicionada aqui, se necessário.
    {:ok, %{entity: entity, loans: %{}}}
  end

  @impl true
  def handle_call({:checkout, loan_id, book, borrower, ttl_ms}, _from, state) do
    if Map.has_key?(state.loans, loan_id) do
      {:reply, {:error, :already_exists}, state}
    else
      timer = Process.send_after(self(), {:expire, loan_id, make_ref()}, ttl_ms)
      {:messages, messages} = Process.info(self(), :messages)
      token = find_expiration_token(messages, loan_id, timer)

      now = DateTime.utc_now()

      record = %{
        loan_id: loan_id,
        book: book,
        borrower: borrower,
        created_at: now,
        expires_at: DateTime.add(now, ttl_ms, :millisecond),
        timer: timer,
        token: token
      }

      loans = Map.put(state.loans, loan_id, record)
      {:reply, {:ok, public_record(record)}, %{state | loans: loans}}
    end
  end

  def handle_call({:return_book, loan_id}, _from, state) do
    case Map.pop(state.loans, loan_id) do
      {nil, _loans} ->
        {:reply, {:error, :not_found}, state}

      {record, loans} ->
        Process.cancel_timer(record.timer)
        {:reply, {:ok, public_record(record)}, %{state | loans: loans}}
    end
  end

  def handle_call({:loan, loan_id}, _from, state) do
    case Map.fetch(state.loans, loan_id) do
      {:ok, record} -> {:reply, {:ok, public_record(record)}, state}
      :error -> {:reply, {:error, :not_found}, state}
    end
  end

  def handle_call(:loans, _from, state) do
    records = state.loans |> Map.values() |> Enum.map(&public_record/1)
    {:reply, records, state}
  end

  @impl true
  def handle_info({:expire, loan_id, token}, state) do
    case Map.get(state.loans, loan_id) do
      %{token: ^token} ->
        {:noreply, %{state | loans: Map.delete(state.loans, loan_id)}}

      _ ->
        {:noreply, state}
    end
  end

  defp public_record(record) do
    Map.drop(record, [:timer, :token])
  end

  # Obtém o token associado à mensagem agendada para rejeitar mensagens antigas.
  defp find_expiration_token(messages, loan_id, _timer) do
    case Enum.find(messages, fn
           {:expire, ^loan_id, token} -> is_reference(token)
           _ -> false
         end) do
      {:expire, ^loan_id, token} -> token
      nil -> make_ref()
    end
  end
end
