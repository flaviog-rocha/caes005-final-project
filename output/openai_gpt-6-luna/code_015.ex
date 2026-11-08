# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de usuários e sessões, atendendo somente aos seguintes requisitos:
#
# 1. Validar os dados recebidos antes de realizar operações.
# 2. Permitir comunicação entre diferentes processos.
# 3. Permitir limitar a quantidade de operações simultâneas.
# 4. Permitir a execução de operações simultâneas.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - ETS
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule UserSessionManager do
  use GenServer

  @table :user_session_manager_data

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def create_user(user_id, attributes \\ %{}) do
    with :ok <- validate_id(user_id, :invalid_user_id),
         :ok <- validate_attributes(attributes) do
      request({:create_user, user_id, attributes})
    end
  end

  def get_user(user_id) do
    with :ok <- validate_id(user_id, :invalid_user_id) do
      request({:get_user, user_id})
    end
  end

  def delete_user(user_id) do
    with :ok <- validate_id(user_id, :invalid_user_id) do
      request({:delete_user, user_id})
    end
  end

  def create_session(session_id, user_id, attributes \\ %{}) do
    with :ok <- validate_id(session_id, :invalid_session_id),
         :ok <- validate_id(user_id, :invalid_user_id),
         :ok <- validate_attributes(attributes) do
      request({:create_session, session_id, user_id, attributes})
    end
  end

  def get_session(session_id) do
    with :ok <- validate_id(session_id, :invalid_session_id) do
      request({:get_session, session_id})
    end
  end

  def delete_session(session_id) do
    with :ok <- validate_id(session_id, :invalid_session_id) do
      request({:delete_session, session_id})
    end
  end

  def sessions_for_user(user_id) do
    with :ok <- validate_id(user_id, :invalid_user_id) do
      request({:sessions_for_user, user_id})
    end
  end

  @impl true
  def init(opts) do
    max_concurrency = Keyword.get(opts, :max_concurrency, System.schedulers_online())

    unless is_integer(max_concurrency) and max_concurrency > 0 do
      raise ArgumentError, "max_concurrency deve ser um inteiro positivo"
    end

    :ets.new(@table, [
      :named_table,
      :set,
      :public,
      read_concurrency: true,
      write_concurrency: true
    ])

    :ets.insert(@table, {:active_operations, 0})

    {:ok, %{max_concurrency: max_concurrency}}
  end

  @impl true
  def handle_call({:execute, operation}, from, state) do
    spawn(fn ->
      result =
        with_limit(state.max_concurrency, fn ->
          execute(operation)
        end)

      GenServer.reply(from, result)
    end)

    {:noreply, state}
  end

  defp request(operation) do
    GenServer.call(__MODULE__, {:execute, operation}, :infinity)
  end

  defp validate_id(value, _error)
       when is_binary(value) and byte_size(value) > 0,
       do: :ok

  defp validate_id(_value, error), do: {:error, error}

  defp validate_attributes(attributes) when is_map(attributes), do: :ok
  defp validate_attributes(_attributes), do: {:error, :invalid_attributes}

  defp with_limit(max_concurrency, operation) do
    acquire_slot(max_concurrency)

    try do
      operation.()
    after
      :ets.update_counter(@table, :active_operations, {2, -1})
    end
  end

  defp acquire_slot(max_concurrency) do
    active = :ets.update_counter(@table, :active_operations, {2, 1})

    if active <= max_concurrency do
      :ok
    else
      :ets.update_counter(@table, :active_operations, {2, -1})
      Process.sleep(2)
      acquire_slot(max_concurrency)
    end
  end

  defp execute({:create_user, user_id, attributes}) do
    user = %{
      id: user_id,
      attributes: attributes,
      created_at: System.system_time(:second)
    }

    if :ets.insert_new(@table, {{:user, user_id}, user}) do
      {:ok, user}
    else
      {:error, :already_exists}
    end
  end

  defp execute({:get_user, user_id}) do
    case :ets.lookup(@table, {:user, user_id}) do
      [{{:user, ^user_id}, user}] -> {:ok, user}
      [] -> {:error, :not_found}
    end
  end

  defp execute({:delete_user, user_id}) do
    case :ets.lookup(@table, {:user, user_id}) do
      [{{:user, ^user_id}, user}] ->
        :ets.delete(@table, {:user, user_id})

        :ets.tab2list(@table)
        |> Enum.each(fn
          {{:session, session_id}, ^user_id, _session} ->
            :ets.delete(@table, {:session, session_id})

          _other ->
            :ok
        end)

        {:ok, user}

      [] ->
        {:error, :not_found}
    end
  end

  defp execute({:create_session, session_id, user_id, attributes}) do
    case :ets.lookup(@table, {:user, user_id}) do
      [] ->
        {:error, :user_not_found}

      [_user] ->
        session = %{
          id: session_id,
          user_id: user_id,
          attributes: attributes,
          created_at: System.system_time(:second)
        }

        if :ets.insert_new(@table, {{:session, session_id}, user_id, session}) do
          {:ok, session}
        else
          {:error, :already_exists}
        end
    end
  end

  defp execute({:get_session, session_id}) do
    case :ets.lookup(@table, {:session, session_id}) do
      [{{:session, ^session_id}, _user_id, session}] -> {:ok, session}
      [] -> {:error, :not_found}
    end
  end

  defp execute({:delete_session, session_id}) do
    case :ets.lookup(@table, {:session, session_id}) do
      [{{:session, ^session_id}, _user_id, session}] ->
        :ets.delete(@table, {:session, session_id})
        {:ok, session}

      [] ->
        {:error, :not_found}
    end
  end

  defp execute({:sessions_for_user, user_id}) do
    sessions =
      :ets.tab2list(@table)
      |> Enum.flat_map(fn
        {{:session, _session_id}, ^user_id, session} -> [session]
        _other -> []
      end)

    {:ok, sessions}
  end
end
