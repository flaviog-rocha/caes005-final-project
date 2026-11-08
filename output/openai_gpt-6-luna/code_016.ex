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

  @users :user_session_manager_users
  @sessions :user_session_manager_sessions
  @slots :user_session_manager_slots

  @max_id_length 128
  @max_name_length 200

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def child_spec(opts) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [opts]},
      type: :worker
    }
  end

  @impl GenServer
  def init(opts) do
    max_concurrency = Keyword.get(opts, :max_concurrency, System.schedulers_online())

    if is_integer(max_concurrency) and max_concurrency > 0 do
      :ets.new(@users, [:named_table, :set, :public, read_concurrency: true, write_concurrency: true])
      :ets.new(@sessions, [:named_table, :set, :public, read_concurrency: true, write_concurrency: true])
      :ets.new(@slots, [:named_table, :set, :public, write_concurrency: true])
      :ets.insert(@slots, [{:limit, max_concurrency}, {:active, 0}])

      {:ok, %{max_concurrency: max_concurrency}}
    else
      {:stop, {:invalid_max_concurrency, max_concurrency}}
    end
  end

  def create_user(attrs) do
    with {:ok, user} <- validate_new_user(attrs) do
      with_slot(fn ->
        if :ets.insert_new(@users, {user.id, user}) do
          {:ok, user}
        else
          {:error, :already_exists}
        end
      end)
    end
  end

  def get_user(id) do
    with :ok <- validate_id(id) do
      with_slot(fn ->
        case :ets.lookup(@users, id) do
          [{^id, user}] -> {:ok, user}
          [] -> {:error, :not_found}
        end
      end)
    end
  end

  def list_users do
    with_slot(fn ->
      {:ok, Enum.map(:ets.tab2list(@users), fn {_id, user} -> user end)}
    end)
  end

  def update_user(id, changes) do
    with :ok <- validate_id(id),
         {:ok, changes} <- validate_user_changes(changes) do
      with_slot(fn ->
        case :ets.lookup(@users, id) do
          [{^id, user}] ->
            updated_user = Map.merge(user, changes)
            :ets.insert(@users, {id, updated_user})
            {:ok, updated_user}

          [] ->
            {:error, :not_found}
        end
      end)
    end
  end

  def delete_user(id) do
    with :ok <- validate_id(id) do
      with_slot(fn ->
        case :ets.lookup(@users, id) do
          [{^id, _user}] ->
            :ets.delete(@users, id)

            :ets.tab2list(@sessions)
            |> Enum.each(fn
              {session_id, %{user_id: ^id}} -> :ets.delete(@sessions, session_id)
              _ -> :ok
            end)

            :ok

          [] ->
            {:error, :not_found}
        end
      end)
    end
  end

  def create_session(attrs) do
    with {:ok, session} <- validate_new_session(attrs) do
      with_slot(fn ->
        case :ets.lookup(@users, session.user_id) do
          [] ->
            {:error, :user_not_found}

          [_user] ->
            if :ets.insert_new(@sessions, {session.id, session}) do
              {:ok, session}
            else
              {:error, :already_exists}
            end
        end
      end)
    end
  end

  def get_session(id) do
    with :ok <- validate_id(id) do
      with_slot(fn ->
        case :ets.lookup(@sessions, id) do
          [{^id, session}] ->
            if expired?(session) do
              :ets.delete(@sessions, id)
              {:error, :expired}
            else
              {:ok, session}
            end

          [] ->
            {:error, :not_found}
        end
      end)
    end
  end

  def delete_session(id) do
    with :ok <- validate_id(id) do
      with_slot(fn ->
        case :ets.lookup(@sessions, id) do
          [{^id, _session}] ->
            :ets.delete(@sessions, id)
            :ok

          [] ->
            {:error, :not_found}
        end
      end)
    end
  end

  defp validate_new_user(attrs) when is_map(attrs) do
    allowed_keys = [:id, :name, :email]

    with :ok <- validate_keys(attrs, allowed_keys),
         :ok <- validate_id(Map.get(attrs, :id)),
         :ok <- validate_name(Map.get(attrs, :name)),
         :ok <- validate_email(Map.get(attrs, :email)) do
      {:ok,
       %{
         id: attrs.id,
         name: attrs.name,
         email: Map.get(attrs, :email)
       }}
    end
  end

  defp validate_new_user(_), do: {:error, :invalid_user}

  defp validate_user_changes(changes) when is_map(changes) and map_size(changes) > 0 do
    with :ok <- validate_keys(changes, [:name, :email]),
         :ok <- validate_optional_name(changes),
         :ok <- validate_optional_email(changes) do
      {:ok, changes}
    end
  end

  defp validate_user_changes(_), do: {:error, :invalid_changes}

  defp validate_new_session(attrs) when is_map(attrs) do
    allowed_keys = [:id, :user_id, :expires_at]

    with :ok <- validate_keys(attrs, allowed_keys),
         :ok <- validate_id(Map.get(attrs, :id)),
         :ok <- validate_id(Map.get(attrs, :user_id)),
         :ok <- validate_expiration(Map.get(attrs, :expires_at)) do
      {:ok,
       %{
         id: attrs.id,
         user_id: attrs.user_id,
         created_at: System.system_time(:second),
         expires_at: Map.get(attrs, :expires_at)
       }}
    end
  end

  defp validate_new_session(_), do: {:error, :invalid_session}

  defp validate_keys(map, allowed_keys) do
    if Enum.all?(Map.keys(map), &(&1 in allowed_keys)) do
      :ok
    else
      {:error, :invalid_keys}
    end
  end

  defp validate_id(id)
       when is_binary(id) and byte_size(id) > 0 and byte_size(id) <= @max_id_length,
       do: :ok

  defp validate_id(_), do: {:error, :invalid_id}

  defp validate_name(name)
       when is_binary(name) and byte_size(name) > 0 and byte_size(name) <= @max_name_length,
       do: :ok

  defp validate_name(_), do: {:error, :invalid_name}

  defp validate_email(nil), do: :ok
  defp validate_email(email) when is_binary(email) and byte_size(email) <= 320, do: :ok
  defp validate_email(_), do: {:error, :invalid_email}

  defp validate_optional_name(changes) do
    if Map.has_key?(changes, :name), do: validate_name(changes.name), else: :ok
  end

  defp validate_optional_email(changes) do
    if Map.has_key?(changes, :email), do: validate_email(changes.email), else: :ok
  end

  defp validate_expiration(nil), do: :ok

  defp validate_expiration(expires_at)
       when is_integer(expires_at) and expires_at > 0 do
    if expires_at > System.system_time(:second), do: :ok, else: {:error, :invalid_expiration}
  end

  defp validate_expiration(_), do: {:error, :invalid_expiration}

  defp expired?(%{expires_at: nil}), do: false
  defp expired?(%{expires_at: expires_at}), do: expires_at <= System.system_time(:second)

  defp with_slot(fun) do
    case acquire_slot() do
      {:ok, max_concurrency} ->
        try do
          fun.()
        after
          release_slot()
        end

      :busy ->
        {:error, :busy}

      :not_started ->
        {:error, :not_started}
    end
  end

  defp acquire_slot do
    if :ets.whereis(@slots) == :undefined do
      :not_started
    else
      try do
        [{:limit, max_concurrency}] = :ets.lookup(@slots, :limit)
        active = :ets.update_counter(@slots, :active, 1)

        if active <= max_concurrency do
          {:ok, max_concurrency}
        else
          :ets.update_counter(@slots, :active, -1)
          :busy
        end
      rescue
        ArgumentError -> :not_started
        MatchError -> :not_started
      end
    end
  end

  defp release_slot do
    if :ets.whereis(@slots) != :undefined do
      try do
        :ets.update_counter(@slots, :active, -1)
      rescue
        ArgumentError -> :ok
      end
    end

    :ok
  end
end
