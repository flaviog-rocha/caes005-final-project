# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de troca de mensagens entre usuários, atendendo somente aos seguintes requisitos:
#
# 1. Detectar encerramentos inesperados de processos.
# 2. Impedir atualizações parcialmente concluídas.
# 3. Validar os dados recebidos antes de realizar operações.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - Process
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule MessageExchange do
  use GenServer

  @max_name_bytes 64
  @max_message_bytes 10_000

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, :ok, opts)
  end

  def register(name), do: register(__MODULE__, name)

  def register(server, name) do
    GenServer.call(server, {:register, name})
  end

  def unregister(name), do: unregister(__MODULE__, name)

  def unregister(server, name) do
    GenServer.call(server, {:unregister, name})
  end

  def send_message(from, to, body), do: send_message(__MODULE__, from, to, body)

  def send_message(server, from, to, body) do
    GenServer.call(server, {:send_message, from, to, body})
  end

  def fetch_messages(name), do: fetch_messages(__MODULE__, name)

  def fetch_messages(server, name) do
    GenServer.call(server, {:fetch_messages, name})
  end

  @impl true
  def init(:ok) do
    {:ok, %{users: %{}, refs: %{}, inboxes: %{}}}
  end

  @impl true
  def handle_call({:register, name}, {pid, _tag}, state) do
    with :ok <- validate_name(name) do
      case Map.get(state.users, name) do
        nil ->
          ref = Process.monitor(pid)

          user = %{pid: pid, ref: ref}

          new_state = %{
            state
            | users: Map.put(state.users, name, user),
              refs: Map.put(state.refs, ref, name),
              inboxes: Map.put_new(state.inboxes, name, [])
          }

          {:reply, :ok, new_state}

        %{pid: ^pid} ->
          {:reply, :ok, state}

        _other ->
          {:reply, {:error, :name_taken}, state}
      end
    else
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:unregister, name}, {pid, _tag}, state) do
    with :ok <- validate_name(name),
         {:ok, %{pid: ^pid, ref: ref}} <- fetch_user(state, name) do
      Process.demonitor(ref, [:flush])
      {:reply, :ok, remove_user(state, name, ref)}
    else
      {:error, reason} -> {:reply, {:error, reason}, state}
      {:ok, _user} -> {:reply, {:error, :not_owner}, state}
    end
  end

  def handle_call({:send_message, from, to, body}, {pid, _tag}, state) do
    with :ok <- validate_name(from),
         :ok <- validate_name(to),
         :ok <- validate_body(body),
         {:ok, %{pid: ^pid}} <- fetch_user(state, from),
         {:ok, recipient} <- fetch_user(state, to) do
      id = make_ref()

      message = %{
        id: id,
        from: from,
        to: to,
        body: body,
        sent_at: System.system_time(:millisecond)
      }

      # A single immutable state replacement adds the complete message.
      inboxes = Map.update!(state.inboxes, to, &[message | &1])
      {:reply, {:ok, id}, %{state | inboxes: inboxes}}
    else
      {:error, reason} -> {:reply, {:error, reason}, state}
      {:ok, _user} -> {:reply, {:error, :not_owner}, state}
    end
  end

  def handle_call({:fetch_messages, name}, {pid, _tag}, state) do
    with :ok <- validate_name(name),
         {:ok, %{pid: ^pid}} <- fetch_user(state, name) do
      messages = state.inboxes |> Map.fetch!(name) |> Enum.reverse()
      inboxes = Map.put(state.inboxes, name, [])
      {:reply, {:ok, messages}, %{state | inboxes: inboxes}}
    else
      {:error, reason} -> {:reply, {:error, reason}, state}
      {:ok, _user} -> {:reply, {:error, :not_owner}, state}
    end
  end

  @impl true
  def handle_info({:DOWN, ref, :process, _pid, _reason}, state) do
    case Map.fetch(state.refs, ref) do
      {:ok, name} -> {:noreply, remove_user(state, name, ref)}
      :error -> {:noreply, state}
    end
  end

  def handle_info(_message, state), do: {:noreply, state}

  defp fetch_user(state, name) do
    case Map.fetch(state.users, name) do
      {:ok, user} -> {:ok, user}
      :error -> {:error, :user_not_found}
    end
  end

  defp remove_user(state, name, ref) do
    %{
      state
      | users: Map.delete(state.users, name),
        refs: Map.delete(state.refs, ref),
        inboxes: Map.delete(state.inboxes, name)
    }
  end

  defp validate_name(name)
       when is_binary(name) and byte_size(name) > 0 and byte_size(name) <= @max_name_bytes do
    if String.valid?(name) and String.trim(name) != "" do
      :ok
    else
      {:error, :invalid_name}
    end
  end

  defp validate_name(_name), do: {:error, :invalid_name}

  defp validate_body(body)
       when is_binary(body) and byte_size(body) > 0 and byte_size(body) <= @max_message_bytes do
    if String.valid?(body), do: :ok, else: {:error, :invalid_message}
  end

  defp validate_body(_body), do: {:error, :invalid_message}
end
