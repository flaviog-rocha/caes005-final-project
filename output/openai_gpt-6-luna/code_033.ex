# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de troca de mensagens entre usuários, atendendo somente aos seguintes requisitos:
#
# 1. Fazer uso de macros.
# 2. Permitir comunicação entre diferentes processos.
# 3. Garantir a consistência dos dados durante operações concorrentes.
# 4. Persistir os dados para recuperação após reinicializações.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - Task
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule DurableMessaging.API do
  defmacro __using__(_opts) do
    quote do
      def start_link(path) when is_binary(path) do
        GenServer.start_link(__MODULE__, path, name: __MODULE__)
      end

      def send_message(from, to, body) do
        task =
          Task.async(fn ->
            GenServer.call(__MODULE__, {:send_message, from, to, body}, :infinity)
          end)

        Task.await(task, :infinity)
      end

      def messages_for(user) do
        task =
          Task.async(fn ->
            GenServer.call(__MODULE__, {:messages_for, user}, :infinity)
          end)

        Task.await(task, :infinity)
      end

      def all_messages do
        task =
          Task.async(fn ->
            GenServer.call(__MODULE__, :all_messages, :infinity)
          end)

        Task.await(task, :infinity)
      end
    end
  end
end

defmodule DurableMessaging do
  use GenServer
  use DurableMessaging.API

  @impl true
  def init(path) do
    with :ok <- File.mkdir_p(Path.dirname(path)),
         :ok <- ensure_file(path),
         {:ok, data} <- File.read(path),
         {:ok, records} <- recover_file(path, data),
         {:ok, fd} <- :file.open(String.to_charlist(path), [:append, :write, :binary, :raw]) do
      {:ok, %{fd: fd, messages: records, next_id: length(records) + 1}}
    else
      {:error, reason} -> {:stop, reason}
    end
  end

  @impl true
  def handle_call({:send_message, from, to, body}, _caller, state)
      when is_binary(from) and byte_size(from) > 0 and is_binary(to) and byte_size(to) > 0 and
             is_binary(body) do
    id = state.next_id
    payload = :erlang.term_to_binary({id, from, to, body}, [:compressed])

    if byte_size(payload) > 4_294_967_295 do
      {:reply, {:error, :message_too_large}, state}
    else
      frame = <<byte_size(payload)::unsigned-big-32, payload::binary>>

      case persist(state.fd, frame) do
        :ok ->
          record = {id, from, to, body}
          updated_state = %{state | messages: state.messages ++ [record], next_id: id + 1}
          {:reply, {:ok, present(record)}, updated_state}

        {:error, reason} ->
          {:stop, {:persistence_error, reason}, {:error, {:persistence, reason}}, state}
      end
    end
  end

  def handle_call({:send_message, _from, _to, _body}, _caller, state) do
    {:reply, {:error, :invalid_message}, state}
  end

  def handle_call({:messages_for, user}, _caller, state) when is_binary(user) do
    messages =
      state.messages
      |> Enum.filter(fn {_id, _from, to, _body} -> to == user end)
      |> Enum.map(&present/1)

    {:reply, messages, state}
  end

  def handle_call(:all_messages, _caller, state) do
    {:reply, Enum.map(state.messages, &present/1), state}
  end

  def handle_call(_request, _caller, state) do
    {:reply, {:error, :unknown_request}, state}
  end

  @impl true
  def terminate(_reason, state) do
    :file.close(state.fd)
    :ok
  end

  defp ensure_file(path) do
    case File.open(path, [:append, :binary]) do
      {:ok, file} -> File.close(file)
      {:error, reason} -> {:error, reason}
    end
  end

  defp recover_file(path, data) do
    {records, valid_size} = decode_records(data, [], 0, 1)

    if valid_size == byte_size(data) do
      {:ok, records}
    else
      truncate_file(path, valid_size)
      |> case do
        :ok -> {:ok, records}
        {:error, reason} -> {:error, reason}
      end
    end
  end

  defp decode_records(<<>>, records, offset, _expected_id) do
    {Enum.reverse(records), offset}
  end

  defp decode_records(<<length::unsigned-big-32, rest::binary>>, records, offset, expected_id)
       when byte_size(rest) >= length do
    <<payload::binary-size(length), tail::binary>> = rest

    case decode_record(payload, expected_id) do
      {:ok, record} ->
        decode_records(tail, [record | records], offset + 4 + length, expected_id + 1)

      :error ->
        {Enum.reverse(records), offset}
    end
  end

  defp decode_records(_incomplete_data, records, offset, _expected_id) do
    {Enum.reverse(records), offset}
  end

  defp decode_record(payload, expected_id) do
    case :erlang.binary_to_term(payload, [:safe]) do
      {^expected_id, from, to, body}
      when is_binary(from) and is_binary(to) and is_binary(body) ->
        {:ok, {expected_id, from, to, body}}

      _ ->
        :error
    end
  rescue
    _ -> :error
  catch
    _, _ -> :error
  end

  defp truncate_file(path, valid_size) do
    case :file.open(String.to_charlist(path), [:read, :write, :binary, :raw]) do
      {:ok, fd} ->
        result =
          with {:ok, _position} <- :file.position(fd, valid_size),
               :ok <- :file.truncate(fd),
               :ok <- :file.sync(fd) do
            :ok
          end

        :file.close(fd)
        result

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp persist(fd, frame) do
    with :ok <- :file.write(fd, frame),
         :ok <- :file.sync(fd) do
      :ok
    end
  end

  defp present({id, from, to, body}) do
    %{id: id, from: from, to: to, body: body}
  end
end
