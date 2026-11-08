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

# ============================================================================
# Sistema de troca de mensagens entre usuários com OTP/GenServer,
# macros de DSL, comunicação entre processos, consistência e persistência.
# ============================================================================

defmodule MessageSystem.DSL do
  @moduledoc false

  # Macro `defoperation`: define uma operação atômica do sistema.
  #
  # Gera:
  #   * uma função pública (API cliente) que envia a requisição ao servidor;
  #   * uma função de implementação `op_<nome>(args..., state)` executada
  #     dentro do processo do servidor (serializando as escritas).
  #
  # O corpo deve retornar `{:ok, resposta, novo_estado}` ou `{:error, motivo}`.
  # A variável `state` fica disponível no corpo da operação.
  defmacro defoperation({name, _meta, args}, do: body) do
    args = args || []
    impl = String.to_atom("op_#{name}")

    quote do
      def unquote(name)(unquote_splicing(args)) do
        GenServer.call(__MODULE__, {unquote(impl), [unquote_splicing(args)]})
      end

      def unquote(impl)(unquote_splicing(args), var!(state)) do
        unquote(body)
      end
    end
  end
end

defmodule MessageSystem do
  @moduledoc """
  Sistema de mensagens entre usuários.

  * Comunicação entre processos: clientes chamam o servidor via `GenServer.call`
    e recebem notificações assíncronas (`{MessageSystem, {:new_message, msg}}`)
    como processos inscritos.
  * Consistência: todas as escritas são serializadas pelo processo do servidor.
  * Persistência: o estado é gravado após cada alteração em arquivo temporário
    seguido de `File.rename/2` (escrita atômica) e recarregado na inicialização.
  * Concorrência em lote: `Task.async_stream/3` e `Task.async/1`.
  """

  use GenServer
  import MessageSystem.DSL

  @default_path "message_system.dat"
  @format_version 1

  # ---------------------------------------------------------------------------
  # Ciclo de vida
  # ---------------------------------------------------------------------------

  @doc "Inicia o servidor. Opções: `:path` (arquivo de persistência)."
  def start_link(opts \\ []) do
    path = Keyword.get(opts, :path, @default_path)
    GenServer.start_link(__MODULE__, path, name: __MODULE__)
  end

  @impl true
  def init(path) do
    {:ok, Map.merge(%{path: path, subscribers: %{}}, load!(path))}
  end

  # ---------------------------------------------------------------------------
  # Operações (definidas com a macro)
  # ---------------------------------------------------------------------------

  defoperation send_message(from, to, text) do
    if not is_binary(text) or text == "" do
      {:error, :invalid_body}
    else
      msg = %{
        id: state.next_id,
        from: from,
        to: to,
        body: text,
        sent_at: System.system_time(:millisecond),
        read?: false
      }

      new_state = %{
        state
        | next_id: state.next_id + 1,
          inboxes: Map.update(state.inboxes, to, [msg], &[msg | &1])
      }

      notify(state, to, {:new_message, msg})
      {:ok, msg, new_state}
    end
  end

  defoperation mark_read(user, id) do
    msgs = Map.get(state.inboxes, user, [])

    if Enum.any?(msgs, &(&1.id == id)) do
      updated = Enum.map(msgs, fn m -> if m.id == id, do: %{m | read?: true}, else: m end)
      {:ok, :ok, %{state | inboxes: Map.put(state.inboxes, user, updated)}}
    else
      {:error, :not_found}
    end
  end

  defoperation inbox(user) do
    {:ok, Map.get(state.inboxes, user, []), state}
  end

  defoperation subscribe(user, pid) do
    # Monitora o processo para remover a inscrição se ele terminar.
    Process.monitor(pid)
    subs = Map.update(state.subscribers, user, MapSet.new([pid]), &MapSet.put(&1, pid))
    {:ok, :ok, %{state | subscribers: subs}}
  end

  # ---------------------------------------------------------------------------
  # Despacho e callbacks do GenServer
  # ---------------------------------------------------------------------------

  @impl true
  def handle_call({fun, args}, _from, state) do
    case apply(__MODULE__, fun, args ++ [state]) do
      {:ok, reply, new_state} ->
        # Persiste antes de responder: a resposta só sai após o dado estar em disco.
        if new_state != state, do: persist!(new_state)
        {:reply, reply, new_state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_info({:DOWN, _ref, :process, pid, _reason}, state) do
    subs = Map.new(state.subscribers, fn {u, set} -> {u, MapSet.delete(set, pid)} end)
    {:noreply, %{state | subscribers: subs}}
  end

  # ---------------------------------------------------------------------------
  # Funções de conveniência (concorrência com Task)
  # ---------------------------------------------------------------------------

  @doc "Envia a mesma mensagem a vários destinatários concorrentemente."
  def broadcast(from, recipients, text) do
    recipients
    |> Task.async_stream(&send_message(from, &1, text), max_concurrency: 8, timeout: 5_000)
    |> Enum.map(fn {:ok, result} -> result end)
  end

  @doc "Consulta as caixas de entrada de vários usuários concorrentemente."
  def inboxes(users) do
    users
    |> Task.async_stream(&{&1, inbox(&1)}, max_concurrency: 8, timeout: 5_000)
    |> Enum.map(fn {:ok, result} -> result end)
    |> Map.new()
  end

  # ---------------------------------------------------------------------------
  # Persistência
  # ---------------------------------------------------------------------------

  @doc false
  def persist!(%{path: path, next_id: next_id, inboxes: inboxes}) do
    bin =
      :erlang.term_to_binary(%{
        version: @format_version,
        next_id: next_id,
        inboxes: inboxes
      })

    tmp = path <> ".tmp"

    File.open!(tmp, [:write, :binary], fn io ->
      IO.binwrite(io, bin)
      # Garante que os bytes estão no disco antes da troca atômica.
      :ok = :file.sync(io)
    end)

    :ok = File.rename(tmp, path)
    :ok
  end

  defp load!(path) do
    case File.read(path) do
      {:ok, bin} ->
        %{version: @format_version, next_id: next_id, inboxes: inboxes} =
          :erlang.binary_to_term(bin, [:safe])

        %{next_id: next_id, inboxes: inboxes}

      {:error, :enoent} ->
        %{next_id: 1, inboxes: %{}}

      {:error, reason} ->
        raise "Falha ao carregar #{path}: #{inspect(reason)}"
    end
  end

  # ---------------------------------------------------------------------------
  # Auxiliares
  # ---------------------------------------------------------------------------

  defp notify(state, user, event) do
    state.subscribers
    |> Map.get(user, MapSet.new())
    |> Enum.each(&send(&1, {__MODULE__, event}))
  end

  defp collect(0, acc), do: Enum.reverse(acc)

  defp collect(n, acc) do
    receive do
      {MessageSystem, {:new_message, msg}} -> collect(n - 1, [msg | acc])
    after
      1_000 -> Enum.reverse(acc)
    end
  end

  # ---------------------------------------------------------------------------
  # Demonstração
  # ---------------------------------------------------------------------------

  @doc "Executa um cenário completo: concorrência, notificações e recuperação."
  def demo(path \\ "message_system.demo.dat") do
    File.rm(path)
    {:ok, _} = start_link(path: path)
    :ok = subscribe("bob", self())

    # Remetentes concorrentes independentes
    1..20
    |> Enum.map(fn i -> Task.async(fn -> send_message("user#{i}", "bob", "oi #{i}") end) end)
    |> Task.await_many()

    notifications = collect(20, [])
    IO.puts("Notificações recebidas por Bob: #{length(notifications)}")

    [first | _] = notifications
    :ok = mark_read("bob", first.id)

    broadcast("alice", ["bob", "carol"], "reunião às 15h")

    IO.puts("Caixas: #{inspect(inboxes(["bob", "carol"]) |> Map.new(fn {u, m} -> {u, length(m)} end))}")

    # Simula reinicialização: os dados devem ser recuperados do disco
    GenServer.stop(__MODULE__)
    {:ok, _} = start_link(path: path)

    IO.puts("Mensagens de Bob após reinício: #{length(inbox("bob"))}")
    GenServer.stop(__MODULE__)
    File.rm(path)
    :ok
  end
end
