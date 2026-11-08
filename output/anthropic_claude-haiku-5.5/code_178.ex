# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de troca de mensagens entre usuários, atendendo somente aos seguintes requisitos:
#
# 1. Recuperar processos após falhas inesperadas.
# 2. Garantir a consistência dos dados durante operações concorrentes.
# 3. Fazer uso de macros.
# 4. Permitir cadastrar, consultar, atualizar e remover registros.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# =============================================================================
# Sistema de troca de mensagens entre usuários (arquivo único e autocontido)
#
# Requisitos atendidos:
#   1. Recuperação após falhas: o estado vive em um GenServer supervisionado
#      (reinício automático) e é persistido em disco de forma atômica (escrita
#      em .tmp + backup .bak). Ao reiniciar, o estado é recarregado do snapshot.
#   2. Consistência em concorrência: todas as operações (leituras e escritas)
#      são serializadas pelo GenServer (único escritor). Cada operação de escrita
#      só é confirmada após a persistência bem-sucedida; caso contrário, o estado
#      anterior é mantido. Atualizações suportam controle otimista via `version`.
#   3. Macros: `defop`, `defapi`, `require_fields!`, `fetch!` e `ensure!`
#      eliminam boilerplate e centralizam validações e erros de negócio.
#   4. CRUD de usuários e mensagens: criar, consultar, atualizar e remover.
#
# Uso (exemplo):
#   {:ok, _} = Chat.start_link(path: "dados.bin")
#   {:ok, ana} = Chat.create_user(%{name: "Ana"})
#   {:ok, beto} = Chat.create_user(%{name: "Beto"})
#   {:ok, msg} = Chat.send_message(ana.id, beto.id, "Oi, Beto!")
#   {:ok, [_]} = Chat.inbox(beto.id)
# =============================================================================

defmodule Chat.Macros do
  @moduledoc false
  # Macros utilitárias usadas pelos módulos do sistema.

  @doc """
  Define uma operação do servidor como uma cláusula de `apply_op/3`.
  A expansão recebe `state` implicitamente e deve retornar `{resposta, novo_estado}`.
  Falhas de negócio são sinalizadas com `throw({:fail, motivo})`.
  """
  defmacro defop(name, arg, do: body) do
    quote do
      def apply_op(unquote(name), unquote(arg), var!(state)) do
        unquote(body)
      end
    end
  end

  @doc "Valida que todas as chaves listadas existem no mapa; caso contrário, falha."
  defmacro require_fields!(map, fields) do
    quote do
      case Enum.reject(unquote(fields), &Map.has_key?(unquote(map), &1)) do
        [] -> :ok
        missing -> throw({:fail, {:missing_fields, missing}})
      end
    end
  end

  @doc "Busca uma chave em um mapa ou falha com o motivo informado."
  defmacro fetch!(collection, key, reason) do
    quote do
      case Map.fetch(unquote(collection), unquote(key)) do
        {:ok, value} -> value
        :error -> throw({:fail, unquote(reason)})
      end
    end
  end

  @doc "Garante uma condição ou falha com o motivo informado."
  defmacro ensure!(condition, reason) do
    quote do
      unless unquote(condition), do: throw({:fail, unquote(reason)})
    end
  end

  @doc """
  Gera uma função pública `nome(args...)` que envia a mensagem
  `{:nome, payload}` ao servidor. Com um argumento, o payload é o próprio
  argumento; com vários, é uma tupla; sem argumentos, é `nil`.
  """
  defmacro defapi({name, _, args}) do
    args = args || []

    payload =
      case args do
        [] -> nil
        [single] -> single
        many -> {:{}, [], many}
      end

    quote do
      def unquote(name)(unquote_splicing(args)) do
        call({unquote(name), unquote(payload)})
      end
    end
  end
end

defmodule Chat.Store do
  @moduledoc """
  Processo dono do estado. Único escritor: serializa todas as operações,
  garantindo consistência mesmo sob concorrência. Persiste o estado após
  cada mutação bem-sucedida.
  """
  use GenServer
  import Chat.Macros
  require Logger

  @default_path "chat_store.bin"

  # ---------------------------------------------------------------------------
  # Inicialização e recuperação
  # ---------------------------------------------------------------------------

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    path = Keyword.get(opts, :path, @default_path)
    state = Map.merge(load(path), %{path: path})

    Logger.info(
      "Chat.Store iniciado: #{map_size(state.users)} usuários, " <>
        "#{map_size(state.messages)} mensagens (seq=#{state.seq})"
    )

    {:ok, state}
  end

  # ---------------------------------------------------------------------------
  # Operações de domínio (cláusulas contíguas de apply_op/3)
  # ---------------------------------------------------------------------------

  defop :create_user, attrs do
    require_fields!(attrs, [:name])
    name = attrs.name
    ensure!(is_binary(name) and String.trim(name) != "", :invalid_name)
    ensure!(not name_taken?(state.users, name, nil), :name_taken)

    id = state.seq + 1
    user = %{id: id, name: name, version: 1}
    {{:ok, user}, %{state | seq: id, users: Map.put(state.users, id, user)}}
  end

  defop :get_user, id do
    {{:ok, fetch!(state.users, id, :user_not_found)}, state}
  end

  defop :list_users, _ do
    users = state.users |> Map.values() |> Enum.sort_by(& &1.id)
    {{:ok, users}, state}
  end

  defop :update_user, {id, attrs} do
    user = fetch!(state.users, id, :user_not_found)
    check_version(user, attrs)

    name = Map.get(attrs, :name, user.name)
    ensure!(is_binary(name) and String.trim(name) != "", :invalid_name)
    ensure!(not name_taken?(state.users, name, id), :name_taken)

    updated = %{user | name: name, version: user.version + 1}
    {{:ok, updated}, %{state | users: Map.put(state.users, id, updated)}}
  end

  defop :delete_user, id do
    fetch!(state.users, id, :user_not_found)

    # Remoção em cascata: mensagens enviadas ou recebidas pelo usuário são removidas
    messages = Map.reject(state.messages, fn {_, m} -> m.from == id or m.to == id end)

    {:ok, %{state | users: Map.delete(state.users, id), messages: messages}}
  end

  defop :send_message, {from, to, body} do
    fetch!(state.users, from, :sender_not_found)
    fetch!(state.users, to, :recipient_not_found)
    ensure!(is_binary(body) and String.trim(body) != "", :invalid_body)

    id = state.seq + 1
    msg = %{id: id, from: from, to: to, body: body, read: false, version: 1}
    {{:ok, msg}, %{state | seq: id, messages: Map.put(state.messages, id, msg)}}
  end

  defop :get_message, id do
    {{:ok, fetch!(state.messages, id, :message_not_found)}, state}
  end

  defop :inbox, user_id do
    fetch!(state.users, user_id, :user_not_found)

    msgs =
      state.messages
      |> Map.values()
      |> Enum.filter(&(&1.to == user_id))
      |> Enum.sort_by(& &1.id)

    {{:ok, msgs}, state}
  end

  defop :update_message, {id, attrs} do
    msg = fetch!(state.messages, id, :message_not_found)
    check_version(msg, attrs)

    changes = Map.take(attrs, [:body, :read])
    body = Map.get(changes, :body, msg.body)
    read = Map.get(changes, :read, msg.read)

    ensure!(is_binary(body) and String.trim(body) != "", :invalid_body)
    ensure!(is_boolean(read), :invalid_read)

    updated = %{msg | body: body, read: read, version: msg.version + 1}
    {{:ok, updated}, %{state | messages: Map.put(state.messages, id, updated)}}
  end

  defop :delete_message, id do
    fetch!(state.messages, id, :message_not_found)
    {:ok, %{state | messages: Map.delete(state.messages, id)}}
  end

  # ---------------------------------------------------------------------------
  # Despacho das chamadas (com tratamento de falhas e persistência)
  # ---------------------------------------------------------------------------

  @impl true
  def handle_call({op, arg}, _from, state) do
    {reply, new_state} = apply_op(op, arg, state)
    commit(reply, new_state, state)
  rescue
    FunctionClauseError ->
      {:reply, {:error, :unknown_operation}, state}

    e ->
      Logger.error("Falha inesperada em operação: " <> Exception.message(e))
      {:reply, {:error, {:internal, Exception.message(e)}}, state}
  catch
    :throw, {:fail, reason} ->
      {:reply, {:error, reason}, state}
  end

  # ---------------------------------------------------------------------------
  # Helpers de domínio
  # ---------------------------------------------------------------------------

  defp name_taken?(users, name, except_id) do
    Enum.any?(users, fn {id, u} -> id != except_id and u.name == name end)
  end

  # Controle otimista: só aplica a mudança se a versão esperada coincidir.
  defp check_version(%{version: v}, %{expected_version: e}) when e != v,
    do: throw({:fail, :version_conflict})

  defp check_version(_record, _attrs), do: :ok

  # Confirma a mutação somente após persistência bem-sucedida.
  defp commit(reply, new_state, old_state) do
    if new_state == old_state do
      {:reply, reply, old_state}
    else
      case persist(new_state) do
        :ok ->
          {:reply, reply, new_state}

        {:error, reason} ->
          Logger.error("Falha ao persistir estado: #{inspect(reason)}")
          {:reply, {:error, {:persistence_failed, reason}}, old_state}
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Persistência atômica e recuperação
  # ---------------------------------------------------------------------------

  # Escreve em arquivo temporário, preserva o snapshot anterior como .bak
  # e só então substitui o arquivo principal (rename é atômico no SO).
  defp persist(%{path: path} = state) do
    tmp = path <> ".tmp"
    bin = :erlang.term_to_binary(Map.take(state, [:users, :messages, :seq]))

    with :ok <- File.write(tmp, bin),
         :ok <- backup(path),
         :ok <- File.rename(tmp, path) do
      :ok
    end
  end

  defp backup(path) do
    if File.exists?(path), do: File.cp(path, path <> ".bak"), else: :ok
  end

  # Tenta o snapshot principal e, em seguida, o backup. Se nada for válido,
  # inicia com estado vazio.
  defp load(path) do
    Enum.find_value([path, path <> ".bak"], default_state(), &read_snapshot/1)
  end

  defp read_snapshot(file) do
    case File.read(file) do
      {:ok, bin} ->
        case decode(bin) do
          {:ok, term} -> term
          :error -> nil
        end

      _ ->
        nil
    end
  end

  defp decode(bin) do
    case :erlang.binary_to_term(bin, [:safe]) do
      %{users: _, messages: _, seq: _} = term -> {:ok, term}
      _ -> :error
    end
  rescue
    ArgumentError -> :error
  end

  defp default_state, do: %{users: %{}, messages: %{}, seq: 0}
end

defmodule Chat do
  @moduledoc """
  API pública do sistema. Cada chamada é enviada ao `Chat.Store`.
  Tentativas automáticas ocorrem somente quando o processo ainda não está
  disponível (`:noproc`, ex.: durante reinício pelo supervisor), situação em
  que a operação ainda não foi executada, evitando duplicidade em escritas.
  """
  import Chat.Macros

  @doc "Inicia o supervisor com o Chat.Store (reinício automático em caso de falha)."
  def start_link(opts \\ []) do
    children = [{Chat.Store, opts}]

    Supervisor.start_link(children,
      strategy: :one_for_one,
      max_restarts: 5,
      max_seconds: 10,
      name: Chat.Supervisor
    )
  end

  # Usuários
  defapi create_user(name)
  defapi list_users()
  defapi get_user(id)
  defapi update_user(id, attrs)
  defapi delete_user(id)

  # Mensagens
  defapi send_message(from, to, body)
  defapi inbox(user_id)
  defapi get_message(id)
  defapi update_message(id, attrs)
  defapi delete_message(id)

  # Envio com retentativa apenas para :noproc (processo ainda não disponível).
  defp call(msg, retries \\ 3) do
    GenServer.call(Chat.Store, msg)
  catch
    :exit, {:noproc, _} when retries > 0 ->
      Process.sleep(100)
      call(msg, retries - 1)

    :exit, reason ->
      {:error, {:unavailable, reason}}
  end
end
