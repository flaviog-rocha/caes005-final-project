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

# Sistema de troca de mensagens entre usuários usando apenas Process e a biblioteca padrão.
#
# Modelo de execução:
#   * O broker é um processo que mantém todo o estado do sistema.
#   * Cada requisição é processada sequencialmente pelo broker, que valida
#     os dados e só então produz um novo estado (imutável). O estado antigo é
#     substituído de uma só vez, portanto nenhuma atualização fica
#     parcialmente concluída.
#   * Os clientes (usuários) são monitorados com Process.monitor/1. Quando um
#     cliente termina, o broker recebe {:DOWN, ...}, remove o usuário e registra
#     se a saída foi esperada (:normal) ou inesperada.
#   * O cliente também monitora o broker, detectando se ele cair durante uma chamada.
#
# Exemplo de uso:
#   broker = MessageExchange.start_broker()
#   alice = spawn(fn -> receive do :stop -> :ok end end)
#   MessageExchange.register(broker, "alice", alice)
#   MessageExchange.send_message(broker, "alice", "bob", "oi")
#   MessageExchange.fetch(broker, "bob")
#   MessageExchange.events(broker)

defmodule MessageExchange do
  @moduledoc """
  Sistema de troca de mensagens com:

    1. Detecção de encerramentos inesperados de processos (monitores).
    2. Atualizações atômicas: o estado só é substituído após todas as
       validações e cálculos, evitando atualizações parcialmente concluídas.
    3. Validação de todos os dados antes de qualquer operação.
  """

  @call_timeout 5_000
  @max_body_bytes 1_000

  # ---------------------------------------------------------------------------
  # API pública
  # ---------------------------------------------------------------------------

  @doc "Inicia o processo broker e retorna seu pid."
  def start_broker do
    spawn(fn -> loop(initial_state()) end)
  end

  @doc "Registra um usuário associado a um processo cliente (padrão: processo chamador)."
  def register(broker, username, client_pid \\ self()) do
    call(broker, {:register, username, client_pid})
  end

  @doc "Envia uma mensagem de `from` para `to`."
  def send_message(broker, from, to, body) do
    call(broker, {:send, from, to, body})
  end

  @doc "Retorna e limpa a caixa de entrada do usuário."
  def fetch(broker, username) do
    call(broker, {:fetch, username})
  end

  @doc "Lista os usuários conectados."
  def users(broker) do
    call(broker, :users)
  end

  @doc "Lista os eventos de desconexão (esperados e inesperados) em ordem cronológica."
  def events(broker) do
    call(broker, :events)
  end

  # ---------------------------------------------------------------------------
  # Lado cliente: chamada síncrona com detecção de falha do broker
  # ---------------------------------------------------------------------------

  defp call(broker, request) when is_pid(broker) do
    ref = make_ref()
    mref = Process.monitor(broker)
    send(broker, {:request, self(), ref, request})

    receive do
      {^ref, reply} ->
        Process.demonitor(mref, [:flush])
        reply

      {:DOWN, ^mref, :process, _pid, reason} ->
        {:error, {:broker_down, reason}}
    after
      @call_timeout ->
        Process.demonitor(mref, [:flush])
        {:error, :timeout}
    end
  end

  defp call(_broker, _request), do: {:error, :invalid_broker}

  # ---------------------------------------------------------------------------
  # Lado broker: loop com estado imutável
  # ---------------------------------------------------------------------------

  defp initial_state do
    %{
      # username => %{pid, mref, inbox, sent}
      users: %{},
      # mref => username (índice para tratar mensagens :DOWN)
      refs: %{},
      # eventos de desconexão (em ordem reversa)
      events: [],
      next_id: 1
    }
  end

  defp loop(state) do
    receive do
      {:request, from, ref, request} ->
        # Cada requisição é executada de forma atômica: se houver erro,
        # o estado antigo é mantido sem nenhuma alteração parcial.
        {reply, new_state} =
          case handle(request, state) do
            {:ok, reply, updated_state} -> {reply, updated_state}
            {:error, reason} -> {{:error, reason}, state}
          end

        send(from, {ref, reply})
        loop(new_state)

      {:DOWN, mref, :process, pid, reason} ->
        loop(handle_down(mref, pid, reason, state))

      _unknown ->
        loop(state)
    end
  end

  # ---------------------------------------------------------------------------
  # Tratamento das requisições (retornam {:ok, resposta, novo_estado} ou {:error, motivo})
  # ---------------------------------------------------------------------------

  defp handle({:register, username, pid}, state) do
    with {:ok, name} <- validate_username(username),
         {:ok, pid} <- validate_client_pid(pid),
         :ok <- ensure_not_registered(state, name) do
      mref = Process.monitor(pid)
      user = %{pid: pid, mref: mref, inbox: [], sent: 0}

      new_state = %{
        state
        | users: Map.put(state.users, name, user),
          refs: Map.put(state.refs, mref, name)
      }

      {:ok, :registered, new_state}
    end
  end

  defp handle({:send, from, to, body}, state) do
    with {:ok, from} <- validate_username(from),
         {:ok, to} <- validate_username(to),
         {:ok, body} <- validate_body(body),
         {:ok, _sender} <- fetch_user(state, from),
         {:ok, recipient} <- fetch_user(state, to),
         :ok <- ensure_alive(recipient.pid) do
      message = %{
        id: state.next_id,
        from: from,
        to: to,
        body: body,
        sent_at: System.system_time(:millisecond)
      }

      # Todas as alterações são calculadas sobre o mapa atual e confirmadas
      # juntas no novo estado. Se qualquer etapa falhar, nada é alterado.
      users =
        state.users
        |> Map.update!(from, &%{&1 | sent: &1.sent + 1})
        |> Map.update!(to, &%{&1 | inbox: [message | &1.inbox]})

      new_state = %{state | users: users, next_id: state.next_id + 1}
      {:ok, {:sent, message.id}, new_state}
    end
  end

  defp handle({:fetch, username}, state) do
    with {:ok, name} <- validate_username(username),
         {:ok, user} <- fetch_user(state, name) do
      messages = Enum.reverse(user.inbox)
      new_state = %{state | users: Map.put(state.users, name, %{user | inbox: []})}
      {:ok, messages, new_state}
    end
  end

  defp handle(:users, state) do
    {:ok, state.users |> Map.keys() |> Enum.sort(), state}
  end

  defp handle(:events, state) do
    {:ok, Enum.reverse(state.events), state}
  end

  defp handle(other, _state) do
    {:error, {:invalid_request, other}}
  end

  # ---------------------------------------------------------------------------
  # Detecção de encerramento de processos
  # ---------------------------------------------------------------------------

  defp handle_down(mref, pid, reason, state) do
    case Map.pop(state.refs, mref) do
      {nil, _refs} ->
        state

      {name, refs} ->
        {_user, users} = Map.pop(state.users, name)

        event =
          if reason == :normal do
            {:disconnected, name}
          else
            {:unexpected_exit, name, pid, reason}
          end

        %{state | users: users, refs: refs, events: [event | state.events]}
    end
  end

  # ---------------------------------------------------------------------------
  # Validações
  # ---------------------------------------------------------------------------

  defp validate_username(name) when is_binary(name) do
    if Regex.match?(~r/^[a-zA-Z0-9_]{3,32}$/, name) do
      {:ok, name}
    else
      {:error, :invalid_username}
    end
  end

  defp validate_username(_), do: {:error, :invalid_username}

  defp validate_body(body) when is_binary(body) do
    size = byte_size(body)

    cond do
      size == 0 -> {:error, :empty_body}
      size > @max_body_bytes -> {:error, :body_too_large}
      not String.valid?(body) -> {:error, :invalid_encoding}
      true -> {:ok, body}
    end
  end

  defp validate_body(_), do: {:error, :invalid_body}

  defp validate_client_pid(pid) when is_pid(pid) do
    if Process.alive?(pid), do: {:ok, pid}, else: {:error, :dead_process}
  end

  defp validate_client_pid(_), do: {:error, :invalid_pid}

  defp ensure_not_registered(state, name) do
    if Map.has_key?(state.users, name) do
      {:error, :already_registered}
    else
      :ok
    end
  end

  defp fetch_user(state, name) do
    case Map.fetch(state.users, name) do
      {:ok, user} -> {:ok, user}
      :error -> {:error, {:unknown_user, name}}
    end
  end

  defp ensure_alive(pid) do
    if Process.alive?(pid), do: :ok, else: {:error, :recipient_unavailable}
  end
end
