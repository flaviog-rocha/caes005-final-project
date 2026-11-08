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
  @moduledoc """
  Sistema de troca de mensagens entre usuários baseado apenas em processos
  (`spawn`, `send`, `receive`, `Process.monitor`) e na biblioteca padrão.

  Requisitos atendidos:

    1. Detecção de encerramentos inesperados: cada usuário é monitorado com
       `Process.monitor/1`. Ao receber `:DOWN` com motivo diferente de
       `:normal` ou `:shutdown`, o usuário é removido e o evento é registrado.

    2. Prevenção de atualizações parcialmente concluídas: toda alteração de
       estado é feita em uma "transação" pura. As validações e a construção
       do novo estado são feitas antes do commit. Se qualquer etapa falhar,
       o estado original é mantido. Os efeitos colaterais (entrega de
       mensagens e cancelamento de monitores) só são executados após o commit.

    3. Validação de dados: nomes, corpos de mensagem, PIDs e regras de
       negócio são validados antes de qualquer operação de estado.
  """

  require Logger

  @call_timeout 5_000
  @max_name_length 32
  @max_body_size 1_000
  @name_regex ~r/^[a-zA-Z0-9_]+$/

  # ---------------------------------------------------------------------------
  # API pública
  # ---------------------------------------------------------------------------

  @doc "Inicia o processo servidor e retorna o seu PID."
  def start do
    spawn(fn -> loop(%{users: %{}, monitors: %{}}) end)
  end

  @doc "Registra um usuário associado a um processo existente."
  def register_user(server, name, user_pid), do: call(server, {:register, name, user_pid})

  @doc "Remove um usuário do sistema."
  def unregister_user(server, name), do: call(server, {:unregister, name})

  @doc "Envia uma mensagem de `from` para `to`."
  def send_message(server, from, to, body), do: call(server, {:send, from, to, body})

  @doc "Retorna as mensagens recebidas por um usuário, da mais antiga para a mais nova."
  def read_inbox(server, name), do: call(server, {:inbox, name})

  # ---------------------------------------------------------------------------
  # Cliente: comunicação síncrona com o servidor
  # ---------------------------------------------------------------------------

  defp call(server, request) when is_pid(server) do
    # Monitora o servidor para detectar sua morte durante a chamada.
    mref = Process.monitor(server)
    tag = make_ref()
    send(server, {:request, {self(), tag}, request})

    receive do
      {^tag, reply} ->
        Process.demonitor(mref, [:flush])
        reply

      {:DOWN, ^mref, :process, _pid, reason} ->
        {:error, {:server_down, reason}}
    after
      @call_timeout ->
        Process.demonitor(mref, [:flush])
        {:error, :timeout}
    end
  end

  defp call(_server, _request), do: {:error, :invalid_server}

  # ---------------------------------------------------------------------------
  # Loop do servidor
  # ---------------------------------------------------------------------------

  defp loop(state) do
    receive do
      {:request, {from, tag}, request} ->
        {reply, new_state} = handle_request(request, state)
        send(from, {tag, reply})
        loop(new_state)

      {:DOWN, ref, :process, pid, reason} ->
        loop(handle_user_down(ref, pid, reason, state))

      _unknown ->
        loop(state)
    end
  end

  defp handle_request({:register, name, pid}, state),
    do: transact(state, &do_register(name, pid, &1))

  defp handle_request({:unregister, name}, state),
    do: transact(state, &do_unregister(name, &1))

  defp handle_request({:send, from, to, body}, state),
    do: transact(state, &do_send(from, to, body, &1))

  defp handle_request({:inbox, name}, state),
    do: transact(state, &do_inbox(name, &1))

  defp handle_request(_invalid, state),
    do: {{:error, :invalid_request}, state}

  # ---------------------------------------------------------------------------
  # Transação: ou o novo estado é aplicado por completo, ou nada é alterado
  # ---------------------------------------------------------------------------

  # A função `fun` recebe o estado atual e retorna:
  #   {:ok, novo_estado, resposta, efeitos}  -> commit + execução dos efeitos
  #   {:error, motivo}                       -> descarta tudo, mantém o estado
  defp transact(state, fun) do
    case fun.(state) do
      {:ok, new_state, reply, effects} ->
        run_effects(effects)
        {reply, new_state}

      {:error, _reason} = error ->
        {error, state}
    end
  end

  # Efeitos colaterais executados somente após o commit bem-sucedido.
  defp run_effects(effects) do
    Enum.each(effects, fn
      {:deliver, pid, msg} -> send(pid, {:new_message, msg})
      {:demonitor, ref} -> Process.demonitor(ref, [:flush])
    end)
  end

  # ---------------------------------------------------------------------------
  # Operações de domínio (puras em relação ao estado)
  # ---------------------------------------------------------------------------

  defp do_register(name, pid, state) do
    with :ok <- validate_name(name),
         :ok <- validate_pid(pid),
         :ok <- ensure_absent(name, state) do
      # O monitor só é criado após todas as validações terem passado.
      ref = Process.monitor(pid)
      user = %{pid: pid, ref: ref, inbox: [], sent: 0}

      new_state = %{
        state
        | users: Map.put(state.users, name, user),
          monitors: Map.put(state.monitors, ref, name)
      }

      {:ok, new_state, :ok, []}
    end
  end

  defp do_unregister(name, state) do
    with {:ok, user} <- fetch_user(name, state) do
      {:ok, remove_user(name, user, state), :ok, [{:demonitor, user.ref}]}
    end
  end

  defp do_send(from, to, body, state) do
    with :ok <- validate_body(body),
         :ok <- validate_distinct(from, to),
         {:ok, sender} <- fetch_user(from, state),
         {:ok, recipient} <- fetch_user(to, state) do
      msg = %{
        id: System.unique_integer([:positive, :monotonic]),
        from: from,
        to: to,
        body: body,
        at: DateTime.utc_now()
      }

      # Atualiza remetente e destinatário no mesmo novo estado.
      users =
        state.users
        |> Map.put(to, %{recipient | inbox: [msg | recipient.inbox]})
        |> Map.put(from, %{sender | sent: sender.sent + 1})

      {:ok, %{state | users: users}, {:ok, msg.id}, [{:deliver, recipient.pid, msg}]}
    end
  end

  defp do_inbox(name, state) do
    with {:ok, user} <- fetch_user(name, state) do
      {:ok, state, Enum.reverse(user.inbox), []}
    end
  end

  # ---------------------------------------------------------------------------
  # Detecção de encerramentos inesperados
  # ---------------------------------------------------------------------------

  defp handle_user_down(ref, pid, reason, state) do
    case Map.fetch(state.monitors, ref) do
      {:ok, name} ->
        user = Map.fetch!(state.users, name)

        if reason in [:normal, :shutdown] do
          Logger.info("Usuário #{name} (#{inspect(pid)}) encerrou: #{inspect(reason)}")
        else
          Logger.warning(
            "Encerramento inesperado do usuário #{name} (#{inspect(pid)}): #{inspect(reason)}"
          )
        end

        remove_user(name, user, state)

      :error ->
        # DOWN de processo que não é usuário; ignorado.
        state
    end
  end

  defp remove_user(name, user, state) do
    %{
      state
      | users: Map.delete(state.users, name),
        monitors: Map.delete(state.monitors, user.ref)
    }
  end

  # ---------------------------------------------------------------------------
  # Validações
  # ---------------------------------------------------------------------------

  defp validate_name(name) when is_binary(name) do
    cond do
      byte_size(name) == 0 -> {:error, :empty_name}
      byte_size(name) > @max_name_length -> {:error, :name_too_long}
      not String.valid?(name) -> {:error, :invalid_name}
      not Regex.match?(@name_regex, name) -> {:error, :invalid_name}
      true -> :ok
    end
  end

  defp validate_name(_name), do: {:error, :invalid_name}

  defp validate_pid(pid) when is_pid(pid) do
    cond do
      node(pid) != node() -> {:error, :remote_process_not_supported}
      not Process.alive?(pid) -> {:error, :dead_process}
      true -> :ok
    end
  end

  defp validate_pid(_pid), do: {:error, :invalid_pid}

  defp validate_body(body) when is_binary(body) do
    trimmed = String.trim(body)

    cond do
      not String.valid?(body) -> {:error, :invalid_body}
      byte_size(trimmed) == 0 -> {:error, :empty_body}
      byte_size(body) > @max_body_size -> {:error, :body_too_large}
      true -> :ok
    end
  end

  defp validate_body(_body), do: {:error, :invalid_body}

  defp validate_distinct(name, name), do: {:error, :same_user}
  defp validate_distinct(_from, _to), do: :ok

  defp ensure_absent(name, state) do
    with :ok <- validate_name(name) do
      if Map.has_key?(state.users, name),
        do: {:error, :already_registered},
        else: :ok
    end
  end

  defp fetch_user(name, state) do
    with :ok <- validate_name(name) do
      case Map.fetch(state.users, name) do
        {:ok, user} -> {:ok, user}
        :error -> {:error, :unknown_user}
      end
    end
  end
end
