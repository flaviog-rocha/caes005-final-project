# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de troca de mensagens entre usuários, atendendo somente aos seguintes requisitos:
#
# 1. Fazer uso de macros.
# 2. Gerar notificações quando determinadas condições forem atingidas.
# 3. Detectar encerramentos inesperados de processos.
# 4. Diferenciar erros de validação de falhas operacionais.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# Sistema de troca de mensagens entre usuários em Elixir (biblioteca padrão).
#
# Requisitos atendidos:
#   1. Macros: `defcondition/3` gera regras de notificação em tempo de compilação.
#   2. Notificações: condições são avaliadas após cada envio e notificam os assinantes.
#   3. Encerramentos inesperados: o Hub monitora cada processo de usuário e classifica
#      o motivo de saída (normal/shutdown vs. falha).
#   4. Erros: validação retorna {:error, {:validation, campo, mensagem}};
#      falhas operacionais retornam {:error, {:operational, motivo}}.

defmodule MessageSystem do
  @moduledoc """
  Sistema de troca de mensagens entre usuários.

  ## Exemplos

      {:ok, hub} = MessageSystem.start_link()
      MessageSystem.subscribe(hub)

      :ok = MessageSystem.register_user(hub, "ana")
      :ok = MessageSystem.register_user(hub, "bruno")
      :ok = MessageSystem.send_message(hub, "ana", "bruno", "Mensagem URGENTE!")

      # Assinante recebe: {:message_system, :notification, :urgent_keyword, msg, ctx}

      {:ok, [_msg]} = MessageSystem.read_messages(hub, "bruno")

      # Erro de validação:
      {:error, {:validation, :body, _}} = MessageSystem.send_message(hub, "ana", "bruno", "   ")

      # Falha operacional (hub inexistente):
      {:error, {:operational, _}} = MessageSystem.register_user(:nao_existe, "carla")
  """

  # ---------------------------------------------------------------------------
  # Macros de condições (regras de notificação)
  # ---------------------------------------------------------------------------

  defmodule Conditions do
    @moduledoc false

    # Ao usar `use MessageSystem.Conditions`, importa a macro `defcondition/3`,
    # cria o atributo acumulado `@conditions` e registra o hook `__before_compile__`.
    defmacro __using__(_opts) do
      quote do
        import MessageSystem.Conditions, only: [defcondition: 3]
        Module.register_attribute(__MODULE__, :conditions, accumulate: true)
        @before_compile MessageSystem.Conditions
      end
    end

    # Define uma condição nomeada. O corpo recebe a variável `ctx` (mapa de contexto)
    # e deve retornar um valor truthy quando a notificação deve ser disparada.
    defmacro defcondition(name, message, do: body) do
      quote do
        @conditions {unquote(name), unquote(message)}

        def unquote(name)(var!(ctx)), do: unquote(body)
      end
    end

    # Gera `__conditions__/0` (metadados) e `evaluate/1` (retorna as condições satisfeitas).
    defmacro __before_compile__(env) do
      specs = env.module |> Module.get_attribute(:conditions) |> Enum.reverse()

      quote do
        @doc false
        def __conditions__, do: unquote(Macro.escape(specs))

        @doc false
        def evaluate(ctx) do
          for {name, message} <- __conditions__(), apply(__MODULE__, name, [ctx]) do
            {name, message}
          end
        end
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Regras de notificação
  # ---------------------------------------------------------------------------

  use MessageSystem.Conditions

  defcondition :inbox_saturated, "Caixa de entrada do destinatário está saturada" do
    ctx.recipient_unread >= 10
  end

  defcondition :sender_flooding, "Remetente enviando mensagens em excesso" do
    ctx.sender_sent >= 50
  end

  defcondition :urgent_keyword, "Mensagem marcada como urgente" do
    String.contains?(String.downcase(ctx.body), "urgente")
  end

  # ---------------------------------------------------------------------------
  # Processo de caixa de entrada de cada usuário
  # ---------------------------------------------------------------------------

  defmodule Mailbox do
    @moduledoc false
    use GenServer

    # Iniciado sem link: a falha de um usuário não derruba o Hub.
    def start(owner), do: GenServer.start(__MODULE__, owner)

    @impl true
    def init(owner), do: {:ok, %{owner: owner, inbox: []}}

    @impl true
    def handle_call({:deliver, msg}, _from, state) do
      inbox = [msg | state.inbox]
      {:reply, {:ok, unread_count(inbox)}, %{state | inbox: inbox}}
    end

    def handle_call(:read, _from, state) do
      unread = state.inbox |> Enum.reject(& &1.read?) |> Enum.reverse()
      inbox = Enum.map(state.inbox, &Map.put(&1, :read?, true))
      {:reply, {:ok, unread}, %{state | inbox: inbox}}
    end

    defp unread_count(inbox), do: Enum.count(inbox, &(not &1.read?))
  end

  # ---------------------------------------------------------------------------
  # Hub: coordena usuários, validações, notificações e monitoramento
  # ---------------------------------------------------------------------------

  use GenServer

  @timeout 5_000
  @max_name 32
  @max_body 500

  # --- API pública -----------------------------------------------------------

  def start_link(opts \\ []) do
    name_opts = Keyword.take(opts, [:name])
    GenServer.start_link(__MODULE__, :ok, name_opts)
  end

  def register_user(hub \\ __MODULE__, name), do: call(hub, {:register, name})

  def send_message(hub \\ __MODULE__, from, to, body),
    do: call(hub, {:send, from, to, body})

  def read_messages(hub \\ __MODULE__, name), do: call(hub, {:read, name})

  # O processo chamador passa a receber notificações.
  def subscribe(hub \\ __MODULE__), do: call(hub, {:subscribe, self()})

  # Encerra o processo de um usuário com o motivo informado (útil para testes).
  def kill_user(hub \\ __MODULE__, name, reason), do: call(hub, {:kill, name, reason})

  # --- Callbacks --------------------------------------------------------------

  @impl true
  def init(:ok) do
    {:ok, %{users: %{}, sent: %{}, subscribers: MapSet.new()}}
  end

  @impl true
  def handle_call({:register, name}, _from, state) do
    with :ok <- validate_name(name),
         :ok <- ensure_absent(state, name),
         {:ok, pid} <- start_mailbox(name) do
      ref = Process.monitor(pid)
      {:reply, :ok, %{state | users: Map.put(state.users, name, {pid, ref})}}
    else
      error -> {:reply, error, state}
    end
  end

  def handle_call({:send, from, to, body}, _from, state) do
    with :ok <- validate_body(body),
         {:ok, _} <- fetch_user(state, from),
         {:ok, recipient} <- fetch_user(state, to),
         {:ok, unread} <- mailbox_call(recipient, {:deliver, build_message(from, to, body)}) do
      sent = Map.update(state.sent, from, 1, &(&1 + 1))
      state = %{state | sent: sent}

      ctx = %{
        sender: from,
        recipient: to,
        body: body,
        recipient_unread: unread,
        sender_sent: Map.fetch!(sent, from)
      }

      Enum.each(evaluate(ctx), fn {kind, message} -> notify(state, kind, message, ctx) end)
      {:reply, :ok, state}
    else
      error -> {:reply, error, state}
    end
  end

  def handle_call({:read, name}, _from, state) do
    with {:ok, pid} <- fetch_user(state, name),
         {:ok, messages} <- mailbox_call(pid, :read) do
      {:reply, {:ok, messages}, state}
    else
      error -> {:reply, error, state}
    end
  end

  def handle_call({:subscribe, pid}, _from, state) do
    {:reply, :ok, %{state | subscribers: MapSet.put(state.subscribers, pid)}}
  end

  def handle_call({:kill, name, reason}, _from, state) do
    case fetch_user(state, name) do
      {:ok, pid} ->
        Process.exit(pid, reason)
        {:reply, :ok, state}

      error ->
        {:reply, error, state}
    end
  end

  # Monitor disparado quando um processo de usuário termina.
  @impl true
  def handle_info({:DOWN, ref, :process, _pid, reason}, state) do
    case Enum.find(state.users, fn {_name, {_pid, r}} -> r == ref end) do
      nil ->
        {:noreply, state}

      {name, _} ->
        users = Map.delete(state.users, name)

        if normal_exit?(reason) do
          notify(state, :user_stopped, "Usuário encerrou normalmente", %{user: name, reason: reason})
        else
          notify(state, :unexpected_exit, "Encerramento inesperado de processo de usuário", %{
            user: name,
            reason: reason
          })
        end

        {:noreply, %{state | users: users}}
    end
  end

  def handle_info(_msg, state), do: {:noreply, state}

  # --- Funções auxiliares -----------------------------------------------------

  # Erros de validação: entrada do cliente inválida (culpa do chamador).
  defp validation_error(field, message), do: {:error, {:validation, field, message}}

  # Falhas operacionais: problemas de infraestrutura/processos (não são culpa da entrada).
  defp operational_error(reason), do: {:error, {:operational, reason}}

  defp validate_name(name) when is_binary(name) and byte_size(name) in 1..@max_name, do: :ok
  defp validate_name(_), do: validation_error(:name, "nome deve ser texto de 1 a #{@max_name} bytes")

  defp validate_body(body) when is_binary(body) do
    cond do
      String.trim(body) == "" -> validation_error(:body, "corpo não pode ser vazio")
      byte_size(body) > @max_body -> validation_error(:body, "corpo excede #{@max_body} bytes")
      true -> :ok
    end
  end

  defp validate_body(_), do: validation_error(:body, "corpo deve ser texto")

  defp ensure_absent(state, name) do
    if Map.has_key?(state.users, name),
      do: validation_error(:name, "usuário já cadastrado"),
      else: :ok
  end

  defp fetch_user(state, name) do
    case Map.fetch(state.users, name) do
      {:ok, {pid, _ref}} -> {:ok, pid}
      :error -> validation_error(:user, "usuário desconhecido: #{inspect(name)}")
    end
  end

  defp start_mailbox(name) do
    case Mailbox.start(name) do
      {:ok, pid} -> {:ok, pid}
      {:error, reason} -> operational_error({:mailbox_start_failed, reason})
    end
  end

  # Chamada a um processo de caixa de entrada, convertendo saídas inesperadas em erro operacional.
  defp mailbox_call(pid, msg) do
    GenServer.call(pid, msg, @timeout)
  catch
    :exit, reason -> operational_error({:mailbox_unavailable, reason})
  end

  # Chamada ao Hub, convertendo indisponibilidade em erro operacional.
  defp call(hub, msg) do
    GenServer.call(hub, msg, @timeout)
  catch
    :exit, reason -> operational_error({:hub_unavailable, reason})
  end

  defp build_message(from, to, body) do
    %{
      from: from,
      to: to,
      body: body,
      sent_at: System.system_time(:millisecond),
      read?: false
    }
  end

  defp notify(state, kind, message, details) do
    Enum.each(state.subscribers, fn pid ->
      send(pid, {:message_system, :notification, kind, message, details})
    end)
  end

  defp normal_exit?(reason) when reason in [:normal, :shutdown], do: true
  defp normal_exit?({:shutdown, _}), do: true
  defp normal_exit?(_), do: false
end
