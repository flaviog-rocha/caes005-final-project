# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de troca de mensagens entre usuários, atendendo somente aos seguintes requisitos:
#
# 1. Fazer uso de macros.
# 2. Gerar notificações quando determinadas condições forem atingidas.
# 3. Detectar encerramentos inesperados de processos.
# 4. Diferenciar erros de validação de falhas operacionais.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# Sistema de troca de mensagens entre usuários (arquivo autocontido).
# Execução: elixir messaging.exs
#
# Requisitos atendidos:
#   1. Macros: Messaging.Macros (defrule, defnotification, operation).
#   2. Notificações disparadas quando condições são satisfeitas.
#   3. Detecção de encerramento inesperado de processos (monitors).
#   4. Erros de validação ({:validation, ...}) separados de falhas
#      operacionais ({:operational, ...}).

defmodule Messaging.Macros do
  @moduledoc """
  Macros de domínio para declarar regras de validação, condições de
  notificação e operações protegidas contra falhas operacionais.
  """

  # Ao usar o módulo, registra os acumuladores de atributos e o
  # callback @before_compile que gera as funções de avaliação.
  defmacro __using__(_opts) do
    quote do
      import Messaging.Macros
      Module.register_attribute(__MODULE__, :rule_fns, accumulate: true)
      Module.register_attribute(__MODULE__, :notification_fns, accumulate: true)
      @before_compile Messaging.Macros
    end
  end

  # Gera validate/1 (regras de validação) e evaluate/1 (condições de notificação)
  # a partir das declarações feitas com defrule/4 e defnotification/4.
  defmacro __before_compile__(env) do
    rules = env.module |> Module.get_attribute(:rule_fns) |> Enum.reverse()
    notifications = env.module |> Module.get_attribute(:notification_fns) |> Enum.reverse()

    quote do
      @doc "Executa todas as regras de validação. Retorna :ok ou {:error, {:validation, erros}}."
      def validate(ctx) do
        errors =
          Enum.flat_map(unquote(rules), fn fun ->
            case apply(__MODULE__, fun, [ctx]) do
              :ok -> []
              {:error, error} -> [error]
            end
          end)

        if errors == [], do: :ok, else: {:error, {:validation, errors}}
      end

      @doc "Avalia as condições de notificação e retorna as que foram satisfeitas."
      def evaluate(ctx) do
        Enum.flat_map(unquote(notifications), fn fun ->
          case apply(__MODULE__, fun, [ctx]) do
            {:notify, _, _} = notification -> [notification]
            :skip -> []
          end
        end)
      end
    end
  end

  # Declara uma regra de validação. Quando a expressão é falsa, produz um erro
  # de validação (erro do usuário, não falha do sistema).
  defmacro defrule(name, message, ctx, do: expr) do
    fun = :"rule_#{name}"

    quote do
      @rule_fns unquote(fun)

      def unquote(fun)(unquote(ctx)) do
        if unquote(expr),
          do: :ok,
          else: {:error, %{rule: unquote(name), message: unquote(message)}}
      end
    end
  end

  # Declara uma condição de notificação. Quando a expressão é verdadeira,
  # a função retorna {:notify, nome, texto}; caso contrário, :skip.
  defmacro defnotification(name, ctx, text, do: expr) do
    fun = :"notify_#{name}"

    quote do
      @notification_fns unquote(fun)

      def unquote(fun)(unquote(ctx)) do
        if unquote(expr),
          do: {:notify, unquote(name), unquote(text)},
          else: :skip
      end
    end
  end

  # Executa um bloco convertendo exceções e saídas de processo em falhas
  # operacionais. Assim, falhas técnicas nunca se confundem com validações.
  defmacro operation(label, do: block) do
    quote do
      try do
        unquote(block)
      rescue
        exception ->
          {:error, {:operational, unquote(label), Exception.message(exception)}}
      catch
        :exit, reason ->
          {:error, {:operational, unquote(label), {:exit, reason}}}
      end
    end
  end
end

defmodule Messaging.Rules do
  @moduledoc "Regras de validação de mensagens."
  use Messaging.Macros

  @max_body_length 500

  # O contexto de validação é um mapa: %{message: msg, users: MapSet}.
  defrule :sender_registered, "remetente não cadastrado", ctx, do: MapSet.member?(ctx.users, ctx.message.from)
  defrule :recipient_registered, "destinatário não cadastrado", ctx, do: MapSet.member?(ctx.users, ctx.message.to)
  defrule :distinct_parties, "remetente e destinatário devem ser diferentes", ctx, do: ctx.message.from != ctx.message.to
  defrule :body_present, "o corpo não pode ser vazio", ctx, do: is_binary(ctx.message.body) and String.trim(ctx.message.body) != ""
  defrule :body_length, "o corpo excede 500 caracteres", ctx, do: is_binary(ctx.message.body) and String.length(ctx.message.body) <= @max_body_length
end

defmodule Messaging.Notifications do
  @moduledoc "Condições que geram notificações."
  use Messaging.Macros

  @threshold 3

  # Dispara uma única vez, quando a caixa do destinatário atinge o limiar.
  defnotification :unread_threshold, ctx, "caixa de entrada atingiu o limite de não lidas", do: ctx.event == :message_delivered and ctx.unread == @threshold

  # Dispara para mensagens que contenham a palavra-chave URGENTE.
  defnotification :urgent_message, ctx, "mensagem urgente recebida", do: ctx.event == :message_delivered and String.contains?(String.upcase(ctx.message.body), "URGENTE")

  # Dispara quando um assinante encerra de forma inesperada.
  defnotification :subscriber_crashed, ctx, "um assinante encerrou inesperadamente", do: ctx.event == :subscriber_down
end

defmodule Messaging.Server do
  @moduledoc """
  Servidor de mensagens. Mantém usuários, caixas de entrada, assinantes
  para notificações e monitora os assinantes para detectar encerramentos
  inesperados.
  """
  use GenServer
  import Messaging.Macros, only: [operation: 2]

  alias Messaging.{Notifications, Rules}

  @ops_user "ops"

  ## API pública

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))
  end

  def register_user(user, server \\ __MODULE__), do: call(server, {:register, user})

  def subscribe(user, pid, server \\ __MODULE__), do: call(server, {:subscribe, user, pid})

  def send_message(from, to, body, server \\ __MODULE__), do: call(server, {:send, from, to, body})

  def inbox(user, server \\ __MODULE__), do: call(server, {:inbox, user})

  def mark_read(user, server \\ __MODULE__), do: call(server, {:mark_read, user})

  # Chamadas ao servidor são protegidas pela macro operation: se o processo
  # não existir ou expirar, o retorno é uma falha operacional.
  defp call(server, request) do
    operation :server_call do
      GenServer.call(server, request)
    end
  end

  ## Callbacks

  @impl true
  def init(_opts) do
    {:ok,
     %{
       users: MapSet.new(),
       inboxes: %{},
       unread: %{},
       subscribers: %{},
       monitors: %{},
       notifications: %{}
     }}
  end

  @impl true
  def handle_call({:register, user}, _from, state) do
    if MapSet.member?(state.users, user) do
      {:reply, validation_error(:user_unique, "usuário já cadastrado"), state}
    else
      {:reply, :ok, %{state | users: MapSet.put(state.users, user)}}
    end
  end

  def handle_call({:subscribe, user, pid}, _from, state) do
    cond do
      not MapSet.member?(state.users, user) ->
        {:reply, validation_error(:user_registered, "usuário não cadastrado"), state}

      not (is_pid(pid) and Process.alive?(pid)) ->
        {:reply, validation_error(:subscriber_alive, "assinante precisa ser um processo vivo"), state}

      true ->
        ref = Process.monitor(pid)

        state = %{
          state
          | subscribers: Map.put(state.subscribers, user, pid),
            monitors: Map.put(state.monitors, ref, {user, pid})
        }

        {:reply, :ok, state}
    end
  end

  def handle_call({:send, from, to, body}, _from, state) do
    message = %{from: from, to: to, body: body, sent_at: DateTime.utc_now()}

    case Rules.validate(%{message: message, users: state.users}) do
      :ok ->
        # Falhas durante a entrega são operacionais e não derrubam o servidor.
        case operation(:deliver, do: deliver(message, state)) do
          {:error, _} = error ->
            {:reply, error, state}

          new_state ->
            ctx = %{
              event: :message_delivered,
              target: to,
              message: message,
              unread: Map.fetch!(new_state.unread, to)
            }

            {:reply, :ok, dispatch(new_state, ctx)}
        end

      {:error, _} = error ->
        {:reply, error, state}
    end
  end

  def handle_call({:inbox, user}, _from, state) do
    {:reply, state.inboxes |> Map.get(user, []) |> Enum.reverse(), state}
  end

  def handle_call({:mark_read, user}, _from, state) do
    {:reply, :ok, %{state | unread: Map.put(state.unread, user, 0)}}
  end

  # Detecção de encerramento de assinantes monitorados.
  @impl true
  def handle_info({:DOWN, ref, :process, pid, reason}, state) do
    case Map.pop(state.monitors, ref) do
      {nil, _monitors} ->
        {:noreply, state}

      {{user, ^pid}, monitors} ->
        subscribers =
          if Map.get(state.subscribers, user) == pid do
            Map.delete(state.subscribers, user)
          else
            state.subscribers
          end

        state = %{state | monitors: monitors, subscribers: subscribers}

        if unexpected_exit?(reason) do
          ctx = %{event: :subscriber_down, target: @ops_user, user: user, reason: reason}
          {:noreply, dispatch(state, ctx)}
        else
          {:noreply, state}
        end
    end
  end

  ## Funções privadas

  defp deliver(message, state) do
    %{
      state
      | inboxes: Map.update(state.inboxes, message.to, [message], &[message | &1]),
        unread: Map.update(state.unread, message.to, 1, &(&1 + 1))
    }
  end

  defp dispatch(state, ctx) do
    ctx
    |> Notifications.evaluate()
    |> Enum.reduce(state, fn {:notify, name, text}, acc ->
      notification = %{name: name, text: text, target: ctx.target}

      # Entrega imediata ao assinante, se houver; sempre registra no histórico.
      case Map.fetch(acc.subscribers, ctx.target) do
        {:ok, pid} -> send(pid, {:notification, notification})
        :error -> :ok
      end

      %{
        acc
        | notifications:
            Map.update(acc.notifications, ctx.target, [notification], &[notification | &1])
      }
    end)
  end

  # Saídas normais e desligamentos controlados não são inesperados.
  defp unexpected_exit?(:normal), do: false
  defp unexpected_exit?(:shutdown), do: false
  defp unexpected_exit?({:shutdown, _}), do: false
  defp unexpected_exit?(_), do: true

  defp validation_error(rule, message),
    do: {:error, {:validation, [%{rule: rule, message: message}]}}
end

defmodule Messaging.System do
  @moduledoc "Supervisor do sistema de mensagens."
  use Supervisor

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    # Reinicia o servidor em caso de encerramento inesperado.
    children = [{Messaging.Server, opts}]
    Supervisor.init(children, strategy: :one_for_one)
  end
end

defmodule Messaging.Demo do
  @moduledoc "Demonstração executável do sistema."

  def run do
    {:ok, _sup} = Messaging.System.start_link()

    Enum.each(["alice", "bob", "carol", "ops"], &Messaging.Server.register_user/1)

    :ok = Messaging.Server.subscribe("bob", self())
    :ok = Messaging.Server.subscribe("ops", self())

    # Assinante que será encerrado de forma inesperada.
    victim = spawn(fn -> Process.sleep(:infinity) end)
    :ok = Messaging.Server.subscribe("carol", victim)
    Process.exit(victim, :kill)

    IO.inspect(Messaging.Server.send_message("alice", "bob", "oi"), label: "1 entrega normal")
    IO.inspect(Messaging.Server.send_message("alice", "bob", ""), label: "2 erro de validação")
    IO.inspect(Messaging.Server.send_message("alice", "mallory", "oi"), label: "3 erro de validação")
    IO.inspect(Messaging.Server.send_message("alice", "bob", "olá"), label: "4 entrega normal")
    IO.inspect(Messaging.Server.send_message("alice", "bob", "URGENTE: reunião agora"), label: "5 dispara notificações")
    IO.inspect(Messaging.Server.send_message("alice", "bob", "x", :servidor_inexistente), label: "6 falha operacional")

    flush()
  end

  defp flush do
    receive do
      {:notification, notification} ->
        IO.inspect(notification, label: "notificação")
        flush()
    after
      300 -> :ok
    end
  end
end

Messaging.Demo.run()
