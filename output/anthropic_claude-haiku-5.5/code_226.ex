# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de usuários e sessões, atendendo somente aos seguintes requisitos:
#
# 1. Permitir a execução de operações simultâneas.
# 2. Permitir que diferentes entidades sejam processadas independentemente.
# 3. Impedir atualizações parcialmente concluídas.
# 4. Gerar notificações quando determinadas condições forem atingidas.
# 5. Permitir cadastrar, consultar, atualizar e remover registros.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule Gerenciador do
  @moduledoc """
  Gerenciamento de usuários e sessões com processamento concorrente.

  Arquitetura:
    * Cada entidade (usuário ou sessão) é um processo GenServer independente,
      registrado via `Registry` e supervisionado por um `DynamicSupervisor`.
    * Operações sobre uma mesma entidade são serializadas pelo seu processo,
      enquanto entidades diferentes são processadas em paralelo.
    * Atualizações são atômicas: as mudanças são aplicadas a uma cópia dos
      dados, validadas por completo e só então substituem o estado. Em caso de
      falha, o estado anterior permanece intacto.
    * Notificações são publicadas para assinantes (processos) quando condições
      são atingidas. Formato da mensagem recebida:
        {:gerenciador, tipo, id, evento}

  Eventos possíveis:
    * {:alert, :suspicious_activity}  - usuário atingiu o limite de falhas de login
    * {:notice, :user_blocked}        - usuário foi bloqueado
    * {:notice, :session_ended}       - sessão expirou ou foi revogada
    * {:notice, :deleted}             - entidade removida

  Assinatura:
    * `subscribe(:user, id)` assina eventos de uma entidade específica.
    * `subscribe(:user)` assina eventos de todas as entidades do tipo.

  Observação: o identificador `:all` é reservado para assinaturas globais.
  """

  use Supervisor

  @registry Gerenciador.Registry
  @subs Gerenciador.Subscriptions
  @sup Gerenciador.EntitySupervisor

  # ---------------------------------------------------------------------------
  # Supervisão
  # ---------------------------------------------------------------------------

  @doc "Inicia a árvore de supervisão do gerenciador."
  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    children = [
      {Registry, keys: :unique, name: @registry},
      {Registry, keys: :duplicate, name: @subs},
      {DynamicSupervisor, strategy: :one_for_one, name: @sup}
    ]

    Supervisor.init(children, strategy: :one_for_all)
  end

  # ---------------------------------------------------------------------------
  # API pública (CRUD)
  # ---------------------------------------------------------------------------

  @doc """
  Cadastra uma nova entidade (`:user` ou `:session`) com o `id` informado.
  Os atributos são validados antes de criar o processo.
  """
  def create(type, id, attrs) when type in [:user, :session] and is_map(attrs) do
    with {:ok, data} <- Gerenciador.Entity.build(type, attrs) do
      case DynamicSupervisor.start_child(@sup, {Gerenciador.Entity, {type, id, data}}) do
        {:ok, _pid} -> {:ok, data}
        {:error, {:already_started, _pid}} -> {:error, :already_exists}
        {:error, reason} -> {:error, reason}
      end
    end
  end

  @doc "Consulta uma entidade pelo tipo e id."
  def get(type, id), do: call(type, id, :get)

  @doc """
  Atualiza atomicamente os campos informados. Ou todas as alterações são
  aplicadas, ou nenhuma é.
  """
  def update(type, id, changes) when is_map(changes), do: call(type, id, {:update, changes})

  @doc "Remove a entidade, notificando os assinantes."
  def delete(type, id), do: call(type, id, :delete)

  # ---------------------------------------------------------------------------
  # Notificações
  # ---------------------------------------------------------------------------

  @doc """
  Assina notificações de uma entidade específica ou de todo um tipo
  (quando `id` é omitido). O processo chamador passa a receber as mensagens.
  """
  def subscribe(type, id \\ :all), do: Registry.register(@subs, {type, id}, nil)

  @doc "Cancela a assinatura realizada com `subscribe/2`."
  def unsubscribe(type, id \\ :all), do: Registry.unregister(@subs, {type, id})

  # ---------------------------------------------------------------------------
  # Auxiliares
  # ---------------------------------------------------------------------------

  defp call(type, id, msg) do
    GenServer.call(Gerenciador.Entity.via(type, id), msg)
  catch
    :exit, {:noproc, _} -> {:error, :not_found}
  end
end

defmodule Gerenciador.Entity do
  @moduledoc false
  # Processo que representa uma única entidade. Todas as operações sobre ela
  # são serializadas aqui, o que garante consistência sem bloquear outras
  # entidades.

  use GenServer, restart: :temporary

  @registry Gerenciador.Registry
  @subs Gerenciador.Subscriptions
  @user_statuses [:active, :blocked]
  @session_statuses [:active, :expired, :revoked]
  @max_failed_logins 3

  @defaults %{
    user: %{name: nil, email: nil, status: :active, failed_logins: 0},
    session: %{user_id: nil, status: :active, expires_at: nil}
  }

  # ---------------------------------------------------------------------------
  # Inicialização e registro
  # ---------------------------------------------------------------------------

  @doc false
  def via(type, id), do: {:via, Registry, {@registry, {type, id}}}

  @doc false
  def start_link({type, id, data}) do
    GenServer.start_link(__MODULE__, {type, id, data}, name: via(type, id))
  end

  @doc false
  # Monta e valida os dados de uma nova entidade.
  def build(type, attrs) do
    defaults = Map.fetch!(@defaults, type)

    case Map.keys(attrs) -- Map.keys(defaults) do
      [] -> validate(type, Map.merge(defaults, attrs))
      unknown -> {:error, {:unknown_fields, unknown}}
    end
  end

  @impl true
  def init({type, id, data}) do
    state = %{type: type, id: id, data: data}
    schedule_expiry(state)
    {:ok, state}
  end

  # ---------------------------------------------------------------------------
  # Callbacks síncronos
  # ---------------------------------------------------------------------------

  @impl true
  def handle_call(:get, _from, state), do: {:reply, {:ok, state.data}, state}

  def handle_call({:update, changes}, _from, state) do
    # Atomicidade: `commit/2` retorna um novo estado somente se TODAS as
    # alterações forem válidas. Caso contrário, o estado antigo é mantido.
    case commit(state, changes) do
      {:ok, new_state, events} ->
        publish(new_state, events)
        {:reply, {:ok, new_state.data}, new_state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  def handle_call(:delete, _from, state) do
    publish(state, [{:notice, :deleted}])
    {:stop, :normal, :ok, state}
  end

  # ---------------------------------------------------------------------------
  # Callbacks assíncronos (expiração automática de sessões)
  # ---------------------------------------------------------------------------

  @impl true
  def handle_info(:check_expiry, %{type: :session, data: %{status: :active, expires_at: at}} = state) do
    if now() >= at do
      case commit(state, %{status: :expired}) do
        {:ok, new_state, events} ->
          publish(new_state, events)
          {:noreply, new_state}

        {:error, _reason} ->
          {:noreply, state}
      end
    else
      {:noreply, state}
    end
  end

  def handle_info(:check_expiry, state), do: {:noreply, state}

  # ---------------------------------------------------------------------------
  # Validação
  # ---------------------------------------------------------------------------

  defp validate(:user, d) do
    cond do
      not (is_binary(d.name) and d.name != "") ->
        {:error, {:invalid, :name}}

      not (is_binary(d.email) and String.contains?(d.email, "@")) ->
        {:error, {:invalid, :email}}

      d.status not in @user_statuses ->
        {:error, {:invalid, :status}}

      not (is_integer(d.failed_logins) and d.failed_logins >= 0) ->
        {:error, {:invalid, :failed_logins}}

      true ->
        {:ok, d}
    end
  end

  defp validate(:session, d) do
    cond do
      is_nil(d.user_id) ->
        {:error, {:invalid, :user_id}}

      d.status not in @session_statuses ->
        {:error, {:invalid, :status}}

      not is_integer(d.expires_at) ->
        {:error, {:invalid, :expires_at}}

      true ->
        {:ok, d}
    end
  end

  # ---------------------------------------------------------------------------
  # Atualização atômica
  # ---------------------------------------------------------------------------

  # Aplica as mudanças sobre uma cópia dos dados, valida o resultado completo
  # e só então produz o novo estado. Não há escrita parcial possível.
  defp commit(state, changes) do
    allowed = Map.keys(Map.fetch!(@defaults, state.type))

    case Map.keys(changes) -- allowed do
      [] ->
        candidate = Map.merge(state.data, changes)

        case validate(state.type, candidate) do
          {:ok, valid} ->
            new_state = %{state | data: valid}
            schedule_expiry(new_state)
            {:ok, new_state, events(state.type, state.data, valid)}

          error ->
            error
        end

      unknown ->
        {:error, {:unknown_fields, unknown}}
    end
  end

  # ---------------------------------------------------------------------------
  # Regras de notificação (transições de condição)
  # ---------------------------------------------------------------------------

  # Eventos são gerados apenas na transição para a condição, evitando
  # notificações repetidas enquanto a condição permanece verdadeira.
  defp events(:user, old, new) do
    []
    |> add(
      new.failed_logins >= @max_failed_logins and old.failed_logins < @max_failed_logins,
      {:alert, :suspicious_activity}
    )
    |> add(new.status == :blocked and old.status != :blocked, {:notice, :user_blocked})
  end

  defp events(:session, old, new) do
    []
    |> add(new.status != :active and old.status == :active, {:notice, :session_ended})
  end

  defp add(list, true, event), do: list ++ [event]
  defp add(list, false, _event), do: list

  # ---------------------------------------------------------------------------
  # Publicação de notificações e expiração
  # ---------------------------------------------------------------------------

  defp publish(%{type: type, id: id}, events) do
    for event <- events, key <- [{type, id}, {type, :all}] do
      Registry.dispatch(@subs, key, fn entries ->
        for {pid, _} <- entries, do: send(pid, {:gerenciador, type, id, event})
      end)
    end

    :ok
  end

  defp schedule_expiry(%{type: :session, data: %{status: :active, expires_at: at}}) do
    delay_ms = max(at - now(), 0) * 1000
    Process.send_after(self(), :check_expiry, delay_ms)
  end

  defp schedule_expiry(_state), do: :ok

  defp now, do: System.system_time(:second)
end
