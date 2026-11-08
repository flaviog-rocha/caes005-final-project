# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de gerenciamento de notificações, atendendo somente aos seguintes requisitos:
#
# 1. Implementar novas tentativas após falhas temporárias.
# 2. Recuperar processos após falhas inesperadas.
# 3. Permitir que diferentes entidades sejam processadas independentemente.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# Arquivo único: notifications.ex
#
# Para executar, declare no mix.exs:
#   def application, do: [mod: {Notifications.Application, []}, extra_applications: [:logger]]
#
# Uso:
#   Notifications.notify("cliente_42", %{titulo: "Olá", corpo: "Seu pedido foi enviado"})

defmodule Notifications do
  @moduledoc """
  API pública do sistema de notificações.

  Garantias:
    * Cada entidade (`entity_id`) tem seu próprio processo, isolado dos demais.
    * Falhas temporárias são reenviadas com backoff exponencial, preservando a ordem
      das notificações dentro de cada entidade.
    * Estado pendente fica em ETS, de propriedade de um processo separado; assim, se o
      worker de uma entidade falhar, o supervisor o reinicia e ele retoma de onde parou.
  """

  alias Notifications.{EntitySupervisor, EntityWorker, Store}

  @doc "Enfileira uma notificação para a entidade informada."
  def notify(entity_id, payload) do
    # Persiste antes de acionar o worker: mesmo que o worker caia, a notificação não se perde.
    :ok = Store.enqueue(entity_id, %{payload: payload, attempts: 0})

    # Garante que exista exatamente um worker para essa entidade.
    {:ok, _pid} = EntitySupervisor.ensure_worker(entity_id)
    EntityWorker.wake(entity_id)
    :ok
  end
end

defmodule Notifications.Application do
  @moduledoc false
  use Application

  @impl true
  def start(_type, _args) do
    children = [
      # Registro de processos por entidade (nome único -> pid).
      {Registry, keys: :unique, name: Notifications.Registry},
      # Dono da tabela ETS com o estado pendente.
      Notifications.Store,
      # Supervisor dinâmico com um worker por entidade.
      Notifications.EntitySupervisor
    ]

    # :rest_for_one: se o Store reiniciar (e perder o ETS), os workers também são reiniciados
    # para não operarem com referências inválidas.
    Supervisor.start_link(children, strategy: :rest_for_one, name: Notifications.Supervisor)
  end
end

defmodule Notifications.Store do
  @moduledoc """
  Armazena notificações pendentes em uma tabela ETS `:ordered_set` com chave `{entity_id, seq}`.

  O processo do Store é apenas o dono da tabela; a tabela é `:public` para que os workers
  leiam e escrevam sem gargalo de mensagens. Como o dono é este processo, o estado
  sobrevive a falhas dos workers.
  """
  use GenServer

  @table :notifications_store

  def start_link(_opts \\ []) do
    GenServer.start_link(__MODULE__, nil, name: __MODULE__)
  end

  @doc "Adiciona uma nova notificação ao final da fila da entidade."
  def enqueue(entity_id, notification) do
    seq = :erlang.unique_integer([:monotonic, :positive])
    put(entity_id, seq, notification)
  end

  @doc "Grava (ou sobrescreve) uma notificação em uma posição específica."
  def put(entity_id, seq, notification) do
    true = :ets.insert(@table, {{entity_id, seq}, notification})
    :ok
  end

  @doc "Remove uma notificação concluída ou descartada."
  def delete(entity_id, seq) do
    true = :ets.delete(@table, {entity_id, seq})
    :ok
  end

  @doc "Lista as notificações pendentes da entidade, em ordem de chegada."
  def pending(entity_id) do
    @table
    |> :ets.match_object({{entity_id, :_}, :_})
    |> Enum.map(fn {{_id, seq}, notification} -> {seq, notification} end)
    |> Enum.sort_by(fn {seq, _} -> seq end)
  end

  @impl true
  def init(nil) do
    :ets.new(@table, [:named_table, :public, :ordered_set, read_concurrency: true, write_concurrency: true])
    {:ok, nil}
  end
end

defmodule Notifications.EntitySupervisor do
  @moduledoc """
  Supervisor dinâmico que mantém um worker por entidade.
  Cada worker é independente: a falha de um não afeta os demais.
  """
  use DynamicSupervisor

  def start_link(_opts \\ []) do
    DynamicSupervisor.start_link(__MODULE__, nil, name: __MODULE__)
  end

  @doc "Inicia o worker da entidade, caso ainda não esteja ativo."
  def ensure_worker(entity_id) do
    case DynamicSupervisor.start_child(__MODULE__, {Notifications.EntityWorker, entity_id}) do
      {:ok, pid} -> {:ok, pid}
      {:error, {:already_started, pid}} -> {:ok, pid}
      other -> other
    end
  end

  @impl true
  def init(nil) do
    # Limite de reinícios alto: a falha de uma entidade não deve derrubar as outras.
    DynamicSupervisor.init(strategy: :one_for_one, max_restarts: 50, max_seconds: 5)
  end
end

defmodule Notifications.EntityWorker do
  @moduledoc """
  Worker responsável pela fila de uma única entidade.

  Comportamento:
    * Processa as notificações em ordem, uma por vez.
    * Em falha temporária, reagenda a tentativa com backoff exponencial.
    * Em falha permanente, ou após esgotar as tentativas, descarta a notificação.
    * Ao ser reiniciado pelo supervisor, recarrega a fila do Store e retoma o processamento.

  Observação de semântica: a entrega é "pelo menos uma vez". Se o worker cair durante
  um envio, a tentativa será repetida após a recuperação. O contador de tentativas é
  persistido antes do envio, então falhas recorrentes que derrubam o worker também
  consomem tentativas e não geram laço infinito.
  """
  use GenServer
  require Logger

  alias Notifications.{Delivery, Store}

  @max_attempts 5
  @base_backoff_ms 200
  @max_backoff_ms 30_000

  def start_link(entity_id) do
    GenServer.start_link(__MODULE__, entity_id, name: via(entity_id))
  end

  @doc "Sinaliza ao worker que há trabalho novo na fila."
  def wake(entity_id) do
    GenServer.cast(via(entity_id), :wake)
  end

  defp via(entity_id), do: {:via, Registry, {Notifications.Registry, entity_id}}

  @impl true
  def init(entity_id) do
    # Recuperação: o estado real está no Store, então nada precisa ser restaurado em memória.
    {:ok, %{entity_id: entity_id, waiting?: false}, {:continue, :drain}}
  end

  @impl true
  def handle_continue(:drain, state), do: drain(state)

  @impl true
  def handle_cast(:wake, %{waiting?: true} = state) do
    # Há um retry agendado; respeitamos o backoff e não antecipamos a tentativa.
    {:noreply, state}
  end

  def handle_cast(:wake, state), do: drain(state)

  @impl true
  def handle_info(:retry, state) do
    drain(%{state | waiting?: false})
  end

  def handle_info(_msg, state), do: {:noreply, state}

  # Processa a cabeça da fila até esvaziá-la ou até agendar um retry.
  defp drain(%{entity_id: entity_id} = state) do
    case Store.pending(entity_id) do
      [] ->
        {:noreply, state}

      [{seq, notification} | _] ->
        attempt = notification.attempts + 1
        notification = %{notification | attempts: attempt}

        # Persiste a tentativa antes do envio para que quedas do processo sejam contabilizadas.
        :ok = Store.put(entity_id, seq, notification)

        case Delivery.deliver(entity_id, notification) do
          :ok ->
            :ok = Store.delete(entity_id, seq)
            drain(state)

          {:error, :permanent, reason} ->
            Logger.error("[#{entity_id}] falha permanente, descartando: #{inspect(reason)}")
            :ok = Store.delete(entity_id, seq)
            drain(state)

          {:error, :temporary, reason} ->
            handle_temporary_failure(state, seq, attempt, reason)
        end
    end
  end

  defp handle_temporary_failure(state, seq, attempt, reason) do
    if attempt >= @max_attempts do
      Logger.error("[#{state.entity_id}] tentativas esgotadas (#{attempt}), descartando: #{inspect(reason)}")
      :ok = Store.delete(state.entity_id, seq)
      drain(state)
    else
      delay = backoff(attempt)

      Logger.warning(
        "[#{state.entity_id}] falha temporária (#{inspect(reason)}), " <>
          "nova tentativa #{attempt + 1}/#{@max_attempts} em #{delay}ms"
      )

      Process.send_after(self(), :retry, delay)
      {:noreply, %{state | waiting?: true}}
    end
  end

  # Backoff exponencial limitado: 200ms, 400ms, 800ms, ...
  defp backoff(attempt) do
    min(@base_backoff_ms * Integer.pow(2, attempt - 1), @max_backoff_ms)
  end
end

defmodule Notifications.Delivery do
  @moduledoc """
  Camada de entrega. Por padrão simula envios com falhas aleatórias.
  Pode ser substituída configurando `config :notifications, deliver_fun: fn entity_id, notification -> ... end`.
  Deve retornar `:ok`, `{:error, :temporary, reason}` ou `{:error, :permanent, reason}`.
  """

  def deliver(entity_id, notification) do
    fun = Application.get_env(:notifications, :deliver_fun, &default_deliver/2)
    fun.(entity_id, notification)
  end

  defp default_deliver(_entity_id, _notification) do
    case :rand.uniform(100) do
      n when n <= 70 -> :ok
      n when n <= 95 -> {:error, :temporary, :timeout}
      _ -> {:error, :permanent, :invalid_recipient}
    end
  end
end
