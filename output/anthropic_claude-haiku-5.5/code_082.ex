# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de gerenciamento de notificações, atendendo somente aos seguintes requisitos:
#
# 1. Permitir que diferentes entidades sejam processadas independentemente.
# 2. Implementar novas tentativas após falhas temporárias.
# 3. Processar grandes volumes de dados sem carregá-los integralmente na memória.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - DETS
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# Sistema de gerenciamento de notificações com DETS.
#
# Requisitos atendidos:
# 1. Cada entidade possui um processo worker próprio (isolamento e paralelismo).
# 2. Falhas temporárias são reenviadas com backoff exponencial e jitter.
# 3. O DETS é consumido em lotes limitados (select com limite), sem carregar tudo na memória.
#
# Uso:
#   {:ok, _} = Notifications.start_link(
#     path: "notifications.dets",
#     sender: fn payload -> if enviar(payload), do: :ok, else: {:error, :transient} end
#   )
#   {:ok, _key} = Notifications.enqueue("cliente_42", %{msg: "olá"})
#
# O sender deve retornar :ok, {:error, :transient} (reenvia) ou {:error, :permanent} (vai para dead-letter).

defmodule Notifications do
  @moduledoc false
  use Supervisor

  alias Notifications.Store

  @config_key {__MODULE__, :config}

  @defaults %{
    path: "notifications.dets",
    max_attempts: 5,
    base_backoff: 1_000,
    max_backoff: 60_000,
    batch_size: 100,
    poll_interval: 500
  }

  def start_link(opts) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc "Enfileira uma notificação para uma entidade. Retorna a chave persistida."
  def enqueue(entity, payload) do
    key = {entity, System.unique_integer([:positive, :monotonic])}
    :ok = Store.put({key, :pending, 0, now_ms(), payload})
    ensure_worker(entity)
    {:ok, key}
  end

  @impl true
  def init(opts) do
    _sender = Keyword.fetch!(opts, :sender)
    cfg = Map.merge(@defaults, Map.new(opts))
    :persistent_term.put(@config_key, cfg)

    children = [
      {Store, cfg.path},
      {Registry, keys: :unique, name: Notifications.Registry},
      {DynamicSupervisor, strategy: :one_for_one, name: Notifications.EntitySupervisor}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  @doc false
  def config, do: :persistent_term.get(@config_key)

  @doc false
  def now_ms, do: System.system_time(:millisecond)

  defp ensure_worker(entity) do
    spec = {Notifications.EntityWorker, {entity, config()}}

    case DynamicSupervisor.start_child(Notifications.EntitySupervisor, spec) do
      {:ok, _pid} -> :ok
      {:error, {:already_started, _pid}} -> :ok
    end
  end
end

defmodule Notifications.Store do
  @moduledoc false
  # Dono da tabela DETS. Registros têm o formato:
  #   {{entity, id}, status, attempts, next_at_ms, payload}
  # status: :pending | :retry | :dead

  use GenServer

  @table :notifications_dets

  def start_link(path), do: GenServer.start_link(__MODULE__, path, name: __MODULE__)

  @impl true
  def init(path) do
    # Garante que terminate/2 seja chamado para fechar o arquivo DETS corretamente.
    Process.flag(:trap_exit, true)

    case :dets.open_file(@table, file: String.to_charlist(path), type: :set) do
      {:ok, @table} -> {:ok, nil}
      {:error, reason} -> {:stop, reason}
    end
  end

  @impl true
  def terminate(_reason, _state) do
    :dets.close(@table)
  end

  @doc "Insere ou substitui um registro (escrita direta na tabela DETS)."
  def put(record), do: :dets.insert(@table, record)

  @doc "Remove um registro."
  def delete(key), do: :dets.delete(@table, key)

  @doc """
  Retorna no máximo `n` registros prontos para processamento de uma entidade.
  Usa select com limite, então apenas um lote é mantido em memória.
  """
  def ready_batch(entity, now, n) do
    ms = [
      {
        {{:"$1", :_}, :"$2", :_, :"$3", :_},
        [
          {:==, :"$1", {:const, entity}},
          {:"=<", :"$3", now},
          {:orelse, {:==, :"$2", :pending}, {:==, :"$2", :retry}}
        ],
        [:"$_"]
      }
    ]

    take(:dets.select(@table, ms, n))
  end

  # Consome continuações até obter ao menos um resultado ou fim da tabela.
  defp take(:"$end_of_table"), do: []
  defp take({[], cont}), do: take(:dets.select(cont))
  defp take({records, _cont}), do: records
end

defmodule Notifications.EntityWorker do
  @moduledoc false
  # Um processo por entidade: processa suas notificações de forma independente.

  use GenServer

  alias Notifications.Store

  def start_link({entity, cfg}) do
    GenServer.start_link(__MODULE__, {entity, cfg}, name: via(entity))
  end

  defp via(entity), do: {:via, Registry, {Notifications.Registry, entity}}

  @impl true
  def init({entity, cfg}) do
    send(self(), :tick)
    {:ok, {entity, cfg}}
  end

  @impl true
  def handle_info(:tick, {entity, cfg} = state) do
    drain(entity, cfg)
    Process.send_after(self(), :tick, cfg.poll_interval)
    {:noreply, state}
  end

  # Processa lotes sucessivos até não haver mais registros prontos.
  defp drain(entity, cfg) do
    batch = Store.ready_batch(entity, Notifications.now_ms(), cfg.batch_size)

    Enum.each(batch, &process(&1, cfg))

    case batch do
      [] -> :ok
      _ -> drain(entity, cfg)
    end
  end

  defp process({key, _status, attempts, _next_at, payload}, cfg) do
    case run(cfg.sender, payload) do
      :ok ->
        Store.delete(key)

      {:error, :permanent} ->
        Store.put({key, :dead, attempts + 1, Notifications.now_ms(), payload})

      _transient ->
        n = attempts + 1

        if n >= cfg.max_attempts do
          Store.put({key, :dead, n, Notifications.now_ms(), payload})
        else
          next_at = Notifications.now_ms() + backoff(n, cfg)
          Store.put({key, :retry, n, next_at, payload})
        end
    end
  end

  # Executa o sender isolando exceções e saídas como falhas temporárias.
  defp run(sender, payload) do
    sender.(payload)
  rescue
    _ -> {:error, :transient}
  catch
    _, _ -> {:error, :transient}
  end

  # Backoff exponencial limitado com jitter de até 10%.
  defp backoff(n, cfg) do
    delay = min(cfg.base_backoff * Integer.pow(2, n - 1), cfg.max_backoff)
    delay + :rand.uniform(max(div(delay, 10), 1))
  end
end
