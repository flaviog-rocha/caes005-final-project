# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de entregas e rastreamento, atendendo somente aos seguintes requisitos:
#
# 1. Validar os dados recebidos antes de realizar operações.
# 2. Quando fizer sentido para o domínio, considerar a comunicação com um banco de dados fictício e, opcionalmente, o uso de Ecto.Migration, sem tornar isso obrigatório.
# 3. Recuperar processos após falhas inesperadas.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - DynamicSupervisor
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# entregas.exs
# Execução: elixir entregas.exs
#
# Gerenciamento de entregas e rastreamento com:
# - validação de dados antes de qualquer operação;
# - banco fictício persistente em disco (:dets, da biblioteca padrão), sem Ecto;
# - um processo GenServer por entrega, supervisionado por DynamicSupervisor;
# - recuperação de processos após falhas (reinício com estado recarregado do banco).

defmodule Delivery.Validator do
  # Status válidos de uma entrega
  @statuses [:pending, :in_transit, :delivered, :cancelled]
  @terminal [:delivered, :cancelled]

  # Transições permitidas (máquina de estados)
  @transitions %{
    pending: [:in_transit, :cancelled],
    in_transit: [:delivered, :cancelled],
    delivered: [],
    cancelled: []
  }

  @doc "Valida os atributos e monta um novo registro de entrega."
  def new(attrs) when is_map(attrs) do
    with {:ok, id} <- non_empty_string(attrs, :id),
         {:ok, origin} <- non_empty_string(attrs, :origin),
         {:ok, destination} <- non_empty_string(attrs, :destination),
         {:ok, weight} <- positive_number(attrs, :weight_kg) do
      now = DateTime.utc_now()

      {:ok,
       %{
         id: id,
         origin: origin,
         destination: destination,
         weight_kg: weight,
         status: :pending,
         events: [%{at: now, from: nil, to: :pending, note: "entrega criada"}]
       }}
    end
  end

  def new(_), do: {:error, :invalid_payload}

  @doc "Valida o identificador informado."
  def check_id(id) when is_binary(id) and byte_size(id) > 0, do: :ok
  def check_id(_), do: {:error, {:invalid, :id}}

  @doc "Valida se a mudança de status é permitida e retorna o registro atualizado."
  def transition(%{status: from} = record, to, note) do
    cond do
      not is_binary(note) ->
        {:error, {:invalid, :note}}

      not (to in @statuses) ->
        {:error, {:invalid_status, to}}

      to in Map.get(@transitions, from, []) ->
        event = %{at: DateTime.utc_now(), from: from, to: to, note: note}
        {:ok, %{record | status: to, events: record.events ++ [event]}}

      true ->
        {:error, {:invalid_transition, from, to}}
    end
  end

  @doc "Indica se o status é final (a entrega não possui mais processo ativo)."
  def terminal?(status), do: status in @terminal

  defp non_empty_string(attrs, key) do
    case Map.get(attrs, key) do
      value when is_binary(value) ->
        case String.trim(value) do
          "" -> {:error, {:invalid, key}}
          trimmed -> {:ok, trimmed}
        end

      _ ->
        {:error, {:invalid, key}}
    end
  end

  defp positive_number(attrs, key) do
    case Map.get(attrs, key) do
      value when is_number(value) and value > 0 -> {:ok, value}
      _ -> {:error, {:invalid, key}}
    end
  end
end

defmodule Delivery.Store do
  # Banco fictício persistente baseado em :dets (biblioteca padrão).
  # Os dados sobrevivem a falhas de processos e reinícios da VM.
  use GenServer

  @table :delivery_store

  def start_link(path), do: GenServer.start_link(__MODULE__, path, name: __MODULE__)

  def put(record), do: GenServer.call(__MODULE__, {:put, record})
  def get(id), do: GenServer.call(__MODULE__, {:get, id})
  def all, do: GenServer.call(__MODULE__, :all)

  @impl true
  def init(path) do
    case :dets.open_file(@table, file: String.to_charlist(path), type: :set) do
      {:ok, _ref} -> {:ok, :no_state}
      {:error, reason} -> {:stop, reason}
    end
  end

  @impl true
  def terminate(_reason, _state) do
    :dets.close(@table)
  end

  @impl true
  def handle_call({:put, record}, _from, state) do
    reply =
      case :dets.insert(@table, {record.id, record}) do
        :ok -> :ok
        {:error, reason} -> {:error, {:persistence_failed, reason}}
      end

    {:reply, reply, state}
  end

  def handle_call({:get, id}, _from, state) do
    reply =
      case :dets.lookup(@table, id) do
        [{_id, record}] -> {:ok, record}
        [] -> {:error, :not_found}
      end

    {:reply, reply, state}
  end

  def handle_call(:all, _from, state) do
    records = :dets.foldl(fn {_id, record}, acc -> [record | acc] end, [], @table)
    {:reply, records, state}
  end
end

defmodule Delivery.Server do
  # Um processo por entrega. Em caso de falha, o DynamicSupervisor o reinicia
  # e o init/1 recarrega o estado a partir do banco (recuperação).
  use GenServer, restart: :transient

  alias Delivery.{Store, Validator}

  def start_link(id), do: GenServer.start_link(__MODULE__, id, name: via(id))

  def via(id), do: {:via, Registry, {Delivery.Registry, id}}

  @impl true
  def init(id) do
    # Recuperação: o estado é sempre reconstruído a partir da fonte persistente
    case Store.get(id) do
      {:ok, record} -> {:ok, record}
      {:error, reason} -> {:stop, reason}
    end
  end

  @impl true
  def handle_call({:advance, to, note}, _from, record) do
    with {:ok, updated} <- Validator.transition(record, to, note),
         # Persiste antes de aplicar o novo estado em memória
         :ok <- Store.put(updated) do
      if Validator.terminal?(updated.status) do
        # Entrega finalizada: encerra o processo normalmente (transient não reinicia)
        {:stop, :normal, {:ok, updated}, updated}
      else
        {:reply, {:ok, updated}, updated}
      end
    else
      {:error, reason} -> {:reply, {:error, reason}, record}
    end
  end

  def handle_call(:snapshot, _from, record) do
    {:reply, {:ok, record}, record}
  end

  @impl true
  def handle_info(_msg, state), do: {:noreply, state}
end

defmodule Delivery.Application do
  # Árvore de supervisão: banco, registro de processos e DynamicSupervisor.
  @doc "Inicia o sistema e recupera entregas em andamento do banco."
  def start(path) do
    children = [
      {Delivery.Store, path},
      {Registry, keys: :unique, name: Delivery.Registry},
      {DynamicSupervisor, strategy: :one_for_one, name: Delivery.DynamicSup}
    ]

    with {:ok, sup} <- Supervisor.start_link(children, strategy: :rest_for_one) do
      Delivery.recover_all()
      {:ok, sup}
    end
  end
end

defmodule Delivery do
  # API pública: criação, atualização e rastreamento de entregas.
  alias Delivery.{Store, Validator, Server}

  @dynsup Delivery.DynamicSup
  @registry Delivery.Registry

  @doc "Cria uma entrega validada, persiste e inicia seu processo."
  def create(attrs) do
    with {:ok, record} <- Validator.new(attrs),
         :ok <- ensure_absent(record.id),
         :ok <- Store.put(record),
         {:ok, _pid} <- start_server(record.id) do
      {:ok, record}
    end
  end

  @doc "Atualiza o status de uma entrega (com validação de transição)."
  def update(id, status, note \\ "") do
    with :ok <- Validator.check_id(id),
         {:ok, pid} <- ensure_server(id) do
      safe_call(pid, {:advance, status, note})
    end
  end

  @doc "Consulta o estado atual e o histórico de eventos de uma entrega."
  def track(id) do
    with :ok <- Validator.check_id(id) do
      case Registry.lookup(@registry, id) do
        [{pid, _}] ->
          case safe_call(pid, :snapshot) do
            {:error, :unavailable} -> Store.get(id)
            reply -> reply
          end

        [] ->
          Store.get(id)
      end
    end
  end

  @doc "Reinicia processos de entregas que estavam em andamento antes de uma parada."
  def recover_all do
    Store.all()
    |> Enum.filter(&(&1.status in [:pending, :in_transit]))
    |> Enum.map(fn %{id: id} -> ensure_server(id) end)
  end

  defp ensure_absent(id) do
    case Store.get(id) do
      {:error, :not_found} -> :ok
      {:ok, _} -> {:error, :already_exists}
      {:error, reason} -> {:error, reason}
    end
  end

  defp ensure_server(id) do
    case Registry.lookup(@registry, id) do
      [{pid, _}] ->
        {:ok, pid}

      [] ->
        case Store.get(id) do
          {:ok, _record} -> start_server(id)
          {:error, reason} -> {:error, reason}
        end
    end
  end

  defp start_server(id) do
    case DynamicSupervisor.start_child(@dynsup, {Server, id}) do
      {:ok, pid} -> {:ok, pid}
      {:error, {:already_started, pid}} -> {:ok, pid}
      {:error, reason} -> {:error, reason}
    end
  end

  defp safe_call(pid, msg) do
    GenServer.call(pid, msg)
  catch
    :exit, _reason -> {:error, :unavailable}
  end
end

defmodule Delivery.Demo do
  # Demonstração: validação, ciclo de vida, falha simulada e recuperação.
  def run do
    path = Path.join(System.tmp_dir!(), "delivery_demo.dets")
    File.rm(path)

    {:ok, _sup} = Delivery.Application.start(path)

    # 1. Validação de dados
    IO.inspect(
      Delivery.create(%{id: "", origin: "SP", destination: "RJ", weight_kg: 10}),
      label: "payload inválido"
    )

    IO.inspect(
      Delivery.create(%{id: "E-1", origin: "SP", destination: "RJ", weight_kg: -3}),
      label: "peso inválido"
    )

    # 2. Criação válida
    {:ok, _record} =
      Delivery.create(%{id: "E-1", origin: "São Paulo", destination: "Rio de Janeiro", weight_kg: 12.5})

    IO.inspect(Delivery.create(%{id: "E-1", origin: "SP", destination: "RJ", weight_kg: 1}),
      label: "duplicada"
    )

    # 3. Transição inválida (pending -> delivered não é permitida)
    IO.inspect(Delivery.update("E-1", :delivered, "pulou etapa"), label: "transição inválida")

    # 4. Transição válida
    {:ok, _} = Delivery.update("E-1", :in_transit, "saiu do centro de distribuição")

    # 5. Falha inesperada: o processo é encerrado abruptamente
    [{pid, _}] = Registry.lookup(Delivery.Registry, "E-1")
    Process.exit(pid, :kill)
    Process.sleep(50)

    # 6. Recuperação automática pelo DynamicSupervisor com estado do banco
    {:ok, recovered} = Delivery.track("E-1")
    IO.inspect(recovered.status, label: "status após recuperação")
    IO.inspect(length(recovered.events), label: "eventos preservados")

    # 7. Finalização (encerra o processo; rastreamento continua via banco)
    {:ok, _} = Delivery.update("E-1", :delivered, "entregue ao destinatário")
    {:ok, final} = Delivery.track("E-1")

    Enum.each(final.events, fn e ->
      IO.puts("#{DateTime.to_iso8601(e.at)} #{inspect(e.from)} -> #{e.to} (#{e.note})")
    end)

    # 8. Entrega finalizada não aceita novas transições
    IO.inspect(Delivery.update("E-1", :in_transit), label: "após finalizada")
  end
end

Delivery.Demo.run()
