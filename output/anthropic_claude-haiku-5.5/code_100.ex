# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de entregas e rastreamento, atendendo somente aos seguintes requisitos:
#
# 1. Recuperar processos após falhas inesperadas.
# 2. Persistir os dados para recuperação após reinicializações.
# 3. Diferenciar erros de validação de falhas operacionais.
# 4. Garantir a consistência dos dados durante operações concorrentes.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - Supervisor
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# Arquivo único: gerenciamento de entregas e rastreamento com recuperação,
# persistência em disco, separação de erros e consistência concorrente.
#
# Execução de exemplo (iex -S mix ou elixir arquivo.exs):
#   {:ok, _} = Delivery.start_link(path: "/tmp/deliveries.snapshot")
#   {:ok, d} = Delivery.create(%{id: "E-1", address: "Rua A, 10"})
#   {:ok, d} = Delivery.transition("E-1", :in_transit, d.version)
#   {:ok, d} = Delivery.track("E-1", "Centro de distribuição")

# ---------------------------------------------------------------------------
# Regras de negócio puras (sem efeitos colaterais).
# Toda entrada inválida retorna {:error, {:validation, detalhes}}.
# ---------------------------------------------------------------------------
defmodule Delivery.Validation do
  @moduledoc false

  @statuses [:pending, :in_transit, :delivered, :cancelled]

  # Máquina de estados: transições permitidas a partir de cada status.
  @transitions %{
    pending: [:in_transit, :cancelled],
    in_transit: [:delivered, :cancelled],
    delivered: [],
    cancelled: []
  }

  @doc "Valida atributos e constrói uma nova entrega no estado inicial."
  def new_delivery(attrs) when is_map(attrs) do
    errors =
      []
      |> require_non_empty(attrs, :id)
      |> require_non_empty(attrs, :address)

    case errors do
      [] ->
        now = now()

        {:ok,
         %{
           id: attrs.id,
           address: attrs.address,
           status: :pending,
           version: 1,
           events: [],
           inserted_at: now,
           updated_at: now
         }}

      _ ->
        {:error, {:validation, Enum.reverse(errors)}}
    end
  end

  def new_delivery(_), do: {:error, {:validation, [{:attrs, :must_be_a_map}]}}

  @doc """
  Aplica uma transição de status. A versão esperada implementa controle
  otimista: se outra escrita ocorreu antes, a operação é rejeitada.
  """
  def transition(%{} = delivery, new_status, expected_version) do
    cond do
      expected_version != delivery.version ->
        {:error, {:validation, [{:version, :conflict}]}}

      new_status not in Map.fetch!(@transitions, delivery.status) ->
        {:error,
         {:validation, [{:status, {:invalid_transition, delivery.status, new_status}}]}}

      true ->
        {:ok,
         %{delivery | status: new_status, version: delivery.version + 1, updated_at: now()}}
    end
  end

  @doc "Acrescenta um evento de rastreamento à entrega."
  def track(%{} = delivery, location) when is_binary(location) and byte_size(location) > 0 do
    if delivery.status in [:delivered, :cancelled] do
      {:error, {:validation, [{:status, {:closed, delivery.status}}]}}
    else
      event = %{at: now(), location: location}

      {:ok,
       %{
         delivery
         | events: [event | delivery.events],
           version: delivery.version + 1,
           updated_at: event.at
       }}
    end
  end

  def track(_delivery, _location),
    do: {:error, {:validation, [{:location, :must_be_non_empty_string}]}}

  def statuses, do: @statuses

  defp require_non_empty(errors, attrs, key) do
    case Map.get(attrs, key) do
      value when is_binary(value) and byte_size(value) > 0 -> errors
      _ -> [{key, :must_be_non_empty_string} | errors]
    end
  end

  defp now, do: System.system_time(:millisecond)
end

# ---------------------------------------------------------------------------
# Persistência em disco com escrita atômica (arquivo temporário + rename)
# e sincronização com o dispositivo. Falhas de I/O são operacionais.
# ---------------------------------------------------------------------------
defmodule Delivery.Storage do
  @moduledoc false

  @doc "Carrega o snapshot. Arquivo inexistente equivale a um estado vazio."
  def load(path) do
    case File.read(path) do
      {:ok, bin} ->
        try do
          {:ok, :erlang.binary_to_term(bin, [:safe])}
        rescue
          ArgumentError -> {:error, {:operational, :corrupted_snapshot}}
        end

      {:error, :enoent} ->
        {:ok, %{}}

      {:error, reason} ->
        {:error, {:operational, reason}}
    end
  end

  @doc "Grava o snapshot de forma atômica: ou o arquivo novo está completo, ou o antigo permanece."
  def save(path, data) do
    tmp = path <> ".tmp"
    bin = :erlang.term_to_binary(data)

    with :ok <- write_sync(tmp, bin),
         :ok <- File.rename(tmp, path) do
      :ok
    else
      {:error, reason} -> {:error, {:operational, reason}}
    end
  end

  defp write_sync(path, bin) do
    case :file.open(path, [:write, :binary]) do
      {:ok, io} ->
        result =
          with :ok <- :file.write(io, bin),
               :ok <- :file.sync(io),
               do: :ok

        _ = :file.close(io)
        result

      {:error, _} = error ->
        error
    end
  end
end

# ---------------------------------------------------------------------------
# Processo dono do estado. Todas as mutações são serializadas pela mensagem
# do GenServer, o que garante consistência em operações concorrentes.
# Persiste antes de confirmar: se a gravação falhar, o estado não muda.
# ---------------------------------------------------------------------------
defmodule Delivery.Store do
  @moduledoc false
  use GenServer

  def start_link(opts) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  def create(server \\ __MODULE__, attrs), do: call(server, {:create, attrs})

  def transition(server \\ __MODULE__, id, status, expected_version),
    do: call(server, {:transition, id, status, expected_version})

  def track(server \\ __MODULE__, id, location), do: call(server, {:track, id, location})

  def fetch(server \\ __MODULE__, id), do: call(server, {:fetch, id})

  def list(server \\ __MODULE__), do: call(server, :list)

  # Converte indisponibilidade do processo (crash, timeout) em erro operacional
  # para o chamador, sem derrubar o cliente.
  defp call(server, msg) do
    GenServer.call(server, msg)
  catch
    :exit, reason -> {:error, {:operational, {:unavailable, reason}}}
  end

  @impl true
  def init(opts) do
    path = Keyword.fetch!(opts, :path)

    # Recuperação: o estado é reconstruído a partir do último snapshot válido.
    case Delivery.Storage.load(path) do
      {:ok, deliveries} -> {:ok, %{path: path, deliveries: deliveries}}
      {:error, reason} -> {:stop, reason}
    end
  end

  @impl true
  def handle_call({:create, attrs}, _from, state) do
    case Delivery.Validation.new_delivery(attrs) do
      {:error, _} = error ->
        {:reply, error, state}

      {:ok, delivery} ->
        if Map.has_key?(state.deliveries, delivery.id) do
          {:reply, {:error, {:validation, [{:id, :already_exists}]}}, state}
        else
          commit(state, Map.put(state.deliveries, delivery.id, delivery), {:ok, delivery})
        end
    end
  end

  def handle_call({:transition, id, status, version}, _from, state) do
    with {:ok, delivery} <- fetch_delivery(state, id),
         {:ok, updated} <- Delivery.Validation.transition(delivery, status, version) do
      commit(state, Map.put(state.deliveries, id, updated), {:ok, updated})
    else
      {:error, _} = error -> {:reply, error, state}
    end
  end

  def handle_call({:track, id, location}, _from, state) do
    with {:ok, delivery} <- fetch_delivery(state, id),
         {:ok, updated} <- Delivery.Validation.track(delivery, location) do
      commit(state, Map.put(state.deliveries, id, updated), {:ok, updated})
    else
      {:error, _} = error -> {:reply, error, state}
    end
  end

  def handle_call({:fetch, id}, _from, state) do
    {:reply, fetch_delivery(state, id), state}
  end

  def handle_call(:list, _from, state) do
    {:reply, {:ok, Map.values(state.deliveries)}, state}
  end

  # Persiste o novo estado antes de substituí-lo em memória.
  defp commit(state, new_deliveries, reply) do
    case Delivery.Storage.save(state.path, new_deliveries) do
      :ok -> {:reply, reply, %{state | deliveries: new_deliveries}}
      {:error, _} = error -> {:reply, error, state}
    end
  end

  defp fetch_delivery(state, id) do
    case Map.fetch(state.deliveries, id) do
      {:ok, delivery} -> {:ok, delivery}
      :error -> {:error, {:validation, [{:id, :not_found}]}}
    end
  end
end

# ---------------------------------------------------------------------------
# Supervisor: reinicia o Store após falhas inesperadas. Ao reiniciar,
# o Store recarrega o snapshot persistido (recuperação).
# ---------------------------------------------------------------------------
defmodule Delivery.Supervisor do
  @moduledoc false
  use Supervisor

  def start_link(opts) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    children = [
      {Delivery.Store, opts}
    ]

    Supervisor.init(children, strategy: :one_for_one, max_restarts: 5, max_seconds: 10)
  end
end

# ---------------------------------------------------------------------------
# Interface pública do módulo de entregas.
# ---------------------------------------------------------------------------
defmodule Delivery do
  @moduledoc false

  @doc "Inicia o sistema sob supervisão. Opções: `path:` (arquivo de snapshot)."
  def start_link(opts) when is_list(opts) do
    Delivery.Supervisor.start_link(opts)
  end

  defdelegate create(attrs), to: Delivery.Store
  defdelegate transition(id, status, expected_version), to: Delivery.Store
  defdelegate track(id, location), to: Delivery.Store
  defdelegate fetch(id), to: Delivery.Store
  defdelegate list(), to: Delivery.Store
end
