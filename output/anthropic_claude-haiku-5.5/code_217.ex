# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de entregas e rastreamento, atendendo somente aos seguintes requisitos:
#
# 1. Validar os dados recebidos antes de realizar operações.
# 2. Implementar expiração automática de registros.
# 3. Recuperar processos após falhas inesperadas.
# 4. Fazer uso de macros.
# 5. Manter um histórico das operações realizadas.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# Arquivo único: gerenciamento de entregas e rastreamento em Elixir.
# Requisitos atendidos:
#   1. Validação dos dados antes de qualquer operação (DeliveryTracker.Validator)
#   2. Expiração automática de registros (varredura periódica + checagem em leitura)
#   3. Recuperação após falhas (Supervisor rest_for_one + estado em ETS fora do worker)
#   4. Uso de macros (ensure/2 e defop/3 em DeliveryTracker.Macros)
#   5. Histórico das operações (tabela ETS ordenada, com limite de tamanho)
#
# Somente biblioteca padrão: GenServer, Supervisor, ETS e System.

defmodule DeliveryTracker.Macros do
  @moduledoc """
  Macros utilitárias do rastreador de entregas.

    * `ensure/2` - expande uma condição em `:ok` ou `{:error, erro}`, para uso com `with`.
    * `defop/3`  - define um handle_call que executa a operação, registra no histórico
                   e responde ao cliente, eliminando código repetitivo.
  """

  defmacro __using__(_opts) do
    quote do
      import DeliveryTracker.Macros
    end
  end

  @doc """
  Retorna `:ok` quando `condition` for verdadeira, caso contrário `{:error, error}`.
  """
  defmacro ensure(condition, error) do
    quote do
      if unquote(condition), do: :ok, else: {:error, unquote(error)}
    end
  end

  @doc """
  Define um clause de `handle_call/3` para a operação `name`.

  O corpo deve retornar `{reply, new_state}`. A macro registra automaticamente
  a operação no histórico do servidor antes de responder.

  Exemplo:

      defop :create, [attrs] do
        {reply, state}
      end
  """
  defmacro defop(name, args, do: body) do
    # Variável `state` sem contexto de hygiene, para que o corpo informado
    # pelo chamador consiga referenciá-la.
    state = Macro.var(:state, nil)

    quote do
      def handle_call({unquote(name), unquote_splicing(args)}, _from, unquote(state)) do
        {reply, new_state} = unquote(body)
        new_state = DeliveryTracker.Server.record(new_state, unquote(name), reply)
        {:reply, reply, new_state}
      end
    end
  end
end

defmodule DeliveryTracker.Validator do
  @moduledoc """
  Validações puras dos dados de entrada e das transições de status.
  Nenhuma operação com estado é realizada antes de passar por aqui.
  """

  import DeliveryTracker.Macros

  @statuses [:pending, :in_transit, :delivered, :cancelled]

  @transitions %{
    pending: [:in_transit, :cancelled],
    in_transit: [:delivered, :cancelled],
    delivered: [],
    cancelled: []
  }

  @default_ttl 3_600_000
  @max_ttl 7 * 24 * 3_600_000

  @doc "Valida os atributos de criação de uma entrega."
  def validate_new(attrs) when is_map(attrs) do
    id = Map.get(attrs, :id)
    destination = Map.get(attrs, :destination)
    ttl = Map.get(attrs, :ttl_ms, @default_ttl)

    with :ok <- validate_id(id),
         :ok <- ensure(non_empty_string?(destination), {:invalid, :destination}),
         :ok <- ensure(is_integer(ttl) and ttl > 0 and ttl <= @max_ttl, {:invalid, :ttl_ms}) do
      {:ok, %{id: id, destination: destination, ttl_ms: ttl}}
    end
  end

  def validate_new(_), do: {:error, {:invalid, :attrs}}

  @doc "Valida o identificador de uma entrega."
  def validate_id(id) do
    ensure(is_binary(id) and byte_size(id) > 0, {:invalid, :id})
  end

  @doc "Valida se a transição de `current` para `next` é permitida."
  def validate_transition(current, next) do
    with :ok <- ensure(next in @statuses, {:invalid, :status}),
         :ok <-
           ensure(
             next in Map.get(@transitions, current, []),
             {:forbidden_transition, current, next}
           ) do
      {:ok, next}
    end
  end

  defp non_empty_string?(s), do: is_binary(s) and String.trim(s) != ""
end

defmodule DeliveryTracker.Storage do
  @moduledoc """
  Dono das tabelas ETS. Como os dados vivem aqui, e não no worker,
  uma falha do `DeliveryTracker.Server` não causa perda de estado.
  """

  use GenServer

  @records :delivery_tracker_records
  @history :delivery_tracker_history
  @meta :delivery_tracker_meta

  def start_link(_opts \\ []) do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    :ets.new(@records, [:set, :public, :named_table, read_concurrency: true])
    :ets.new(@history, [:ordered_set, :public, :named_table])
    :ets.new(@meta, [:set, :public, :named_table])
    :ets.insert(@meta, {:boots, 0})
    {:ok, %{}}
  end
end

defmodule DeliveryTracker.Server do
  @moduledoc """
  Worker que mantém entregas, aplica transições validadas, expira registros
  automaticamente e registra o histórico de operações.
  """

  use GenServer
  use DeliveryTracker.Macros

  alias DeliveryTracker.Validator

  @records :delivery_tracker_records
  @history :delivery_tracker_history
  @meta :delivery_tracker_meta
  @sweep_interval 1_000
  @max_history 1_000

  # ---------------------------------------------------------------------------
  # API pública
  # ---------------------------------------------------------------------------

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc "Cria uma entrega. Atributos: `:id`, `:destination` e opcionalmente `:ttl_ms`."
  def create(attrs), do: GenServer.call(__MODULE__, {:create, attrs})

  @doc "Atualiza o status de uma entrega seguindo as transições permitidas."
  def update_status(id, status), do: GenServer.call(__MODULE__, {:update_status, id, status})

  @doc "Consulta uma entrega ainda não expirada."
  def fetch(id), do: GenServer.call(__MODULE__, {:fetch, id})

  @doc "Retorna as últimas `limit` operações registradas, da mais antiga para a mais recente."
  def history(limit \\ 50) when is_integer(limit) and limit > 0 do
    @history
    |> :ets.tab2list()
    |> Enum.take(-limit)
    |> Enum.map(fn {_key, entry} -> entry end)
  end

  @doc """
  Registra uma entrada no histórico. Chamada pela macro `defop/3` e pelos
  eventos internos (expiração e recuperação). Retorna o estado sem alterações.
  """
  def record(state, op, result) do
    key = :erlang.unique_integer([:monotonic, :positive])
    :ets.insert(@history, {key, %{op: op, result: result, at: now()}})

    if :ets.info(@history, :size) > @max_history do
      :ets.delete(@history, :ets.first(@history))
    end

    state
  end

  # ---------------------------------------------------------------------------
  # Callbacks
  # ---------------------------------------------------------------------------

  @impl true
  def init(opts) do
    state = %{sweep_interval: Keyword.get(opts, :sweep_interval, @sweep_interval)}

    # Cada inicialização incrementa o contador. Valores acima de 1 indicam
    # que o processo foi reiniciado pelo supervisor após uma falha.
    case :ets.update_counter(@meta, :boots, 1) do
      1 -> :ok
      n -> record(state, :recovered, {:restarts, n - 1})
    end

    Process.send_after(self(), :sweep, state.sweep_interval)
    {:ok, state}
  end

  @impl true
  def handle_call({:fetch, id}, _from, state) do
    {:reply, fetch_live(id), state}
  end

  defop :create, [attrs] do
    reply =
      with {:ok, data} <- Validator.validate_new(attrs),
           :ok <- ensure(not live?(data.id), {:conflict, :already_exists}) do
        insert_new(data)
      end

    {reply, state}
  end

  defop :update_status, [id, status] do
    reply =
      with {:ok, rec} <- fetch_live(id),
           {:ok, next} <- Validator.validate_transition(rec.status, status) do
        updated = %{rec | status: next, updated_at: now()}
        :ets.insert(@records, {id, updated})
        {:ok, updated}
      end

    {reply, state}
  end

  @impl true
  def handle_call(_msg, _from, state) do
    {:reply, {:error, :unknown_operation}, state}
  end

  @impl true
  def handle_info(:sweep, state) do
    ts = now()

    expired =
      :ets.foldl(
        fn
          {id, %{expires_at: exp}}, acc when exp <= ts -> [id | acc]
          _, acc -> acc
        end,
        [],
        @records
      )

    Enum.each(expired, fn id ->
      :ets.delete(@records, id)
      record(state, :expire, id)
    end)

    Process.send_after(self(), :sweep, state.sweep_interval)
    {:noreply, state}
  end

  # ---------------------------------------------------------------------------
  # Funções privadas
  # ---------------------------------------------------------------------------

  defp insert_new(%{id: id, destination: destination, ttl_ms: ttl}) do
    ts = now()

    rec = %{
      id: id,
      destination: destination,
      status: :pending,
      created_at: ts,
      updated_at: ts,
      expires_at: ts + ttl
    }

    :ets.insert(@records, {id, rec})
    {:ok, rec}
  end

  defp fetch_live(id) do
    with :ok <- Validator.validate_id(id) do
      case :ets.lookup(@records, id) do
        [{^id, rec}] ->
          if rec.expires_at > now(), do: {:ok, rec}, else: {:error, :expired}

        [] ->
          {:error, :not_found}
      end
    end
  end

  defp live?(id), do: match?({:ok, _}, fetch_live(id))

  defp now, do: System.system_time(:millisecond)
end

defmodule DeliveryTracker.Supervisor do
  @moduledoc """
  Supervisor `rest_for_one`: se o `Storage` cair, o `Server` também é reiniciado.
  Se apenas o `Server` falhar, ele é reiniciado e recupera o estado do ETS.
  """

  use Supervisor

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    children = [
      DeliveryTracker.Storage,
      {DeliveryTracker.Server, opts}
    ]

    Supervisor.init(children, strategy: :rest_for_one, max_restarts: 10, max_seconds: 10)
  end
end

defmodule DeliveryTracker do
  @moduledoc """
  Fachada pública do sistema de entregas e rastreamento.

      {:ok, _} = DeliveryTracker.start_link()
      {:ok, _} = DeliveryTracker.create(%{id: "ENT-1", destination: "Rua A, 10", ttl_ms: 60_000})
      {:ok, _} = DeliveryTracker.update_status("ENT-1", :in_transit)
      {:ok, _} = DeliveryTracker.fetch("ENT-1")
      DeliveryTracker.history(10)
  """

  defdelegate start_link(opts \\ []), to: DeliveryTracker.Supervisor
  defdelegate create(attrs), to: DeliveryTracker.Server
  defdelegate update_status(id, status), to: DeliveryTracker.Server
  defdelegate fetch(id), to: DeliveryTracker.Server
  defdelegate history(limit \\ 50), to: DeliveryTracker.Server
end
