# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o monitoramento de sensores, atendendo somente aos seguintes requisitos:
#
# 1. Validar os dados recebidos antes de realizar operações.
# 2. Permitir que diferentes entidades sejam processadas independentemente.
# 3. Diferenciar erros de validação de falhas operacionais.
# 4. Permitir cadastrar, consultar, atualizar e remover registros.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - ETS
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# sensor_monitor.ex
#
# Monitoramento de sensores com ETS.
#
# Contrato de retorno de todas as operações públicas:
#   {:ok, valor}                              -> sucesso
#   {:error, {:validation, [{campo, msg}]}}   -> entrada inválida, rejeitada ANTES de qualquer operação
#   {:error, {:operational, motivo}}          -> falha na execução (store indisponível, conflito de
#                                               existência, não encontrado, exceção, timeout...)

defmodule SensorMonitor do
  @moduledoc """
  Fachada pública do monitoramento de sensores.

  Cada entidade (sensor) é identificada por `id` e tratada de forma independente:
  as escritas são serializadas pelo `SensorMonitor.Store` por chave, e `process_batch/2`
  executa cada operação em um processo separado, de modo que a falha de uma entidade
  não afeta as demais.
  """

  alias SensorMonitor.Store

  @default_batch_timeout 5_000

  @doc "Inicia o armazenamento (ETS + GenServer dono da tabela)."
  def start_link(opts \\ []), do: Store.start_link(opts)

  @doc "Cadastra um sensor. Campos obrigatórios: :id, :type, :value, :unit."
  def create(attrs), do: Store.create(attrs)

  @doc "Consulta um sensor pelo id."
  def fetch(id), do: Store.fetch(id)

  @doc "Atualiza campos (:type, :value, :unit) de um sensor existente."
  def update(id, changes), do: Store.update(id, changes)

  @doc "Remove um sensor e retorna o registro removido."
  def delete(id), do: Store.delete(id)

  @doc "Lista todos os sensores ordenados por id."
  def list, do: Store.list()

  @doc """
  Processa uma lista de operações de forma independente, uma por processo.

  Operações aceitas:
    {:create, attrs} | {:fetch, id} | {:update, id, changes} | {:delete, id}

  Retorna uma lista de `{operacao, resultado}` na mesma ordem da entrada.
  Falhas de uma operação (inclusive crashes e timeouts) não afetam as outras.
  """
  def process_batch(operations, opts \\ []) when is_list(operations) do
    timeout = Keyword.get(opts, :timeout, @default_batch_timeout)

    operations
    |> Task.async_stream(&run_isolated/1,
      max_concurrency: System.schedulers_online(),
      ordered: true,
      timeout: timeout,
      on_timeout: :kill_task
    )
    |> Enum.zip(operations)
    |> Enum.map(fn
      {{:ok, {op, result}}, _op} ->
        {op, result}

      {{:exit, reason}, op} ->
        {op, {:error, {:operational, {:task_exit, reason}}}}
    end)
  end

  # Executa uma operação isolando qualquer exceção inesperada.
  defp run_isolated(op) do
    {op, dispatch(op)}
  rescue
    e -> {op, {:error, {:operational, {:exception, Exception.message(e)}}}}
  end

  defp dispatch({:create, attrs}), do: create(attrs)
  defp dispatch({:fetch, id}), do: fetch(id)
  defp dispatch({:update, id, changes}), do: update(id, changes)
  defp dispatch({:delete, id}), do: delete(id)

  defp dispatch(_other) do
    {:error,
     {:validation,
      [operation: "deve ser {:create, attrs} | {:fetch, id} | {:update, id, changes} | {:delete, id}"]}}
  end
end

defmodule SensorMonitor.Validator do
  @moduledoc false
  # Validação pura: nenhuma função aqui toca no ETS ou em processos.

  @types [:temperature, :humidity, :pressure]
  @units %{temperature: ["C", "F", "K"], humidity: ["%"], pressure: ["hPa", "Pa"]}
  @fields [:id, :type, :value, :unit]
  @mutable_fields [:type, :value, :unit]
  @max_id_bytes 64

  @doc "Valida apenas o identificador de uma entidade."
  def validate_id(id) do
    case check(:id, id) do
      [] -> {:ok, id}
      errors -> {:error, {:validation, errors}}
    end
  end

  @doc "Valida um novo sensor completo."
  def validate_new(attrs) do
    with {:ok, map} <- ensure_map(attrs) do
      errors =
        unknown_errors(map, @fields) ++
          missing_errors(map) ++
          field_errors(map) ++
          cross_errors(map)

      result(errors, Map.take(map, @fields))
    end
  end

  @doc "Valida alterações parciais sobre um sensor já existente."
  def validate_update(current, changes) do
    with {:ok, map} <- ensure_map(changes) do
      merged = Map.merge(current, map)

      errors =
        empty_errors(map) ++
          unknown_errors(map, @mutable_fields) ++
          field_errors(map) ++
          cross_errors(merged)

      result(errors, merged)
    end
  end

  # -- Helpers de estrutura --------------------------------------------------

  defp ensure_map(value) when is_map(value), do: {:ok, value}
  defp ensure_map(_), do: {:error, {:validation, [attrs: "deve ser um mapa"]}}

  defp result([], value), do: {:ok, value}
  defp result(errors, _value), do: {:error, {:validation, errors}}

  defp empty_errors(map) when map_size(map) == 0, do: [changes: "não pode ser vazio"]
  defp empty_errors(_), do: []

  defp unknown_errors(map, allowed) do
    for k <- Map.keys(map), k not in allowed, do: {k, "campo desconhecido"}
  end

  defp missing_errors(map) do
    for f <- @fields, not Map.has_key?(map, f), do: {f, "é obrigatório"}
  end

  defp field_errors(map) do
    for {k, v} <- map, k in @fields, e <- check(k, v), do: e
  end

  # -- Regras por campo ------------------------------------------------------

  defp check(:id, v) when is_binary(v) and byte_size(v) > 0 and byte_size(v) <= @max_id_bytes,
    do: []

  defp check(:id, _), do: [id: "deve ser string não vazia de até #{@max_id_bytes} bytes"]

  defp check(:type, v) when v in @types, do: []
  defp check(:type, _), do: [type: "deve ser um de #{inspect(@types)}"]

  defp check(:value, v) when is_number(v), do: []
  defp check(:value, _), do: [value: "deve ser numérico"]

  defp check(:unit, v) when is_binary(v) and v != "", do: []
  defp check(:unit, _), do: [unit: "deve ser string não vazia"]

  # Regra entre campos: a unidade precisa ser compatível com o tipo.
  defp cross_errors(%{type: t, unit: u}) when t in @types do
    allowed = Map.fetch!(@units, t)

    if u in allowed,
      do: [],
      else: [unit: "deve ser um de #{inspect(allowed)} para o tipo #{t}"]
  end

  defp cross_errors(_), do: []
end

defmodule SensorMonitor.Store do
  @moduledoc false
  # Dono da tabela ETS. Escritas passam pelo GenServer (serializadas e validadas
  # antes de tocar na tabela). Leituras acessam a tabela diretamente (read_concurrency).

  use GenServer

  alias SensorMonitor.Validator

  @table :sensor_monitor_sensors
  @call_timeout 5_000

  # -- API pública -----------------------------------------------------------

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def create(attrs), do: call({:create, attrs})

  def update(id, changes) do
    with {:ok, id} <- Validator.validate_id(id), do: call({:update, id, changes})
  end

  def delete(id) do
    with {:ok, id} <- Validator.validate_id(id), do: call({:delete, id})
  end

  def fetch(id) do
    with {:ok, id} <- Validator.validate_id(id) do
      safe_read(fn ->
        case :ets.lookup(@table, id) do
          [{^id, sensor}] -> {:ok, sensor}
          [] -> {:error, {:operational, {:not_found, id}}}
        end
      end)
    end
  end

  def list do
    safe_read(fn ->
      sensors =
        @table
        |> :ets.tab2list()
        |> Enum.map(fn {_id, sensor} -> sensor end)
        |> Enum.sort_by(& &1.id)

      {:ok, sensors}
    end)
  end

  # -- GenServer -------------------------------------------------------------

  @impl true
  def init(_opts) do
    :ets.new(@table, [:set, :protected, :named_table, read_concurrency: true])
    {:ok, :ok}
  end

  @impl true
  def handle_call(msg, _from, state) do
    {:reply, execute(msg), state}
  end

  # Executa a operação isolando exceções para não derrubar o GenServer.
  defp execute(msg) do
    apply_op(msg)
  rescue
    e -> {:error, {:operational, {:exception, Exception.message(e)}}}
  end

  defp apply_op({:create, attrs}) do
    # Validação ocorre antes de qualquer leitura/escrita na tabela.
    with {:ok, sensor} <- Validator.validate_new(attrs) do
      if :ets.member(@table, sensor.id) do
        {:error, {:operational, {:already_exists, sensor.id}}}
      else
        true = :ets.insert(@table, {sensor.id, sensor})
        {:ok, sensor}
      end
    end
  end

  defp apply_op({:update, id, changes}) do
    with {:ok, current} <- lookup(id),
         {:ok, updated} <- Validator.validate_update(current, changes) do
      true = :ets.insert(@table, {id, updated})
      {:ok, updated}
    end
  end

  defp apply_op({:delete, id}) do
    with {:ok, sensor} <- lookup(id) do
      true = :ets.delete(@table, id)
      {:ok, sensor}
    end
  end

  defp apply_op(_other) do
    {:error, {:validation, [operation: "operação desconhecida"]}}
  end

  defp lookup(id) do
    case :ets.lookup(@table, id) do
      [{^id, sensor}] -> {:ok, sensor}
      [] -> {:error, {:operational, {:not_found, id}}}
    end
  end

  # -- Infraestrutura de chamada --------------------------------------------

  defp call(msg) do
    GenServer.call(__MODULE__, msg, @call_timeout)
  catch
    :exit, {:noproc, _} -> {:error, {:operational, :store_unavailable}}
    :exit, {:timeout, _} -> {:error, {:operational, :timeout}}
    :exit, reason -> {:error, {:operational, {:exit, reason}}}
  end

  defp safe_read(fun) do
    fun.()
  rescue
    ArgumentError -> {:error, {:operational, :store_unavailable}}
  end
end
