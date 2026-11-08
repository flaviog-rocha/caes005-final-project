# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de gerenciamento de notificações, atendendo somente aos seguintes requisitos:
#
# 1. Recuperar processos após falhas inesperadas.
# 2. Gerar notificações quando determinadas condições forem atingidas.
# 3. Permitir consultar o estado atual das operações em andamento.
# 4. Validar os dados recebidos antes de realizar operações.
# 5. Manter um histórico das operações realizadas.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - ETS
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# Sistema de gerenciamento de notificações com ETS.
#
# Uso (exemplo):
#   {:ok, _sup} = NotificationSystem.start_link(scan_interval: 200, orphan_grace: 200)
#   {:ok, _rule} = NotificationSystem.add_rule(%{kind: :max_failures, type: :envio, threshold: 2})
#   :ok = NotificationSystem.subscribe()
#   {:ok, id} = NotificationSystem.submit(:envio, %{to: "a@b.com"}, fn -> enviar() end)
#   NotificationSystem.status(id)
#   NotificationSystem.running()
#   NotificationSystem.history(id)
#   NotificationSystem.notifications()

defmodule NotificationSystem do
  # Aliases declarados antes dos módulos aninhados para que possam ser referenciados entre si.
  alias NotificationSystem.{Heir, Store, Schema, Validator, History, Notifier, Runner, Recovery}

  # Dono "herdeiro" das tabelas ETS. Quando o Store cai, as tabelas são transferidas
  # para este processo (opção :heir do ETS), evitando perda de dados. Quando o Store
  # reinicia, ele recupera as tabelas por meio deste processo.
  defmodule Heir do
    use GenServer

    def start_link(_), do: GenServer.start_link(__MODULE__, nil, name: __MODULE__)

    # Devolve ao processo solicitante todas as tabelas que este herdeiro possui.
    def reclaim(pid), do: GenServer.call(__MODULE__, {:reclaim, pid})

    @impl true
    def init(_), do: {:ok, []}

    @impl true
    def handle_call({:reclaim, pid}, _from, tids) do
      for tid <- tids, :ets.info(tid) != :undefined do
        :ets.give_away(tid, pid, :reclaimed)
      end

      {:reply, :ok, []}
    end

    @impl true
    # Mensagem enviada pela VM quando o dono de uma tabela morre e o herdeiro recebe a tabela.
    def handle_info({:"ETS-TRANSFER", tid, _from, _data}, tids) do
      {:noreply, [tid | tids]}
    end

    def handle_info(_msg, tids), do: {:noreply, tids}
  end

  # Processo dono das tabelas ETS. Se cair, o supervisor o reinicia e as tabelas são recuperadas.
  defmodule Store do
    use GenServer

    def start_link(_), do: GenServer.start_link(__MODULE__, nil, name: __MODULE__)

    @impl true
    def init(_) do
      # Recupera tabelas que possam ter sobrevivido a uma queda anterior.
      :ok = Heir.reclaim(self())

      heir = Process.whereis(Heir)

      # Cria apenas as tabelas que ainda não existem.
      for {key, type} <- Schema.tables(), :ets.info(Schema.table(key)) == :undefined do
        :ets.new(Schema.table(key), [
          type,
          :public,
          :named_table,
          read_concurrency: true,
          heir: {heir, :none}
        ])
      end

      # Contadores de sequência (inseridos apenas se não existirem, preservando valores recuperados).
      :ets.insert_new(Schema.table(:meta), {:history_seq, 0})
      :ets.insert_new(Schema.table(:meta), {:notification_seq, 0})

      {:ok, nil}
    end

    @impl true
    # Ignora mensagens de transferência de tabelas (ETS-TRANSFER) recebidas durante a recuperação.
    def handle_info(_msg, state), do: {:noreply, state}
  end

  # Utilitários de acesso às tabelas ETS e à estrutura de registros de operações.
  defmodule Schema do
    # Tabelas e seus tipos. O histórico e as notificações são ordered_set para manter a ordem por sequência.
    @tables [
      ops: :set,
      history: :ordered_set,
      notifications: :ordered_set,
      rules: :set,
      meta: :set,
      subscribers: :set
    ]

    # Campos do registro de operação, armazenado como tupla na tabela :ops.
    @op_fields [
      :id,
      :status,
      :type,
      :payload,
      :fun,
      :owner,
      :attempts,
      :max_attempts,
      :started_at,
      :finished_at,
      :result
    ]

    def tables, do: @tables
    def table(key), do: :"ns_#{key}"

    # Retorna os valores de tabelas com formato {chave, valor}.
    def values(key) do
      key
      |> table()
      |> :ets.tab2list()
      |> Enum.map(&elem(&1, 1))
    end

    def op_to_tuple(op), do: @op_fields |> Enum.map(&Map.fetch!(op, &1)) |> List.to_tuple()
    def op_to_map(tuple), do: @op_fields |> Enum.zip(Tuple.to_list(tuple)) |> Map.new()
    def op_pos(field), do: Enum.find_index(@op_fields, &(&1 == field)) + 1

    def all_ops do
      :ops
      |> table()
      |> :ets.tab2list()
      |> Enum.map(&op_to_map/1)
    end

    # Tempo monotônico em milissegundos (usado para durações e ordenação temporal).
    def now, do: System.monotonic_time(:millisecond)

    def task_supervisor, do: NotificationSystem.TaskSupervisor
  end

  # Validação pura de dados de entrada.
  defmodule Validator do
    @max_attempts_limit 10

    def operation(type, payload, fun, opts \\ []) do
      max = Keyword.get(opts, :max_attempts, 3)

      errors =
        for {invalid?, reason} <- [
              {not (is_atom(type) and type not in [nil, true, false]), :invalid_type},
              {not is_map(payload), :invalid_payload},
              {not is_function(fun, 0), :invalid_fun},
              {not (is_integer(max) and max in 1..@max_attempts_limit), :invalid_max_attempts}
            ],
            invalid?,
            do: reason

      case errors do
        [] -> {:ok, %{type: type, payload: payload, fun: fun, max_attempts: max}}
        _ -> {:error, errors}
      end
    end

    def rule(%{kind: kind, threshold: threshold} = rule) do
      rule = Map.put_new(rule, :type, :any)

      errors =
        for {invalid?, reason} <- [
              {kind not in [:max_failures, :max_duration], :invalid_kind},
              {not is_atom(rule.type), :invalid_type},
              {not (is_integer(threshold) and threshold > 0), :invalid_threshold}
            ],
            invalid?,
            do: reason

      case errors do
        [] -> {:ok, rule}
        _ -> {:error, errors}
      end
    end

    def rule(_), do: {:error, [:invalid_rule]}
  end

  # Histórico de operações (append-only, ordenado por sequência).
  defmodule History do
    def add(op_id, event, data \\ %{}) do
      seq = :ets.update_counter(Schema.table(:meta), :history_seq, 1)
      entry = %{seq: seq, op_id: op_id, event: event, data: data, at: Schema.now()}
      :ets.insert(Schema.table(:history), {seq, entry})
      :ok
    end

    def list(:all), do: Schema.values(:history)
    def list(op_id), do: Enum.filter(Schema.values(:history), &(&1.op_id == op_id))
  end

  # Geração de notificações a partir de regras e eventos.
  defmodule Notifier do
    def emit(level, message, op_id \\ nil) do
      seq = :ets.update_counter(Schema.table(:meta), :notification_seq, 1)
      notification = %{seq: seq, level: level, message: message, op_id: op_id, at: Schema.now()}
      :ets.insert(Schema.table(:notifications), {seq, notification})

      # Envia a notificação para os assinantes ainda vivos.
      for {pid, _} <- :ets.tab2list(Schema.table(:subscribers)), Process.alive?(pid) do
        send(pid, {:notification, notification})
      end

      notification
    end

    def list, do: Schema.values(:notifications)

    # Avalia regras de limite de falhas após uma operação falhar.
    def on_failure(%{type: type, id: id}) do
      for rule <- rules(:max_failures), applies?(rule, type) do
        failed = Enum.count(Schema.all_ops(), &(&1.status == :failed and applies?(rule, &1.type)))

        if failed >= rule.threshold do
          emit(
            :alert,
            "limite de falhas atingido: #{failed}/#{rule.threshold} (#{inspect(rule.type)})",
            id
          )
        end
      end

      :ok
    end

    # Avalia regras de duração máxima após uma operação concluída.
    def on_completion(%{type: type, id: id}, duration) do
      for rule <- rules(:max_duration), applies?(rule, type), duration > rule.threshold do
        emit(:warning, "duração de #{duration}ms excedeu o limite de #{rule.threshold}ms", id)
      end

      :ok
    end

    defp rules(kind) do
      :rules
      |> Schema.values()
      |> Enum.filter(&(&1.kind == kind))
    end

    defp applies?(%{type: :any}, _type), do: true
    defp applies?(%{type: type}, type), do: true
    defp applies?(_rule, _type), do: false
  end

  # Execução de operações. Cada tentativa roda em um processo filho supervisionado
  # pelo Task.Supervisor, e o estado é mantido na tabela ETS :ops.
  defmodule Runner do
    def submit(type, payload, fun, opts) do
      with {:ok, spec} <- Validator.operation(type, payload, fun, opts) do
        id = :erlang.unique_integer([:positive, :monotonic])
        now = Schema.now()

        op = %{
          id: id,
          status: :queued,
          type: spec.type,
          payload: spec.payload,
          fun: spec.fun,
          owner: nil,
          attempts: 0,
          max_attempts: spec.max_attempts,
          started_at: now,
          finished_at: nil,
          result: nil
        }

        # Registro é inserido antes de iniciar o processo, para que o Recovery o enxergue caso algo falhe.
        :ets.insert(Schema.table(:ops), Schema.op_to_tuple(op))
        History.add(id, :submitted, %{type: spec.type})

        {:ok, pid} = Task.Supervisor.start_child(Schema.task_supervisor(), fn -> run(id) end)
        :ets.update_element(Schema.table(:ops), id, {Schema.op_pos(:owner), pid})

        {:ok, id}
      end
    end

    # Executa uma tentativa da operação. Pode ser chamado pelo submit ou pelo Recovery.
    # Observação: em caso de recuperação, a função é executada novamente (semântica at-least-once).
    def run(id) do
      ops = Schema.table(:ops)
      [tuple] = :ets.lookup(ops, id)
      %{type: type, fun: fun} = Schema.op_to_map(tuple)

      started = Schema.now()

      :ets.update_element(ops, id, [
        {Schema.op_pos(:owner), self()},
        {Schema.op_pos(:status), :running},
        {Schema.op_pos(:started_at), started}
      ])

      attempt = :ets.update_counter(ops, id, {Schema.op_pos(:attempts), 1})
      History.add(id, :attempt_started, %{attempt: attempt})

      case execute(fun) do
        {:ok, value} ->
          finish(id, :done, value)
          Notifier.on_completion(%{id: id, type: type}, Schema.now() - started)

        {:error, reason} ->
          finish(id, :failed, reason)
          Notifier.on_failure(%{id: id, type: type})
      end

      :ok
    end

    defp execute(fun) do
      case fun.() do
        {:error, reason} -> {:error, reason}
        value -> {:ok, value}
      end
    catch
      kind, reason -> {:error, {kind, reason}}
    end

    defp finish(id, status, result) do
      :ets.update_element(Schema.table(:ops), id, [
        {Schema.op_pos(:status), status},
        {Schema.op_pos(:finished_at), Schema.now()},
        {Schema.op_pos(:result), result}
      ])

      History.add(id, status, %{result: result})
    end
  end

  # Recuperação de operações órfãs (processo responsável morreu sem concluir).
  defmodule Recovery do
    use GenServer

    def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

    @impl true
    def init(opts) do
      state = %{
        interval: Keyword.get(opts, :scan_interval, 500),
        grace: Keyword.get(opts, :orphan_grace, 300)
      }

      send(self(), :scan)
      {:ok, state}
    end

    @impl true
    def handle_info(:scan, state) do
      recover(state.grace)
      Process.send_after(self(), :scan, state.interval)
      {:noreply, state}
    end

    defp recover(grace) do
      now = Schema.now()

      for op <- Schema.all_ops(), op.status in [:queued, :running], orphan?(op, now, grace) do
        handle_orphan(op, now)
      end

      :ok
    end

    # Processo responsável morreu sem concluir a operação.
    defp orphan?(%{owner: pid}, _now, _grace) when is_pid(pid), do: not Process.alive?(pid)

    # Operação criada mas sem processo associado após o período de carência.
    defp orphan?(%{owner: nil, started_at: t}, now, grace), do: now - t > grace

    defp handle_orphan(%{id: id, attempts: attempts, max_attempts: max}, now)
         when attempts < max do
      # Limpa o dono para evitar recuperações duplicadas durante a nova tentativa.
      :ets.update_element(Schema.table(:ops), id, [
        {Schema.op_pos(:owner), nil},
        {Schema.op_pos(:started_at), now}
      ])

      History.add(id, :recovering, %{attempts: attempts})
      Notifier.emit(:warning, "operação #{id} recuperada após falha inesperada", id)

      {:ok, _pid} =
        Task.Supervisor.start_child(Schema.task_supervisor(), fn -> Runner.run(id) end)
    end

    defp handle_orphan(%{id: id, attempts: attempts}, now) do
      :ets.update_element(Schema.table(:ops), id, [
        {Schema.op_pos(:status), :abandoned},
        {Schema.op_pos(:finished_at), now},
        {Schema.op_pos(:result), :max_attempts_exceeded}
      ])

      History.add(id, :abandoned, %{attempts: attempts})
      Notifier.emit(:critical, "operação #{id} abandonada após #{attempts} tentativas", id)
    end
  end

  # API pública

  # Inicia a árvore de supervisão. Ordem e estratégia garantem a recuperação das tabelas
  # (Heir antes do Store) e das operações (Recovery após os demais processos).
  def start_link(opts \\ []) do
    children = [
      Heir,
      Store,
      {Task.Supervisor, name: Schema.task_supervisor()},
      {Recovery, opts}
    ]

    Supervisor.start_link(children, strategy: :rest_for_one, name: __MODULE__)
  end

  # Valida e submete uma operação para execução.
  def submit(type, payload, fun, opts \\ []), do: Runner.submit(type, payload, fun, opts)

  # Consulta o estado de uma operação específica (sem expor a função).
  def status(id) do
    case :ets.lookup(Schema.table(:ops), id) do
      [tuple] -> {:ok, public(Schema.op_to_map(tuple))}
      [] -> {:error, :not_found}
    end
  end

  # Lista operações em andamento (enfileiradas ou executando).
  def running do
    Schema.all_ops()
    |> Enum.filter(&(&1.status in [:queued, :running]))
    |> Enum.map(&public/1)
  end

  # Contagem de operações por estado.
  def summary, do: Schema.all_ops() |> Enum.frequencies_by(& &1.status)

  # Histórico de todas as operações ou de uma operação específica.
  def history(op_id \\ :all), do: History.list(op_id)

  # Notificações geradas até o momento.
  def notifications, do: Notifier.list()

  # Adiciona uma regra de notificação após validação.
  # Tipos de regra: %{kind: :max_failures | :max_duration, type: atom | :any, threshold: pos_integer}
  def add_rule(rule) do
    with {:ok, valid} <- Validator.rule(rule) do
      id = :erlang.unique_integer([:positive, :monotonic])
      :ets.insert(Schema.table(:rules), {id, Map.put(valid, :id, id)})
      {:ok, id}
    end
  end

  # Assina notificações (o pid receberá mensagens {:notification, map}).
  def subscribe(pid \\ self()) do
    :ets.insert(Schema.table(:subscribers), {pid, true})
    :ok
  end

  defp public(op), do: Map.delete(op, :fun)
end
