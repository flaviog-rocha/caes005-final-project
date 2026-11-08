# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de gerenciamento de notificações, atendendo somente aos seguintes requisitos:
#
# 1. Gerar notificações quando determinadas condições forem atingidas.
# 2. Quando fizer sentido para o domínio, considerar a comunicação com um banco de dados fictício e, opcionalmente, o uso de Ecto.Migration, sem tornar isso obrigatório.
# 3. Permitir cadastrar, consultar, atualizar e remover registros.
# 4. Manter um histórico das operações realizadas.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# Sistema de gerenciamento de notificações (arquivo único, apenas biblioteca padrão).
#
# Execução de exemplo:
#   elixir notificacoes.exs
# ou, em iex (carregando o arquivo com `iex notificacoes.exs`):
#   Notificacoes.Demo.run()

defmodule Notificacoes.Rule do
  @moduledoc """
  Regra (condição) que, quando atingida por uma medição, gera uma notificação.
  Contém apenas funções puras: validação, avaliação e renderização de mensagem.
  """

  @fields [:name, :metric, :operator, :threshold, :message, :enabled]
  @required [:name, :metric, :operator, :threshold, :message]
  @operators [:gt, :gte, :lt, :lte, :eq, :neq]

  @doc "Valida atributos para criação de uma regra, aplicando valores padrão."
  def validate_new(attrs) do
    attrs = normalize(attrs)

    case Enum.reject(@required, &Map.has_key?(attrs, &1)) do
      [] -> validate_changes(Map.put_new(attrs, :enabled, true))
      missing -> {:error, {:missing_fields, missing}}
    end
  end

  @doc "Valida um conjunto parcial de alterações (usado em atualizações)."
  def validate_changes(changes) do
    changes = normalize(changes)

    Enum.reduce_while(changes, {:ok, changes}, fn {k, v}, acc ->
      case check(k, v) do
        :ok -> {:cont, acc}
        error -> {:halt, error}
      end
    end)
  end

  @doc "Verifica se a regra é disparada por uma medição (`metric`, `value`)."
  def matches?(%{enabled: false}, _metric, _value), do: false

  def matches?(%{metric: metric, operator: op, threshold: t}, metric, value),
    do: compare(op, value, t)

  def matches?(_rule, _metric, _value), do: false

  @doc "Renderiza a mensagem substituindo `{value}` e `{threshold}`."
  def render(%{message: msg, threshold: t}, value) do
    msg
    |> String.replace("{value}", to_string(value))
    |> String.replace("{threshold}", to_string(t))
  end

  # --- Privado ---

  defp normalize(attrs) do
    Map.new(attrs, fn {k, v} -> {normalize_key(k), v} end)
  end

  defp normalize_key(k) when is_atom(k), do: k

  defp normalize_key(k) when is_binary(k),
    do: Enum.find(@fields, k, &(Atom.to_string(&1) == k))

  defp check(:name, v) when is_binary(v) and byte_size(v) > 0, do: :ok
  defp check(:metric, v) when is_atom(v), do: :ok
  defp check(:operator, v) when v in @operators, do: :ok
  defp check(:threshold, v) when is_number(v), do: :ok
  defp check(:message, v) when is_binary(v), do: :ok
  defp check(:enabled, v) when is_boolean(v), do: :ok
  defp check(k, v) when k in @fields, do: {:error, {:invalid_field, k, v}}
  defp check(k, _v), do: {:error, {:unknown_field, k}}

  defp compare(:gt, a, b), do: a > b
  defp compare(:gte, a, b), do: a >= b
  defp compare(:lt, a, b), do: a < b
  defp compare(:lte, a, b), do: a <= b
  defp compare(:eq, a, b), do: a == b
  defp compare(:neq, a, b), do: a != b
end

defmodule Notificacoes.Notification do
  @moduledoc "Regras de validação para notificações."

  @statuses [:unread, :read]

  @doc "Apenas o campo `status` pode ser alterado em uma notificação."
  def validate_update(%{status: s} = changes) when s in @statuses and map_size(changes) == 1,
    do: {:ok, changes}

  def validate_update(changes), do: {:error, {:invalid_update, changes}}
end

defmodule Notificacoes.Engine do
  @moduledoc """
  Motor de avaliação: dado um conjunto de regras e uma medição, produz os
  atributos das notificações que devem ser geradas. Função pura.
  """

  alias Notificacoes.Rule

  def evaluate(rules, %{metric: metric, value: value}) do
    for rule <- rules, Rule.matches?(rule, metric, value) do
      %{
        rule_id: rule.id,
        metric: metric,
        value: value,
        message: Rule.render(rule, value),
        status: :unread
      }
    end
  end
end

defmodule Notificacoes.FakeDB do
  @moduledoc """
  Banco de dados fictício em memória, implementado com GenServer.

  Cada operação (inclusive a avaliação de medições) é executada dentro de uma
  única chamada ao processo, garantindo atomicidade e consistência do histórico.

  Se fosse usar Ecto, a tabela de notificações poderia ser criada com algo como:

  # defmodule Repo.Migrations.CreateNotifications do
  #   use Ecto.Migration
  #
  #   def change do
  #     create table(:notifications) do
  #       add :rule_id, references(:rules), null: false
  #       add :metric, :string, null: false
  #       add :value, :float, null: false
  #       add :message, :text, null: false
  #       add :status, :string, null: false, default: "unread"
  #       timestamps()
  #     end
  #   end
  # end
  """

  use GenServer

  @tables [:rules, :notifications]

  # --- API pública ---

  def start_link(_opts \\ []) do
    GenServer.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  def insert(table, attrs, action \\ :create), do: call({:insert, table, attrs, action})
  def get(table, id), do: call({:get, table, id})
  def list(table), do: call({:list, table})
  def update(table, id, changes), do: call({:update, table, id, changes})
  def delete(table, id), do: call({:delete, table, id})
  def evaluate(measurement), do: call({:evaluate, measurement})
  def history(filters \\ []), do: call({:history, filters})

  defp call(msg), do: GenServer.call(__MODULE__, msg)

  # --- Callbacks do GenServer ---

  @impl true
  def init(:ok) do
    state = %{
      tables: Map.new(@tables, &{&1, %{}}),
      seq: Map.new(@tables, &{&1, 0}),
      history: [],
      hseq: 0
    }

    {:ok, state}
  end

  @impl true
  def handle_call({:insert, table, attrs, action}, _from, st) when table in @tables do
    {record, st} = insert_record(st, table, attrs, action)
    {:reply, {:ok, record}, st}
  end

  def handle_call({:get, table, id}, _from, st) do
    {:reply, fetch(st, table, id), st}
  end

  def handle_call({:list, table}, _from, st) do
    records = st.tables |> Map.fetch!(table) |> Map.values() |> Enum.sort_by(& &1.id)
    {:reply, {:ok, records}, st}
  end

  def handle_call({:update, table, id, changes}, _from, st) do
    case fetch(st, table, id) do
      {:ok, rec} ->
        now = DateTime.utc_now()
        updated = rec |> Map.merge(changes) |> Map.put(:updated_at, now)

        diff = %{from: Map.take(rec, Map.keys(changes)), to: changes}

        st =
          st
          |> put_in([:tables, table, id], updated)
          |> log(:update, table, id, diff)

        {:reply, {:ok, updated}, st}

      error ->
        {:reply, error, st}
    end
  end

  def handle_call({:delete, table, id}, _from, st) do
    case fetch(st, table, id) do
      {:ok, rec} ->
        st =
          %{st | tables: Map.update!(st.tables, table, &Map.delete(&1, id))}
          |> log(:delete, table, id, rec)

        {:reply, {:ok, rec}, st}

      error ->
        {:reply, error, st}
    end
  end

  def handle_call({:evaluate, measurement}, _from, st) do
    rules = st.tables.rules |> Map.values() |> Enum.sort_by(& &1.id)
    attrs_list = Notificacoes.Engine.evaluate(rules, measurement)

    {notifications, st} =
      Enum.map_reduce(attrs_list, st, fn attrs, acc ->
        insert_record(acc, :notifications, attrs, :generate)
      end)

    st = log(st, :evaluate, nil, nil, %{measurement: measurement, generated: length(notifications)})

    {:reply, {:ok, notifications}, st}
  end

  def handle_call({:history, filters}, _from, st) do
    entries =
      st.history
      |> Enum.reverse()
      |> Enum.filter(fn entry ->
        Enum.all?(filters, fn {k, v} -> Map.get(entry, k) == v end)
      end)

    {:reply, {:ok, entries}, st}
  end

  # --- Funções privadas ---

  defp insert_record(st, table, attrs, action) do
    id = st.seq[table] + 1
    now = DateTime.utc_now()

    record =
      attrs
      |> Map.put(:id, id)
      |> Map.put(:inserted_at, now)
      |> Map.put(:updated_at, now)

    st =
      st
      |> put_in([:tables, table, id], record)
      |> update_in([:seq, table], &(&1 + 1))
      |> log(action, table, id, record)

    {record, st}
  end

  defp fetch(st, table, id) do
    case get_in(st, [:tables, table, id]) do
      nil -> {:error, :not_found}
      rec -> {:ok, rec}
    end
  end

  defp log(st, action, table, record_id, data) do
    entry = %{
      seq: st.hseq + 1,
      at: DateTime.utc_now(),
      action: action,
      table: table,
      record_id: record_id,
      data: data
    }

    %{st | history: [entry | st.history], hseq: st.hseq + 1}
  end
end

defmodule Notificacoes do
  @moduledoc """
  API pública do sistema de notificações.

  - Regras: cadastrar, consultar, atualizar e remover condições.
  - Notificações: consultar, marcar como lidas e remover.
  - `ingest/2`: envia uma medição; as regras atingidas geram notificações.
  - `history/1`: consulta o histórico de todas as operações.
  """

  alias Notificacoes.{FakeDB, Rule, Notification}

  def start_link(opts \\ []), do: FakeDB.start_link(opts)

  # --- Regras ---

  def create_rule(attrs) do
    with {:ok, valid} <- Rule.validate_new(attrs), do: FakeDB.insert(:rules, valid)
  end

  def get_rule(id), do: FakeDB.get(:rules, id)

  def list_rules, do: FakeDB.list(:rules)

  def update_rule(id, changes) do
    with {:ok, valid} <- Rule.validate_changes(changes), do: FakeDB.update(:rules, id, valid)
  end

  def delete_rule(id), do: FakeDB.delete(:rules, id)

  # --- Notificações ---

  def get_notification(id), do: FakeDB.get(:notifications, id)

  @doc "Lista notificações. Filtros opcionais por campo, ex.: `status: :unread`."
  def list_notifications(filters \\ []) do
    with {:ok, all} <- FakeDB.list(:notifications) do
      {:ok, Enum.filter(all, &Enum.all?(filters, fn {k, v} -> Map.get(&1, k) == v end))}
    end
  end

  def update_notification(id, changes) do
    with {:ok, valid} <- Notification.validate_update(changes),
         do: FakeDB.update(:notifications, id, valid)
  end

  def delete_notification(id), do: FakeDB.delete(:notifications, id)

  # --- Motor de avaliação ---

  @doc "Processa uma medição e gera as notificações das regras atingidas."
  def ingest(metric, value) when is_atom(metric) and is_number(value),
    do: FakeDB.evaluate(%{metric: metric, value: value})

  def ingest(_metric, _value), do: {:error, :invalid_measurement}

  # --- Histórico ---

  @doc "Retorna o histórico em ordem cronológica. Filtros: `table:`, `action:`, `record_id:`."
  def history(filters \\ []), do: FakeDB.history(filters)
end

defmodule Notificacoes.Demo do
  @moduledoc "Exemplo de uso do sistema."

  def run do
    {:ok, _pid} = Notificacoes.start_link()

    {:ok, rule} =
      Notificacoes.create_rule(%{
        "name" => "Temperatura alta",
        "metric" => :temperatura,
        "operator" => :gt,
        "threshold" => 30,
        "message" => "Temperatura {value} acima do limite {threshold}"
      })

    IO.inspect(rule, label: "Regra criada")

    {:ok, notifs} = Notificacoes.ingest(:temperatura, 35)
    IO.inspect(notifs, label: "Notificações geradas")

    {:ok, []} = Notificacoes.ingest(:temperatura, 20)

    [notif | _] = notifs
    {:ok, read} = Notificacoes.update_notification(notif.id, %{status: :read})
    IO.inspect(read, label: "Notificação lida")

    {:ok, unread} = Notificacoes.list_notifications(status: :unread)
    IO.inspect(length(unread), label: "Não lidas")

    {:ok, _} = Notificacoes.update_rule(rule.id, %{threshold: 40})
    {:ok, _} = Notificacoes.delete_rule(rule.id)

    {:ok, history} = Notificacoes.history()
    Enum.each(history, &IO.inspect(&1, label: "Histórico"))

    :ok
  end
end
