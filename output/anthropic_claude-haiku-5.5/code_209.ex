# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de gerenciamento de notificações, atendendo somente aos seguintes requisitos:
#
# 1. Gerar notificações quando determinadas condições forem atingidas.
# 2. Quando fizer sentido para o domínio, considerar a comunicação com um banco de dados fictício e, opcionalmente, o uso de Ecto.Migration, sem tornar isso obrigatório.
# 3. Permitir cadastrar, consultar, atualizar e remover registros.
# 4. Manter um histórico das operações realizadas.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# Arquivo único: sistema de gerenciamento de notificações com banco fictício em memória.
#
# Visão geral:
#   - Notifications.Store:    operações puras sobre o estado do banco (tabelas, histórico).
#   - Notifications.Repo:     GenServer que serializa transações sobre o estado (atomicidade
#                             com rollback automático em caso de {:error, _}).
#   - Notifications.Rules:    regras de negócio puras que geram notificações a partir de
#                             transições de estado (disparo na borda: só quando a condição
#                             passa de falsa para verdadeira).
#   - Notifications.Products: CRUD de produtos (domínio de exemplo) que aciona as regras.
#   - Notifications:          API pública para consultar e marcar notificações como lidas,
#                             além de consultar o histórico de operações.
#   - Notifications.Demo:     exemplo de uso.
#
# Opcional (Ecto): o Repo pode ser substituído por um Ecto.Repo. A migration equivalente
# seria algo como:
#
#   defmodule Notifications.Repo.Migrations.CreateTables do
#     use Ecto.Migration
#
#     def change do
#       create table(:products) do
#         add :name, :string, null: false
#         add :quantity, :integer, null: false
#         add :min_stock, :integer, null: false
#         timestamps()
#       end
#
#       create table(:notifications) do
#         add :rule, :string, null: false
#         add :level, :string, null: false
#         add :product_id, references(:products, on_delete: :delete_all)
#         add :message, :text, null: false
#         add :read, :boolean, default: false
#         add :created_at, :utc_datetime_usec
#       end
#
#       create table(:history) do
#         add :op, :string, null: false
#         add :table_name, :string, null: false
#         add :record_id, :integer
#         add :before, :map
#         add :after, :map
#         add :at, :utc_datetime_usec
#       end
#     end
#   end

defmodule Notifications.Store do
  @moduledoc """
  Operações puras sobre o estado do banco fictício.

  Estado: %{seq: integer, hseq: integer, tables: %{table => %{id => row}}, history: [entry]}
  """

  def new, do: %{seq: 0, hseq: 0, tables: %{}, history: []}

  # Insere uma linha gerando um id sequencial. Retorna {linha, novo_estado}.
  def insert(state, table, attrs) do
    id = state.seq + 1
    row = Map.put(attrs, :id, id)

    state =
      state
      |> Map.put(:seq, id)
      |> put_row(table, id, row)

    {row, state}
  end

  # Busca uma linha pelo id.
  def fetch(state, table, id) do
    state.tables
    |> Map.get(table, %{})
    |> Map.fetch(id)
    |> case do
      {:ok, row} -> {:ok, row}
      :error -> {:error, :not_found}
    end
  end

  # Lista todas as linhas de uma tabela, ordenadas por id.
  def all(state, table) do
    state.tables
    |> Map.get(table, %{})
    |> Map.values()
    |> Enum.sort_by(& &1.id)
  end

  # Substitui (ou cria) uma linha existente pelo seu id.
  def put(state, table, row), do: put_row(state, table, row.id, row)

  # Remove uma linha pelo id.
  def delete(state, table, id) do
    tables = Map.update(state.tables, table, %{}, &Map.delete(&1, id))
    %{state | tables: tables}
  end

  # Registra uma operação no histórico (imutável, em ordem decrescente).
  def log(state, op, table, id, before, after_state) do
    hseq = state.hseq + 1

    entry = %{
      seq: hseq,
      op: op,
      table: table,
      record_id: id,
      before: before,
      after: after_state,
      at: DateTime.utc_now()
    }

    %{state | hseq: hseq, history: [entry | state.history]}
  end

  def history(state), do: state.history

  defp put_row(state, table, id, row) do
    tables = Map.update(state.tables, table, %{id => row}, &Map.put(&1, id, row))
    %{state | tables: tables}
  end
end

defmodule Notifications.Repo do
  @moduledoc """
  Banco fictício: GenServer que mantém o estado do Store e executa transações.

  Uma transação recebe o estado atual e deve retornar:
    * {:ok, resultado, novo_estado}  -> confirma o novo estado
    * {:error, motivo}               -> descarta alterações (rollback)
  """
  use GenServer

  def start_link(_opts \\ []) do
    GenServer.start_link(__MODULE__, Notifications.Store.new(), name: __MODULE__)
  end

  def transaction(fun) when is_function(fun, 1) do
    GenServer.call(__MODULE__, {:transaction, fun})
  end

  @impl true
  def init(state), do: {:ok, state}

  @impl true
  def handle_call({:transaction, fun}, _from, state) do
    case fun.(state) do
      {:ok, result, new_state} -> {:reply, {:ok, result}, new_state}
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end
end

defmodule Notifications.Rules do
  @moduledoc """
  Regras que geram notificações quando condições são atingidas.

  Uma regra dispara apenas na transição: a condição passa a ser verdadeira
  no estado atual e não era verdadeira no estado anterior (nil na criação).
  """

  def evaluate(previous, current) do
    for rule <- rules(),
        triggered?(rule, previous, current) do
      %{
        rule: rule.code,
        level: rule.level,
        product_id: current.id,
        message: rule.message.(current)
      }
    end
  end

  defp triggered?(rule, previous, current) do
    rule.condition.(current) and not (previous != nil and rule.condition.(previous))
  end

  defp rules do
    [
      %{
        code: :out_of_stock,
        level: :critical,
        condition: fn p -> p.quantity == 0 end,
        message: fn p -> "Produto esgotado: #{p.name}" end
      },
      %{
        code: :low_stock,
        level: :warning,
        condition: fn p -> p.quantity > 0 and p.quantity <= p.min_stock end,
        message: fn p ->
          "Estoque baixo para #{p.name}: #{p.quantity} unidade(s) (mínimo #{p.min_stock})"
        end
      }
    ]
  end
end

defmodule Notifications.Products do
  @moduledoc "CRUD de produtos. Cada operação aciona as regras e registra o histórico."

  alias Notifications.{Repo, Store, Rules}

  @table :products
  @fields [:name, :quantity, :min_stock]

  def create(attrs) do
    with {:ok, data} <- validate(attrs) do
      Repo.transaction(fn state ->
        {product, state} = Store.insert(state, @table, data)
        {notes, state} = notify(state, nil, product)
        state = Store.log(state, :create, @table, product.id, nil, product)
        {:ok, %{product: product, notifications: notes}, state}
      end)
    end
  end

  def get(id) do
    Repo.transaction(fn state ->
      with {:ok, product} <- Store.fetch(state, @table, id) do
        {:ok, product, state}
      end
    end)
  end

  def list do
    Repo.transaction(fn state ->
      {:ok, Store.all(state, @table), state}
    end)
  end

  def update(id, attrs) do
    changes = Map.take(attrs, @fields)

    Repo.transaction(fn state ->
      with {:ok, old} <- Store.fetch(state, @table, id),
           {:ok, data} <- validate(Map.merge(old, changes)) do
        new = Map.merge(old, data)
        state = Store.put(state, @table, new)
        {notes, state} = notify(state, old, new)
        state = Store.log(state, :update, @table, id, old, new)
        {:ok, %{product: new, notifications: notes}, state}
      end
    end)
  end

  def delete(id) do
    Repo.transaction(fn state ->
      with {:ok, old} <- Store.fetch(state, @table, id) do
        state =
          state
          |> Store.delete(@table, id)
          |> Store.log(:delete, @table, id, old, nil)

        {:ok, old, state}
      end
    end)
  end

  # Avalia as regras e persiste as notificações geradas.
  defp notify(state, previous, current) do
    now = DateTime.utc_now()

    Rules.evaluate(previous, current)
    |> Enum.map_reduce(state, fn note, acc ->
      Store.insert(acc, :notifications, Map.merge(note, %{read: false, created_at: now}))
    end)
  end

  defp validate(attrs) do
    name = Map.get(attrs, :name)
    quantity = Map.get(attrs, :quantity)
    min_stock = Map.get(attrs, :min_stock)

    cond do
      not (is_binary(name) and name != "") -> {:error, :invalid_name}
      not (is_integer(quantity) and quantity >= 0) -> {:error, :invalid_quantity}
      not (is_integer(min_stock) and min_stock >= 0) -> {:error, :invalid_min_stock}
      true -> {:ok, %{name: name, quantity: quantity, min_stock: min_stock}}
    end
  end
end

defmodule Notifications do
  @moduledoc "API pública para consultar e gerenciar notificações e o histórico."

  alias Notifications.{Repo, Store}

  # Lista notificações. Opções: unread_only: true | false
  def list(opts \\ []) do
    unread_only? = Keyword.get(opts, :unread_only, false)

    Repo.transaction(fn state ->
      rows = Store.all(state, :notifications)
      rows = if unread_only?, do: Enum.reject(rows, & &1.read), else: rows
      {:ok, rows, state}
    end)
  end

  def get(id) do
    Repo.transaction(fn state ->
      with {:ok, note} <- Store.fetch(state, :notifications, id) do
        {:ok, note, state}
      end
    end)
  end

  def mark_read(id) do
    Repo.transaction(fn state ->
      with {:ok, note} <- Store.fetch(state, :notifications, id) do
        updated = %{note | read: true}

        state =
          state
          |> Store.put(:notifications, updated)
          |> Store.log(:update, :notifications, id, note, updated)

        {:ok, updated, state}
      end
    end)
  end

  def delete(id) do
    Repo.transaction(fn state ->
      with {:ok, note} <- Store.fetch(state, :notifications, id) do
        state =
          state
          |> Store.delete(:notifications, id)
          |> Store.log(:delete, :notifications, id, note, nil)

        {:ok, note, state}
      end
    end)
  end

  # Histórico de operações (mais recente primeiro).
  def history do
    Repo.transaction(fn state ->
      {:ok, Store.history(state), state}
    end)
  end
end

defmodule Notifications.Demo do
  @moduledoc "Exemplo de uso: `Notifications.Demo.run()`."

  alias Notifications.{Products, Notifications}

  def run do
    {:ok, _pid} = Notifications.Repo.start_link()

    {:ok, %{product: p}} =
      Products.create(%{name: "Teclado", quantity: 10, min_stock: 3})

    {:ok, _} = Products.update(p.id, %{quantity: 3})
    {:ok, _} = Products.update(p.id, %{quantity: 0})

    {:ok, unread} = Notifications.list(unread_only: true)
    IO.inspect(unread, label: "Notificações não lidas")

    [first | _] = unread
    {:ok, _} = Notifications.mark_read(first.id)

    {:ok, history} = Notifications.history()
    IO.inspect(length(history), label: "Operações no histórico")

    :ok
  end
end
