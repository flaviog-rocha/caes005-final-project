# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de pedidos de um e-commerce, atendendo somente aos seguintes requisitos:
#
# 1. Permitir a execução de operações simultâneas.
# 2. Diferenciar erros de validação de falhas operacionais.
# 3. Persistir os dados para recuperação após reinicializações.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - DETS
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# Gerenciamento de pedidos de e-commerce com GenServer + DETS.
#
# Requisitos atendidos:
#   1. Operações simultâneas: qualquer número de processos chama a API em paralelo.
#      As escritas são serializadas pelo GenServer (uma leitura-modificação-escrita
#      por vez, sem condições de corrida). As leituras vão direto ao DETS, que
#      trata acessos concorrentes.
#   2. Erros de validação vs. operacionais:
#        {:error, {:validation, motivos}}   -> entrada ou regra de negócio inválida
#        {:error, {:operational, motivo}}   -> falha de infraestrutura (DETS, processo indisponível, timeout)
#   3. Persistência: os dados ficam em um arquivo DETS. Ao reiniciar, o módulo
#      reabre o arquivo e retoma a sequência de IDs de onde parou.
#
# Uso (exemplo em comentário):
#   # {:ok, _} = Ecommerce.OrderManager.start_link(path: "pedidos.dets")
#   # {:ok, pedido} = Ecommerce.OrderManager.create_order(%{
#   #   customer_id: "cli-1",
#   #   items: [%{sku: "ABC", qty: 2, unit_price_cents: 1500}]
#   # })
#   # {:ok, _} = Ecommerce.OrderManager.pay_order(pedido.id)
#   # {:ok, _} = Ecommerce.OrderManager.ship_order(pedido.id)
#   # {:ok, _} = Ecommerce.OrderManager.get_order(pedido.id)
#
# Observação: a tabela DETS tem nome global (@table). Use uma única instância
# do GenServer por nó.

defmodule Ecommerce.OrderManager do
  @moduledoc """
  Gerenciador de pedidos com persistência em DETS.

  Ciclo de vida do pedido:
    pending -> paid -> shipped
    pending | paid -> cancelled

  Todas as funções públicas retornam `{:ok, valor}` ou `{:error, {tipo, detalhe}}`,
  onde `tipo` é `:validation` (culpa da entrada/regra) ou `:operational` (falha do sistema).
  """

  use GenServer

  @table :ecommerce_orders
  @call_timeout 5_000

  # Transições permitidas entre status
  @transitions %{
    pending: [:paid, :cancelled],
    paid: [:shipped, :cancelled],
    shipped: [],
    cancelled: []
  }

  # ---------------------------------------------------------------------------
  # API pública
  # ---------------------------------------------------------------------------

  @doc "Inicia o gerenciador. Opções: `:path` (arquivo DETS) e `:name` (nome do processo)."
  def start_link(opts \\ []) do
    path = Keyword.get(opts, :path, "orders.dets")
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, path, name: name)
  end

  @doc "Cria um pedido. `attrs` deve conter `:customer_id` e `:items`."
  def create_order(attrs, server \\ __MODULE__) do
    safe_call(server, {:create, attrs})
  end

  @doc "Marca o pedido como pago."
  def pay_order(id, server \\ __MODULE__), do: safe_call(server, {:transition, id, :paid})

  @doc "Marca o pedido como enviado."
  def ship_order(id, server \\ __MODULE__), do: safe_call(server, {:transition, id, :shipped})

  @doc "Cancela o pedido."
  def cancel_order(id, server \\ __MODULE__),
    do: safe_call(server, {:transition, id, :cancelled})

  @doc """
  Consulta um pedido. A leitura ocorre diretamente no DETS, no processo do chamador,
  sem passar pelo GenServer, o que permite leituras simultâneas.
  """
  def get_order(id) do
    case :dets.lookup(@table, id) do
      [{^id, order}] -> {:ok, order}
      [] -> {:error, {:validation, :not_found}}
      {:error, reason} -> {:error, {:operational, reason}}
    end
  catch
    :exit, reason -> {:error, {:operational, {:table_unavailable, reason}}}
  end

  # ---------------------------------------------------------------------------
  # Callbacks do GenServer
  # ---------------------------------------------------------------------------

  @impl true
  def init(path) do
    # Garante que terminate/2 seja chamado em desligamentos supervisionados,
    # para fechar o DETS corretamente.
    Process.flag(:trap_exit, true)

    with {:ok, @table} <-
           :dets.open_file(@table,
             file: String.to_charlist(path),
             type: :set,
             auto_save: 60_000
           ),
         {:ok, next_id} <- load_next_id() do
      {:ok, %{next_id: next_id}}
    else
      {:error, reason} -> {:stop, {:operational, reason}}
    end
  end

  @impl true
  def handle_call({:create, attrs}, _from, state) do
    case validate_create(attrs) do
      {:error, errors} ->
        {:reply, {:error, {:validation, errors}}, state}

      {:ok, data} ->
        seq = state.next_id
        id = "ORD-" <> Integer.to_string(seq)

        order =
          Map.merge(data, %{
            id: id,
            status: :pending,
            created_at: DateTime.utc_now()
          })

        # Grava o pedido e o novo contador juntos, em uma única inserção de lista.
        case persist([{:next_id, seq + 1}, {id, order}]) do
          :ok -> {:reply, {:ok, order}, %{state | next_id: seq + 1}}
          {:error, _} = err -> {:reply, err, state}
        end
    end
  end

  def handle_call({:transition, id, to}, _from, state) do
    with {:ok, order} <- fetch(id),
         :ok <- check_transition(order.status, to),
         updated = Map.put(order, :status, to),
         :ok <- persist([{id, updated}]) do
      {:reply, {:ok, updated}, state}
    else
      {:error, _} = err -> {:reply, err, state}
    end
  end

  @impl true
  def terminate(_reason, _state) do
    :dets.close(@table)
    :ok
  end

  # ---------------------------------------------------------------------------
  # Persistência (DETS)
  # ---------------------------------------------------------------------------

  defp load_next_id do
    case :dets.lookup(@table, :next_id) do
      [{:next_id, n}] -> {:ok, n}
      [] -> {:ok, 1}
      {:error, reason} -> {:error, reason}
    end
  end

  defp fetch(id) do
    case :dets.lookup(@table, id) do
      [{^id, order}] -> {:ok, order}
      [] -> {:error, {:validation, :not_found}}
      {:error, reason} -> {:error, {:operational, reason}}
    end
  end

  # Grava os objetos e força a sincronização em disco antes de responder.
  defp persist(objects) do
    with :ok <- :dets.insert(@table, objects),
         :ok <- :dets.sync(@table) do
      :ok
    else
      {:error, reason} -> {:error, {:operational, reason}}
    end
  end

  # ---------------------------------------------------------------------------
  # Regras de negócio e validação
  # ---------------------------------------------------------------------------

  defp check_transition(from, to) do
    if to in Map.get(@transitions, from, []) do
      :ok
    else
      {:error, {:validation, {:invalid_transition, from, to}}}
    end
  end

  defp validate_create(attrs) when is_map(attrs) do
    customer_id = Map.get(attrs, :customer_id)
    items = Map.get(attrs, :items)

    top_errors = [
      check(valid_text?(customer_id), "customer_id deve ser texto não vazio")
    ]

    item_errors =
      case items do
        [_ | _] ->
          items
          |> Enum.with_index(1)
          |> Enum.flat_map(fn {item, i} -> validate_item(item, i) end)

        _ ->
          ["items deve ser uma lista não vazia"]
      end

    errors = Enum.reject(top_errors, &is_nil/1) ++ item_errors

    if errors == [] do
      normalized = Enum.map(items, &normalize_item/1)

      total =
        Enum.reduce(normalized, 0, fn i, acc -> acc + i.qty * i.unit_price_cents end)

      {:ok, %{customer_id: customer_id, items: normalized, total_cents: total}}
    else
      {:error, errors}
    end
  end

  defp validate_create(_), do: {:error, ["attrs deve ser um mapa"]}

  defp validate_item(item, i) when is_map(item) do
    sku = Map.get(item, :sku)
    qty = Map.get(item, :qty)
    price = Map.get(item, :unit_price_cents)

    [
      check(valid_text?(sku), "item #{i}: sku deve ser texto não vazio"),
      check(is_integer(qty) and qty > 0, "item #{i}: qty deve ser inteiro positivo"),
      check(
        is_integer(price) and price >= 0,
        "item #{i}: unit_price_cents deve ser inteiro não negativo"
      )
    ]
    |> Enum.reject(&is_nil/1)
  end

  defp validate_item(_, i), do: ["item #{i}: deve ser um mapa"]

  defp normalize_item(item) do
    %{
      sku: Map.get(item, :sku),
      qty: Map.get(item, :qty),
      unit_price_cents: Map.get(item, :unit_price_cents)
    }
  end

  defp valid_text?(v), do: is_binary(v) and String.trim(v) != ""

  defp check(true, _msg), do: nil
  defp check(_, msg), do: msg

  # ---------------------------------------------------------------------------
  # Auxiliares de chamada
  # ---------------------------------------------------------------------------

  # Converte falhas do processo (indisponível, timeout, crash) em erro operacional.
  defp safe_call(server, msg) do
    GenServer.call(server, msg, @call_timeout)
  catch
    :exit, reason -> {:error, {:operational, {:unavailable, reason}}}
  end
end
