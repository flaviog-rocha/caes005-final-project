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

# arquivo: orders.exs
#
# Gerenciamento de pedidos de e-commerce com:
#   1. Operações simultâneas: leituras são feitas diretamente pelos processos
#      chamadores (o DETS serializa acessos internamente) e escritas passam por
#      um GenServer que garante consistência em operações de leitura-modificação-escrita.
#   2. Separação de erros:
#        {:error, {:validation, detalhe}}  -> culpa do cliente (dados inválidos,
#                                             pedido inexistente, transição proibida)
#        {:error, {:operational, detalhe}} -> falha do sistema (DETS, timeout,
#                                             processo indisponível)
#   3. Persistência: DETS, com sincronização após cada escrita e recuperação
#      automática do arquivo na reinicialização.
#
# Execução: iex orders.exs  (ou: elixir orders.exs)

defmodule Ecommerce.Orders.Order do
  @moduledoc """
  Estrutura que representa um pedido persistido.
  """

  @enforce_keys [:id, :customer_id, :items, :total, :status, :created_at]
  defstruct [:id, :customer_id, :items, :total, :status, :created_at, :updated_at]
end

defmodule Ecommerce.Orders.Validator do
  @moduledoc """
  Validação pura de entradas. Todos os erros retornados aqui são do tipo
  `{:validation, detalhes}`, pois indicam dados incorretos enviados pelo cliente.
  Não há efeitos colaterais, portanto esta validação roda concorrentemente
  no processo chamador, sem passar pelo GenServer.
  """

  @spec validate_new(term()) ::
          {:ok, %{customer_id: binary(), items: list(map()), total: non_neg_integer()}}
          | {:error, {:validation, list()}}
  def validate_new(attrs) when is_map(attrs) do
    customer_id = Map.get(attrs, :customer_id)
    items = Map.get(attrs, :items)

    errors = customer_errors(customer_id) ++ items_errors(items)

    case errors do
      [] ->
        # Mantém somente os campos conhecidos e calcula o total em centavos.
        normalized = Enum.map(items, &Map.take(&1, [:sku, :quantity, :unit_price]))
        total = Enum.reduce(normalized, 0, &(&1.quantity * &1.unit_price + &2))

        {:ok, %{customer_id: customer_id, items: normalized, total: total}}

      _ ->
        {:error, {:validation, errors}}
    end
  end

  def validate_new(_), do: {:error, {:validation, [:attrs_must_be_a_map]}}

  # --- Regras de cliente ---

  defp customer_errors(id) when is_binary(id) and byte_size(id) > 0, do: []
  defp customer_errors(_), do: [{:customer_id, :required}]

  # --- Regras de itens ---

  defp items_errors(items) when is_list(items) and items != [] do
    items
    |> Enum.with_index()
    |> Enum.flat_map(fn {item, idx} -> item_errors(item, idx) end)
  end

  defp items_errors(_), do: [{:items, :must_not_be_empty}]

  defp item_errors(%{sku: sku, quantity: qty, unit_price: price}, idx) do
    []
    |> add_if(not (is_binary(sku) and byte_size(sku) > 0), {idx, :sku, :required})
    |> add_if(not (is_integer(qty) and qty > 0), {idx, :quantity, :must_be_positive_integer})
    |> add_if(
      not (is_integer(price) and price >= 0),
      {idx, :unit_price, :must_be_non_negative_integer}
    )
  end

  defp item_errors(_, idx), do: [{idx, :item, :invalid_format}]

  defp add_if(acc, true, error), do: acc ++ [error]
  defp add_if(acc, false, _error), do: acc
end

defmodule Ecommerce.Orders do
  @moduledoc """
  API pública e GenServer responsável pelas escritas no DETS.

  Estratégia de concorrência:
    * Leituras (`get_order/1`, `list_orders/1`) acessam o DETS diretamente
      no processo chamador, em paralelo.
    * Escritas (`create_order/1`, `transition/2`) são serializadas pelo GenServer,
      evitando condições de corrida em leitura-modificação-escrita, como
      transições de status concorrentes sobre o mesmo pedido.

  Estratégia de erros:
    * `{:error, {:validation, _}}` para erros de regra/entrada.
    * `{:error, {:operational, _}}` para falhas de infraestrutura.
  """

  use GenServer

  alias Ecommerce.Orders.{Order, Validator}

  @table :ecommerce_orders_dets
  @seq_key :__sequence__
  @call_timeout 5_000
  @default_file "orders.dets"

  # Transições de status permitidas (máquina de estados do pedido)
  @transitions %{
    pending: [:paid, :cancelled],
    paid: [:shipped, :cancelled],
    shipped: [],
    cancelled: []
  }

  # ---------------------------------------------------------------------------
  # API pública
  # ---------------------------------------------------------------------------

  @doc "Inicia o processo. Opções: `:data_file` (caminho do arquivo DETS) e `:name`."
  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc """
  Cria um pedido. Valida a entrada no processo chamador e, se válida,
  envia a persistência ao GenServer.

  Exemplo:
      Ecommerce.Orders.create_order(%{
        customer_id: "cust-1",
        items: [%{sku: "ABC", quantity: 2, unit_price: 1500}]
      })
  """
  @spec create_order(map()) :: {:ok, Order.t()} | {:error, tuple()}
  def create_order(attrs) do
    case Validator.validate_new(attrs) do
      {:ok, data} -> call({:create, data})
      {:error, _} = error -> error
    end
  end

  @doc "Busca um pedido pelo id. Leitura concorrente, sem passar pelo GenServer."
  @spec get_order(term()) :: {:ok, Order.t()} | {:error, tuple()}
  def get_order(id) when is_binary(id) do
    case dets(fn -> :dets.lookup(@table, id) end) do
      {:ok, [{^id, order}]} -> {:ok, order}
      {:ok, []} -> {:error, {:validation, :not_found}}
      {:error, _} = error -> error
    end
  end

  def get_order(_), do: {:error, {:validation, :invalid_id}}

  @doc "Lista pedidos, opcionalmente filtrando por status."
  @spec list_orders(atom() | nil) :: {:ok, [Order.t()]} | {:error, tuple()}
  def list_orders(status \\ nil) do
    match_spec = [{{:"$1", :"$2"}, [{:is_binary, :"$1"}], [:"$2"]}]

    case dets(fn -> :dets.select(@table, match_spec) end) do
      {:ok, orders} ->
        filtered =
          orders
          |> Enum.filter(&(is_nil(status) or &1.status == status))
          |> Enum.sort_by(& &1.id)

        {:ok, filtered}

      {:error, _} = error ->
        error
    end
  end

  @doc "Altera o status de um pedido, respeitando a máquina de estados."
  @spec transition(binary(), atom()) :: {:ok, Order.t()} | {:error, tuple()}
  def transition(id, new_status) when is_binary(id) and is_atom(new_status) do
    call({:transition, id, new_status})
  end

  def transition(_, _), do: {:error, {:validation, :invalid_arguments}}

  # ---------------------------------------------------------------------------
  # Chamadas ao GenServer com captura de falhas de infraestrutura
  # ---------------------------------------------------------------------------

  defp call(message) do
    GenServer.call(__MODULE__, message, @call_timeout)
  catch
    # Timeout, processo inexistente ou encerrado: falha operacional.
    :exit, reason -> {:error, {:operational, {:unavailable, reason}}}
  end

  # Envolve operações DETS, convertendo erros do módulo e exceções em
  # `{:error, {:operational, _}}`. Retorna `{:ok, resultado}` em caso de sucesso.
  defp dets(fun) do
    case fun.() do
      {:error, reason} -> {:error, {:operational, reason}}
      result -> {:ok, result}
    end
  rescue
    e -> {:error, {:operational, Exception.message(e)}}
  catch
    kind, reason -> {:error, {:operational, {kind, reason}}}
  end

  # ---------------------------------------------------------------------------
  # Callbacks do GenServer
  # ---------------------------------------------------------------------------

  @impl true
  def init(opts) do
    # Permite que terminate/2 seja chamado em desligamentos controlados,
    # fechando o DETS corretamente.
    Process.flag(:trap_exit, true)

    path =
      opts
      |> Keyword.get(:data_file, @default_file)
      |> String.to_charlist()

    # O DETS recupera automaticamente (repair) arquivos não fechados corretamente.
    case :dets.open_file(@table, file: path, type: :set) do
      {:ok, @table} ->
        # Garante a existência do contador de sequência (idempotente).
        :dets.insert_new(@table, {@seq_key, 0})
        {:ok, %{}}

      {:error, reason} ->
        {:stop, {:dets_open_failed, reason}}
    end
  end

  @impl true
  def handle_call({:create, data}, _from, state) do
    reply =
      with {:ok, seq} <- dets(fn -> :dets.update_counter(@table, @seq_key, 1) end),
           order = build_order(seq, data),
           {:ok, _} <- persist(order) do
        {:ok, order}
      end

    {:reply, reply, state}
  end

  def handle_call({:transition, id, to}, _from, state) do
    reply =
      with {:ok, order} <- get_order(id),
           :ok <- check_transition(order.status, to),
           updated = %{order | status: to, updated_at: DateTime.utc_now()},
           {:ok, _} <- persist(updated) do
        {:ok, updated}
      end

    {:reply, reply, state}
  end

  @impl true
  def terminate(_reason, _state) do
    # Fecha o DETS, garantindo que os dados sejam gravados no disco.
    :dets.close(@table)
    :ok
  end

  # ---------------------------------------------------------------------------
  # Funções auxiliares
  # ---------------------------------------------------------------------------

  defp build_order(seq, data) do
    now = DateTime.utc_now()

    %Order{
      id: format_id(seq),
      customer_id: data.customer_id,
      items: data.items,
      total: data.total,
      status: :pending,
      created_at: now,
      updated_at: now
    }
  end

  defp format_id(seq) do
    "ORD-" <> String.pad_leading(Integer.to_string(seq), 12, "0")
  end

  # Grava o pedido e força a sincronização com o disco.
  defp persist(%Order{id: id} = order) do
    dets(fn ->
      with :ok <- :dets.insert(@table, {id, order}) do
        :dets.sync(@table)
      end
    end)
  end

  defp check_transition(from, to) do
    if to in Map.get(@transitions, from, []) do
      :ok
    else
      {:error, {:validation, {:invalid_transition, from, to}}}
    end
  end
end

# ---------------------------------------------------------------------------
# Exemplo de uso (para iex):
#
#   {:ok, _} = Ecommerce.Orders.start_link(data_file: "pedidos.dets")
#
#   {:ok, pedido} =
#     Ecommerce.Orders.create_order(%{
#       customer_id: "cliente-42",
#       items: [%{sku: "NOTEBOOK", quantity: 1, unit_price: 350_000}]
#     })
#
#   # Erro de validação (culpa do cliente):
#   {:error, {:validation, _}} = Ecommerce.Orders.create_order(%{customer_id: "", items: []})
#
#   # Transição inválida (regra de negócio):
#   {:error, {:validation, {:invalid_transition, :pending, :shipped}}} =
#     Ecommerce.Orders.transition(pedido.id, :shipped)
#
#   {:ok, _} = Ecommerce.Orders.transition(pedido.id, :paid)
#   {:ok, _} = Ecommerce.Orders.transition(pedido.id, :shipped)
#
#   # Após reiniciar a VM, os dados continuam disponíveis:
#   {:ok, _} = Ecommerce.Orders.start_link(data_file: "pedidos.dets")
#   {:ok, _} = Ecommerce.Orders.get_order(pedido.id)
# ---------------------------------------------------------------------------
