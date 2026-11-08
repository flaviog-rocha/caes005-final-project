# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de estoque de uma loja, atendendo somente aos seguintes requisitos:
#
# 1. Implementar expiração automática de registros.
# 2. Validar os dados recebidos antes de realizar operações.
# 3. Detectar encerramentos inesperados de processos.
# 4. Quando fizer sentido para o domínio, considerar a comunicação com um banco de dados fictício e, opcionalmente, o uso de Ecto.Migration, sem tornar isso obrigatório.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule Loja.Estoque do
  @moduledoc """
  Gerenciamento de estoque em memória, com expiração automática dos registros.
  """

  alias Loja.Estoque.Server

  @default_ttl_ms 60_000

  def start_link(_opts \\ []) do
    Supervisor.start_link(__MODULE__.Supervisor, :ok, name: __MODULE__.Supervisor)
  end

  def add_stock(sku, quantity, opts \\ []) do
    with :ok <- validate_sku(sku),
         :ok <- validate_quantity(quantity),
         {:ok, ttl_ms} <- validate_ttl(opts) do
      GenServer.call(Server, {:add_stock, sku, quantity, ttl_ms})
    end
  end

  def remove_stock(sku, quantity) do
    with :ok <- validate_sku(sku),
         :ok <- validate_quantity(quantity) do
      GenServer.call(Server, {:remove_stock, sku, quantity})
    end
  end

  def get_stock(sku) do
    with :ok <- validate_sku(sku) do
      GenServer.call(Server, {:get_stock, sku})
    end
  end

  def all_stock do
    GenServer.call(Server, :all_stock)
  end

  def child_spec(opts) do
    %{
      id: __MODULE__.Supervisor,
      start: {__MODULE__, :start_link, [opts]},
      type: :supervisor
    }
  end

  defp validate_sku(sku) when is_binary(sku) do
    if String.trim(sku) == "", do: {:error, :invalid_sku}, else: :ok
  end

  defp validate_sku(_), do: {:error, :invalid_sku}

  defp validate_quantity(quantity) when is_integer(quantity) and quantity > 0, do: :ok
  defp validate_quantity(_), do: {:error, :invalid_quantity}

  defp validate_ttl(opts) when is_list(opts) do
    if Keyword.keyword?(opts) do
      ttl_ms = Keyword.get(opts, :ttl_ms, @default_ttl_ms)

      if is_integer(ttl_ms) and ttl_ms > 0 do
        {:ok, ttl_ms}
      else
        {:error, :invalid_ttl}
      end
    else
      {:error, :invalid_options}
    end
  end

  defp validate_ttl(_), do: {:error, :invalid_options}
end

defmodule Loja.Estoque.Supervisor do
  use Supervisor

  def start_link(_arg) do
    Supervisor.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  @impl true
  def init(:ok) do
    children = [
      %{
        id: Loja.Estoque.Server,
        start: {Loja.Estoque.Server, :start_link, [[]]},
        restart: :permanent,
        type: :worker
      }
    ]

    # O Supervisor monitora o processo de estoque e o reinicia se terminar inesperadamente.
    Supervisor.init(children, strategy: :one_for_one)
  end
end

defmodule Loja.Estoque.Server do
  use GenServer

  @default_ttl_ms 60_000

  def start_link(_opts \\ []) do
    GenServer.start_link(__MODULE__, %{}, name: __MODULE__)
  end

  @impl true
  def init(state), do: {:ok, state}

  @impl true
  def handle_call({:add_stock, sku, quantity, ttl_ms}, _from, state)
      when is_binary(sku) and is_integer(quantity) and quantity > 0 and
             is_integer(ttl_ms) and ttl_ms > 0 do
    state = purge_expired(state)
    previous_quantity = get_in(state, [sku, :quantity]) || 0
    state = cancel_entry_timer(state, sku)
    token = make_ref()
    expires_at = now_ms() + ttl_ms
    timer_ref = Process.send_after(self(), {:expire, sku, token}, ttl_ms)

    entry = %{
      quantity: previous_quantity + quantity,
      expires_at: expires_at,
      timer_ref: timer_ref,
      token: token
    }

    {:reply, {:ok, entry.quantity}, Map.put(state, sku, entry)}
  end

  def handle_call({:add_stock, _sku, _quantity, _ttl_ms}, _from, state) do
    {:reply, {:error, :invalid_request}, state}
  end

  def handle_call({:remove_stock, sku, quantity}, _from, state)
      when is_binary(sku) and is_integer(quantity) and quantity > 0 do
    state = purge_expired(state)

    case Map.fetch(state, sku) do
      {:ok, %{quantity: available} = entry} when quantity <= available ->
        remaining = available - quantity

        if remaining == 0 do
          Process.cancel_timer(entry.timer_ref)
          {:reply, :ok, Map.delete(state, sku)}
        else
          updated_entry = %{entry | quantity: remaining}
          {:reply, {:ok, remaining}, Map.put(state, sku, updated_entry)}
        end

      {:ok, _entry} ->
        {:reply, {:error, :insufficient_stock}, state}

      :error ->
        {:reply, {:error, :not_found}, state}
    end
  end

  def handle_call({:remove_stock, _sku, _quantity}, _from, state) do
    {:reply, {:error, :invalid_request}, state}
  end

  def handle_call({:get_stock, sku}, _from, state) when is_binary(sku) do
    state = purge_expired(state)

    reply =
      case Map.fetch(state, sku) do
        {:ok, entry} ->
          {:ok,
           %{
             sku: sku,
             quantity: entry.quantity,
             expires_in_ms: max(entry.expires_at - now_ms(), 0)
           }}

        :error ->
          {:error, :not_found}
      end

    {:reply, reply, state}
  end

  def handle_call(:all_stock, _from, state) do
    state = purge_expired(state)

    stock =
      state
      |> Enum.map(fn {sku, entry} ->
        %{
          sku: sku,
          quantity: entry.quantity,
          expires_in_ms: max(entry.expires_at - now_ms(), 0)
        }
      end)
      |> Enum.sort_by(& &1.sku)

    {:reply, stock, state}
  end

  def handle_call(_request, _from, state) do
    {:reply, {:error, :invalid_request}, state}
  end

  @impl true
  def handle_info({:expire, sku, token}, state) do
    case Map.get(state, sku) do
      %{token: ^token, expires_at: expires_at} = entry ->
        remaining_ms = expires_at - now_ms()

        if remaining_ms <= 0 do
          {:noreply, Map.delete(state, sku)}
        else
          timer_ref = Process.send_after(self(), {:expire, sku, token}, remaining_ms)
          {:noreply, Map.put(state, sku, %{entry | timer_ref: timer_ref})}
        end

      _ ->
        # Ignora notificações de temporizadores antigos após atualização ou remoção.
        {:noreply, state}
    end
  end

  def handle_info(_message, state), do: {:noreply, state}

  defp purge_expired(state) do
    current_time = now_ms()

    Enum.reduce(state, state, fn {sku, entry}, acc ->
      if entry.expires_at <= current_time do
        Process.cancel_timer(entry.timer_ref)
        Map.delete(acc, sku)
      else
        acc
      end
    end)
  end

  defp cancel_entry_timer(state, sku) do
    case Map.get(state, sku) do
      %{timer_ref: timer_ref} ->
        Process.cancel_timer(timer_ref)
        Map.delete(state, sku)

      nil ->
        state
    end
  end

  defp now_ms, do: System.monotonic_time(:millisecond)
end
