# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de transações financeiras, atendendo somente aos seguintes requisitos:
#
# 1. Persistir os dados para recuperação após reinicializações.
# 2. Impedir que uma mesma operação seja executada mais de uma vez.
# 3. Permitir consultar o estado atual das operações em andamento.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - ETS
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# Arquivo: finance_tx_manager.ex
# Gerenciador de transações financeiras com ETS, persistência em disco e idempotência.
# Escrita: serializada pelo GenServer, gravada em log (write-ahead) antes de atualizar a ETS.
# Leitura: direta na ETS, sem passar pelo processo.

defmodule FinanceTxManager do
  @moduledoc """
  Gerencia operações financeiras com:
    * persistência em log binário com fsync, recuperada no start;
    * idempotência: uma operação com o mesmo id não pode ser criada duas vezes;
    * consulta do estado atual e das operações em andamento.

  Estados: :pending -> :processing -> :completed | :failed (também :pending -> :failed).
  """

  use GenServer

  @table __MODULE__
  @default_path "finance_tx_manager.log"
  @in_progress [:pending, :processing]

  # Transições válidas: {estado_atual, novo_estado}
  @transitions %{
    {:pending, :processing} => true,
    {:pending, :failed} => true,
    {:processing, :completed} => true,
    {:processing, :failed} => true
  }

  # ---------------------------------------------------------------------------
  # API pública
  # ---------------------------------------------------------------------------

  @doc "Inicia o gerenciador. Opções: `:path` (arquivo de persistência)."
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Cria uma operação com `id` único e `amount` inteiro positivo.
  Retorna `{:error, :already_exists}` se o id já foi usado (inclusive antes de reinicializações).
  """
  def create(id, amount) when is_binary(id) and is_integer(amount) and amount > 0 do
    GenServer.call(__MODULE__, {:create, id, amount})
  end

  @doc "Move a operação `id` para o estado `to`, respeitando as transições válidas."
  def transition(id, to) when is_binary(id) and is_atom(to) do
    GenServer.call(__MODULE__, {:transition, id, to})
  end

  @doc "Consulta o estado atual de uma operação (leitura direta na ETS)."
  def get(id) when is_binary(id) do
    case :ets.lookup(@table, id) do
      [{^id, record}] -> {:ok, record}
      [] -> {:error, :not_found}
    end
  end

  @doc "Lista as operações em andamento (:pending ou :processing)."
  def in_progress do
    for {_id, record} <- :ets.tab2list(@table), record.status in @in_progress, do: record
  end

  # ---------------------------------------------------------------------------
  # GenServer callbacks
  # ---------------------------------------------------------------------------

  @impl true
  def init(opts) do
    path = Keyword.get(opts, :path, @default_path)

    # Tabela pública de leitura, escrita apenas pelo processo dono (este GenServer).
    :ets.new(@table, [:set, :named_table, :protected, read_concurrency: true])

    records = load(path)
    Enum.each(records, fn record -> :ets.insert(@table, {record.id, record}) end)

    # Reescreve o log de forma compacta e atômica (tmp + rename) e
    # descarta eventuais registros truncados por queda durante a escrita.
    :ok = compact(path, records)

    {:ok, io} = File.open(path, [:append, :binary])
    {:ok, %{path: path, io: io}}
  end

  @impl true
  def handle_call({:create, id, amount}, _from, state) do
    case :ets.lookup(@table, id) do
      [_] ->
        {:reply, {:error, :already_exists}, state}

      [] ->
        now = now()

        record = %{
          id: id,
          amount: amount,
          status: :pending,
          created_at: now,
          updated_at: now
        }

        case append(state, record) do
          :ok ->
            :ets.insert(@table, {id, record})
            {:reply, {:ok, record}, state}

          {:error, reason} ->
            {:reply, {:error, reason}, state}
        end
    end
  end

  def handle_call({:transition, id, to}, _from, state) do
    case :ets.lookup(@table, id) do
      [] ->
        {:reply, {:error, :not_found}, state}

      [{^id, record}] ->
        if Map.has_key?(@transitions, {record.status, to}) do
          updated = %{record | status: to, updated_at: now()}

          case append(state, updated) do
            :ok ->
              :ets.insert(@table, {id, updated})
              {:reply, {:ok, updated}, state}

            {:error, reason} ->
              {:reply, {:error, reason}, state}
          end
        else
          {:reply, {:error, {:invalid_transition, record.status, to}}, state}
        end
    end
  end

  @impl true
  def terminate(_reason, %{io: io}) do
    File.close(io)
    :ok
  end

  # ---------------------------------------------------------------------------
  # Persistência
  # ---------------------------------------------------------------------------

  # Formato de cada registro no log: <<tamanho::32, termo_binário::binary>>
  defp frame(record) do
    bin = :erlang.term_to_binary(record)
    <<byte_size(bin)::32, bin::binary>>
  end

  # Grava o registro no log e força sincronização com o disco.
  defp append(%{io: io}, record) do
    with :ok <- IO.binwrite(io, frame(record)),
         :ok <- :file.sync(io) do
      :ok
    end
  end

  # Carrega o log; o último registro de cada id prevalece.
  defp load(path) do
    case File.read(path) do
      {:ok, bin} -> bin |> decode(%{}) |> Map.values()
      {:error, :enoent} -> []
      {:error, reason} -> raise "falha ao ler #{path}: #{inspect(reason)}"
    end
  end

  defp decode(<<len::32, term::binary-size(len), rest::binary>>, acc) do
    record = :erlang.binary_to_term(term, [:safe])
    decode(rest, Map.put(acc, record.id, record))
  end

  # Fim do arquivo ou registro truncado no final (queda durante escrita): ignorado.
  defp decode(_remaining, acc), do: acc

  # Reescreve o log com apenas o estado atual, de forma atômica.
  defp compact(path, records) do
    tmp = path <> ".tmp"
    {:ok, io} = File.open(tmp, [:write, :binary])

    result =
      Enum.reduce_while(records, :ok, fn record, :ok ->
        case IO.binwrite(io, frame(record)) do
          :ok -> {:cont, :ok}
          {:error, reason} -> {:halt, {:error, reason}}
        end
      end)

    with :ok <- result,
         :ok <- :file.sync(io),
         :ok <- File.close(io),
         :ok <- File.rename(tmp, path) do
      :ok
    else
      error ->
        File.close(io)
        File.rm(tmp)
        error
    end
  end

  defp now, do: System.system_time(:millisecond)
end
