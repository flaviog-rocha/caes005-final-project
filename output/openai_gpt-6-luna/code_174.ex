# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de estoque de uma loja, atendendo somente aos seguintes requisitos:
#
# 1. Processar grandes volumes de dados sem carregá-los integralmente na memória.
# 2. Permitir consultar o estado atual das operações em andamento.
# 3. Manter um histórico das operações realizadas.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule Loja.Estoque do
  use GenServer

  @tabela __MODULE__.Tabela

  def start_link(opts \\ []) do
    case Keyword.fetch(opts, :name) do
      {:ok, name} -> GenServer.start_link(__MODULE__, opts, name: name)
      :error -> GenServer.start_link(__MODULE__, opts, name: __MODULE__)
    end
  end

  @doc """
  Processa um enumerable de `{sku, quantidade}` sem acumulá-lo na memória.
  A ação deve ser `:adicionar` ou `:remover`.
  """
  def processar(enumerable, action, server \\ __MODULE__)
      when action in [:adicionar, :remover] do
    operation_id = GenServer.call(server, {:iniciar_operacao, action})

    try do
      Enum.each(enumerable, fn item ->
        GenServer.call(server, {:processar_item, operation_id, action, item}, :infinity)
      end)

      GenServer.call(server, {:finalizar_operacao, operation_id, :concluida, nil})
    rescue
      exception ->
        GenServer.call(
          server,
          {:finalizar_operacao, operation_id, :falhou, Exception.message(exception)}
        )

        reraise exception, __STACKTRACE__
    catch
      kind, reason ->
        GenServer.call(
          server,
          {:finalizar_operacao, operation_id, :falhou, inspect({kind, reason})}
        )

        :erlang.raise(kind, reason, __STACKTRACE__)
    end
  end

  def processar(_enumerable, action, _server) do
    raise ArgumentError, "ação inválida: #{inspect(action)}"
  end

  @doc "Retorna as operações que ainda estão em andamento."
  def estado(server \\ __MODULE__) do
    GenServer.call(server, :estado)
  end

  @doc "Retorna a quantidade disponível de um SKU."
  def estoque(sku, server \\ __MODULE__) when is_binary(sku) do
    GenServer.call(server, {:estoque, sku})
  end

  @doc """
  Retorna um stream do histórico persistido em disco.
  Cada elemento representa o início, um item ou o encerramento de uma operação.
  """
  def historico(server \\ __MODULE__) do
    path = GenServer.call(server, :caminho_historico)

    Stream.resource(
      fn -> File.open!(path, [:read, :binary]) end,
      &proximo_registro/1,
      &File.close/1
    )
  end

  @impl true
  def init(opts) do
    inventory_path = Keyword.get(opts, :arquivo_estoque, "estoque.dets")
    history_path = Keyword.get(opts, :arquivo_historico, "historico.estoque")

    inventory_path |> Path.dirname() |> File.mkdir_p!()
    history_path |> Path.dirname() |> File.mkdir_p!()

    with {:ok, @tabela} <-
           :dets.open_file(@tabela,
             file: String.to_charlist(inventory_path),
             type: :set,
             repair: true
           ),
         {:ok, history_io} <- File.open(history_path, [:append, :binary]) do
      {:ok,
       %{
         history_path: history_path,
         history_io: history_io,
         operacoes: %{}
       }}
    else
      {:error, reason} -> {:stop, reason}
    end
  end

  @impl true
  def handle_call({:iniciar_operacao, action}, _from, state) do
    id = "op-#{System.unique_integer([:positive, :monotonic])}"
    now = timestamp()

    operation = %{
      id: id,
      acao: action,
      iniciada_em: now,
      processados: 0,
      sucessos: 0,
      falhas: 0
    }

    append_history!(state.history_io, %{
      tipo: :inicio,
      operacao_id: id,
      acao: action,
      instante: now
    })

    {:reply, id, put_in(state, [:operacoes, id], operation)}
  end

  def handle_call({:processar_item, id, action, item}, _from, state) do
    operation = Map.fetch!(state.operacoes, id)
    {result, event} = aplicar_item(action, item)

    append_history!(state.history_io, Map.merge(event, %{
      tipo: :item,
      operacao_id: id,
      instante: timestamp()
    }))

    operation =
      operation
      |> Map.update!(:processados, &(&1 + 1))
      |> then(fn op ->
        case result do
          :ok -> Map.update!(op, :sucessos, &(&1 + 1))
          :erro -> Map.update!(op, :falhas, &(&1 + 1))
        end
      end)

    {:reply, result, put_in(state, [:operacoes, id], operation)}
  end

  def handle_call({:finalizar_operacao, id, status, motivo}, _from, state) do
    operation = Map.fetch!(state.operacoes, id)

    append_history!(state.history_io, %{
      tipo: :fim,
      operacao_id: id,
      status: status,
      motivo: motivo,
      instante: timestamp(),
      processados: operation.processados,
      sucessos: operation.sucessos,
      falhas: operation.falhas
    })

    summary =
      operation
      |> Map.put(:status, status)
      |> Map.put(:motivo, motivo)
      |> Map.put(:finalizada_em, timestamp())

    {:reply, summary, update_in(state.operacoes, &Map.delete(&1, id))}
  end

  def handle_call(:estado, _from, state) do
    {:reply, state.operacoes, state}
  end

  def handle_call({:estoque, sku}, _from, state) do
    quantity =
      case :dets.lookup(@tabela, sku) do
        [{^sku, value}] -> value
        [] -> 0
      end

    {:reply, quantity, state}
  end

  def handle_call(:caminho_historico, _from, state) do
    {:reply, state.history_path, state}
  end

  @impl true
  def terminate(_reason, state) do
    File.close(state.history_io)
    :dets.close(@tabela)
    :ok
  end

  defp aplicar_item(action, {sku, quantity})
       when is_binary(sku) and byte_size(sku) > 0 and is_integer(quantity) and quantity > 0 do
    current =
      case :dets.lookup(@tabela, sku) do
        [{^sku, value}] -> value
        [] -> 0
      end

    case action do
      :adicionar ->
        updated = current + quantity
        :ok = :dets.insert(@tabela, {sku, updated})

        {:ok,
         %{
           acao: action,
           sku: sku,
           quantidade: quantity,
           resultado: :aplicado,
           estoque_apos: updated
         }}

      :remover when current >= quantity ->
        updated = current - quantity

        if updated == 0 do
          :ok = :dets.delete(@tabela, sku)
        else
          :ok = :dets.insert(@tabela, {sku, updated})
        end

        {:ok,
         %{
           acao: action,
           sku: sku,
           quantidade: quantity,
           resultado: :aplicado,
           estoque_apos: updated
         }}

      :remover ->
        {:erro,
         %{
           acao: action,
           sku: sku,
           quantidade: quantity,
           resultado: :estoque_insuficiente,
           estoque_apos: current
         }}
    end
  end

  defp aplicar_item(action, item) do
    {:erro,
     %{
       acao: action,
       entrada_invalida: inspect(item),
       resultado: :entrada_invalida
     }}
  end

  defp append_history!(io, record) do
    binary = :erlang.term_to_binary(record)
    frame = <<byte_size(binary)::unsigned-big-32, binary::binary>>
    :ok = IO.binwrite(io, frame)
  end

  defp proximo_registro(io) do
    case read_exact(io, 4) do
      :eof ->
        {:halt, io}

      {:ok, <<size::unsigned-big-32>>} ->
        case read_exact(io, size) do
          {:ok, binary} ->
            {[binary_to_term(binary, [:safe])], io}

          :eof ->
            raise "histórico incompleto: registro truncado"
        end

      {:error, reason} ->
        raise "erro ao ler histórico: #{inspect(reason)}"
    end
  end

  defp read_exact(_io, 0), do: {:ok, <<>>}

  defp read_exact(io, size), do: read_exact(io, size, <<>>)

  defp read_exact(_io, 0, acc), do: {:ok, acc}

  defp read_exact(io, remaining, acc) do
    case IO.binread(io, remaining) do
      :eof when byte_size(acc) == 0 ->
        :eof

      :eof ->
        {:error, :registro_truncado}

      {:error, reason} ->
        {:error, reason}

      data ->
        read_exact(io, remaining - byte_size(data), acc <> data)
    end
  end

  defp timestamp, do: DateTime.utc_now() |> DateTime.to_iso8601()
end
