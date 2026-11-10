# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de um sistema de cache, atendendo somente aos seguintes requisitos:
#
# 1. Manter um histórico das operações realizadas.
# 2. Persistir os dados para recuperação após reinicializações.
# 3. Permitir comunicação entre diferentes processos.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule PersistentCache do
  @moduledoc """
  Sistema de cache persistente baseado em ETS para performance e 
  um arquivo de log (AOF) para recuperação após reinicialização.
  """
  use GenServer

  @table_name :persistent_cache_table
  @log_file "cache_operations.log"

  # --- API Pública ---

  def start_link(_), do: GenServer.start_link(__MODULE__, nil, name: __MODULE__)

  def put(key, value) do
    GenServer.call(__MODULE__, {:put, key, value})
  end

  def get(key) do
    case :ets.lookup(@table_name, key) do
      [{^key, value}] -> {:ok, value}
      [] -> :error
    end
  end

  # --- Callbacks do GenServer ---

  @impl true
  def init(_) do
    :ets.new(@table_name, [:set, :public, :named_table, read_concurrency: true])
    load_from_log()
    {:ok, nil}
  end

  @impl true
  def handle_call({:put, key, value}, _from, state) do
    :ets.insert(@table_name, {key, value})
    append_to_log({:put, key, value})
    {:reply, :ok, state}
  end

  # --- Funções Privadas ---

  defp append_to_log(op) do
    {:ok, file} = File.open(@log_file, [:append])
    IO.binwrite(file, :erlang.term_to_binary(op) <> "\n")
    File.close(file)
  end

  defp load_from_log do
    if File.exists?(@log_file) do
      @log_file
      |> File.stream!([], :line)
      |> Enum.each(fn line ->
        case :erlang.binary_to_term(String.trim(line)) do
          {:put, key, value} -> :ets.insert(@table_name, {key, value})
          _ -> :ignore
        end
      end)
    end
  end
end
