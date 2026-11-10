# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de votação online, atendendo somente aos seguintes requisitos:
#
# 1. Persistir os dados para recuperação após reinicializações.
# 2. Implementar expiração automática de registros.
# 3. Fazer uso de macros.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule VoteSystem do
  @moduledoc """
  Sistema de votação persistente com expiração automática usando ETS e GenServer.
  """
  use GenServer

  @table_name :votes_table
  @file_path "votes.dat"

  # --- Macro para simplificar a definição de comandos de votação ---
  defmacro defvote(name, do: block) do
    quote do
      def unquote(name)(key, value) do
        GenServer.call(__MODULE__, {:vote, unquote(name), key, value})
      end
    end
  end

  # --- API Pública ---
  def start_link(_), do: GenServer.start_link(__MODULE__, nil, name: __MODULE__)

  defvote(:upvote) do: :ok
  defvote(:downvote) do: :ok

  def get_votes(key) do
    case :ets.lookup(@table_name, key) do
      [{^key, value, _expiry}] -> value
      [] -> 0
    end
  end

  # --- Callbacks do GenServer ---
  @impl true
  def init(_) do
    table = :ets.new(@table_name, [:set, :public, :named_table])
    load_from_disk(table)
    
    # Timer para limpar registros expirados a cada minuto
    :timer.send_interval(60_000, :cleanup)
    
    {:ok, %{}}
  end

  @impl true
  def handle_call({:vote, type, key, _}, _from, state) do
    {value, _} = :ets.lookup(@table_name, key) |> case do
      [{^key, v, _}] -> {v, nil}
      [] -> {0, nil}
    end

    new_value = if type == :upvote, do: value + 1, else: value - 1
    # Expiração de 1 hora
    expiry = System.system_time(:second) + 3600
    :ets.insert(@table_name, {key, new_value, expiry})
    
    persist()
    {:reply, :ok, state}
  end

  @impl true
  def handle_info(:cleanup, state) do
    now = System.system_time(:second)
    :ets.foldl(fn {key, _, expiry}, _ ->
      if expiry < now, do: :ets.delete(@table_name, key)
    end, nil, @table_name)
    
    persist()
    {:noreply, state}
  end

  # --- Persistência ---
  defp persist do
    data = :ets.tab2list(@table_name)
    File.write!(@file_path, :erlang.term_to_binary(data))
  end

  defp load_from_disk(table) do
    if File.exists?(@file_path) do
      data = @file_path |> File.read!() |> :erlang.binary_to_term()
      :ets.insert(table, data)
    end
  end
end
