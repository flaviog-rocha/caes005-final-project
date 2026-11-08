# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de troca de mensagens entre usuários, atendendo somente aos seguintes requisitos:
#
# 1. Recuperar processos após falhas inesperadas.
# 2. Garantir a consistência dos dados durante operações concorrentes.
# 3. Fazer uso de macros.
# 4. Permitir cadastrar, consultar, atualizar e remover registros.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule Mensagens.Mensagem do
  @enforce_keys [:id, :de, :para, :conteudo]
  defstruct [:id, :de, :para, :conteudo]
end

defmodule Mensagens.CRUD do
  defmacro __using__(_opts) do
    quote do
      def create(attrs), do: GenServer.call(Mensagens.Worker, {:create, attrs})
      def get(id), do: GenServer.call(Mensagens.Worker, {:get, id})
      def update(id, attrs), do: GenServer.call(Mensagens.Worker, {:update, id, attrs})
      def delete(id), do: GenServer.call(Mensagens.Worker, {:delete, id})
      def all, do: GenServer.call(Mensagens.Worker, :all)
      def messages_for(user), do: GenServer.call(Mensagens.Worker, {:messages_for, user})
    end
  end
end

defmodule Mensagens.TableKeeper do
  use GenServer

  @table Mensagens.Table

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, :ok, opts)
  end

  @impl true
  def init(:ok) do
    :ets.new(@table, [:named_table, :set, :public, read_concurrency: true])
    {:ok, @table}
  end
end

defmodule Mensagens.Store do
  use GenServer

  alias Mensagens.Mensagem

  @table Mensagens.Table

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, :ok, opts)
  end

  @impl true
  def init(:ok) do
    {:ok, %{mensagens: carregar_mensagens()}}
  end

  @impl true
  def handle_call({:create, attrs}, _from, state) do
    with {:ok, campos} <- validar_campos(attrs) do
      id = :ets.update_counter(@table, :next_id, {2, 1}, {:next_id, 0})
      mensagem = struct!(Mensagem, Map.put(campos, :id, id))
      :ets.insert(@table, {{:mensagem, id}, mensagem})
      {:reply, {:ok, mensagem}, %{state | mensagens: Map.put(state.mensagens, id, mensagem)}}
    else
      erro -> {:reply, erro, state}
    end
  end

  def handle_call({:get, id}, _from, state) do
    resposta =
      case Map.fetch(state.mensagens, id) do
        {:ok, mensagem} -> {:ok, mensagem}
        :error -> {:error, :nao_encontrada}
      end

    {:reply, resposta, state}
  end

  def handle_call({:update, id, attrs}, _from, state) do
    case Map.fetch(state.mensagens, id) do
      {:ok, mensagem} ->
        campos_atuais = Map.take(mensagem, [:de, :para, :conteudo])

        with {:ok, campos} <- validar_campos(Map.merge(campos_atuais, normalizar(attrs))) do
          atualizada = struct!(Mensagem, Map.put(campos, :id, id))
          :ets.insert(@table, {{:mensagem, id}, atualizada})

          {:reply, {:ok, atualizada},
           %{state | mensagens: Map.put(state.mensagens, id, atualizada)}}
        else
          erro -> {:reply, erro, state}
        end

      :error ->
        {:reply, {:error, :nao_encontrada}, state}
    end
  end

  def handle_call({:delete, id}, _from, state) do
    case Map.pop(state.mensagens, id) do
      {nil, _mensagens} ->
        {:reply, {:error, :nao_encontrada}, state}

      {mensagem, mensagens} ->
        :ets.delete(@table, {:mensagem, id})
        {:reply, {:ok, mensagem}, %{state | mensagens: mensagens}}
    end
  end

  def handle_call(:all, _from, state) do
    mensagens = state.mensagens |> Map.values() |> Enum.sort_by(& &1.id)
    {:reply, {:ok, mensagens}, state}
  end

  def handle_call({:messages_for, user}, _from, state) do
    mensagens =
      state.mensagens
      |> Map.values()
      |> Enum.filter(&(&1.de == user or &1.para == user))
      |> Enum.sort_by(& &1.id)

    {:reply, {:ok, mensagens}, state}
  end

  defp carregar_mensagens do
    @table
    |> :ets.tab2list()
    |> Enum.reduce(%{}, fn
      {{:mensagem, id}, mensagem}, acc -> Map.put(acc, id, mensagem)
      _, acc -> acc
    end)
  end

  defp validar_campos(attrs) do
    campos = normalizar(attrs)

    case Map.take(campos, [:de, :para, :conteudo]) do
      %{de: de, para: para, conteudo: conteudo} when not is_nil(de) and not is_nil(para) and
                                                        is_binary(conteudo) and byte_size(conteudo) > 0 ->
        {:ok, %{de: de, para: para, conteudo: conteudo}}

      _ ->
        {:error, :campos_invalidos}
    end
  end

  defp normalizar(attrs) when is_map(attrs), do: Map.new(attrs, fn {k, v} -> {chave(k), v} end)
  defp normalizar(attrs) when is_list(attrs), do: attrs |> Map.new() |> normalizar()
  defp normalizar(_attrs), do: %{}

  defp chave(key) when is_atom(key), do: key

  defp chave(key) when is_binary(key) do
    case key do
      "de" -> :de
      "para" -> :para
      "conteudo" -> :conteudo
      _ -> key
    end
  end

  defp chave(key), do: key
end

defmodule Mensagens.Worker do
  use GenServer

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, :ok, opts)
  end

  @impl true
  def init(:ok), do: {:ok, %{}}

  @impl true
  def handle_call(request, _from, state) do
    resposta = GenServer.call(Mensagens.Store, request)
    {:reply, resposta, state}
  end
end

defmodule Mensagens do
  use Mensagens.CRUD

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, :ok, Keyword.put_new(opts, :name, __MODULE__))
  end

  def send_message(de, para, conteudo) do
    create(%{de: de, para: para, conteudo: conteudo})
  end

  @impl true
  def init(:ok) do
    children = [
      {Mensagens.TableKeeper, name: Mensagens.TableKeeper},
      {Mensagens.Store, name: Mensagens.Store},
      {Mensagens.Worker, name: Mensagens.Worker}
    ]

    Supervisor.init(children, strategy: :rest_for_one)
  end
end
