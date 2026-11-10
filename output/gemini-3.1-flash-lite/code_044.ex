# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de transações financeiras, atendendo somente aos seguintes requisitos:
#
# 1. Quando fizer sentido para o domínio, considerar a comunicação com um banco de dados fictício e, opcionalmente, o uso de Ecto.Migration, sem tornar isso obrigatório.
# 2. Permitir a execução de operações simultâneas.
# 3. Permitir limitar a quantidade de operações simultâneas.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - DynamicSupervisor
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule TransactionManager do
  @moduledoc """
  Gerenciador de transações financeiras utilizando DynamicSupervisor para
  execução concorrente com limitação de carga via :poolboy ou semáforos,
  aqui implementado via GenServer para controle de concorrência.
  """

  use Supervisor

  def start_link(init_arg) do
    Supervisor.start_link(__MODULE__, init_arg, name: __MODULE__)
  end

  @impl true
  def init(_arg) do
    children = [
      {DynamicSupervisor, name: TransactionSupervisor, strategy: :one_for_one},
      {TransactionLimiter, max_concurrent: 5}
    ]

    Supervisor.init(children, strategy: :one_for_all)
  end

  def process_transaction(data) do
    TransactionLimiter.execute(fn ->
      DynamicSupervisor.start_child(TransactionSupervisor, {TransactionWorker, data})
    end)
  end
end

defmodule TransactionLimiter do
  use GenServer

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  @impl true
  def init(opts), do: {:ok, %{max: opts[:max_concurrent], running: 0, queue: :queue.new()}}

  def execute(fun) do
    GenServer.call(__MODULE__, {:request, fun})
  end

  @impl true
  def handle_call({:request, fun}, from, state) do
    if state.running < state.max do
      spawn_transaction(fun)
      {:reply, :ok, %{state | running: state.running + 1}}
    else
      {:noreply, %{state | queue: :queue.in({from, fun}, state.queue)}}
    end
  end

  @impl true
  def handle_info(:done, state) do
    case :queue.out(state.queue) do
      {{:value, {from, fun}}, new_queue} ->
        spawn_transaction(fun)
        GenServer.reply(from, :ok)
        {:noreply, %{state | queue: new_queue}}
      {:empty, _} ->
        {:noreply, %{state | running: state.running - 1}}
    end
  end

  defp spawn_transaction(fun) do
    spawn(fn ->
      fun.()
      send(TransactionLimiter, :done)
    end)
  end
end

defmodule TransactionWorker do
  use Agent, restart: :temporary

  def start_link(data) do
    Agent.start_link(fn ->
      # Simulação de interação com Banco de Dados
      # Em um cenário real com Ecto: Repo.insert(%Transaction{...})
      IO.puts("Processando transação: #{inspect(data)}")
      Process.sleep(1000)
      IO.puts("Transação concluída: #{inspect(data)}")
    end)
  end
end
