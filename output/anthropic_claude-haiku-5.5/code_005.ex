# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de estoque de uma loja, atendendo somente aos seguintes requisitos:
#
# 1. Manter um histórico das operações realizadas.
# 2. Detectar encerramentos inesperados de processos.
# 3. Recuperar processos após falhas inesperadas.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - Stream
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# Gerenciamento de estoque com histórico, detecção de falhas e recuperação.
# Execute com: elixir estoque.exs

# Armazena o histórico de operações (event log). É a fonte da verdade:
# o estado do estoque é reconstruído a partir dele após uma falha.
defmodule Estoque.Historico do
  use Agent

  def start_link(_opts) do
    # Estado: lista de registros em ordem reversa (mais recente primeiro) e sequência.
    Agent.start_link(fn -> %{registros: [], seq: 0} end, name: __MODULE__)
  end

  # Registra um evento atribuindo id sequencial e timestamp.
  def registrar(evento) do
    Agent.get_and_update(__MODULE__, fn %{registros: regs, seq: seq} ->
      seq = seq + 1
      registro = Map.merge(%{id: seq, em: System.system_time(:millisecond)}, evento)
      {registro, %{registros: [registro | regs], seq: seq}}
    end)
  end

  # Retorna um Stream lazy do histórico em ordem cronológica, opcionalmente filtrado.
  def stream(filtro \\ fn _ -> true end) do
    Agent.get(__MODULE__, & &1.registros)
    |> Enum.reverse()
    |> Stream.filter(filtro)
  end
end

# Processo que mantém o estoque. Em caso de falha, é reiniciado pelo supervisor
# e reconstrói seu estado reproduzindo o histórico.
defmodule Estoque.Servidor do
  use GenServer

  def start_link(_opts) do
    GenServer.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  def entrada(produto, qtd) when is_binary(produto) and is_integer(qtd) and qtd > 0 do
    GenServer.call(__MODULE__, {:operacao, :entrada, produto, qtd})
  end

  def saida(produto, qtd) when is_binary(produto) and is_integer(qtd) and qtd > 0 do
    GenServer.call(__MODULE__, {:operacao, :saida, produto, qtd})
  end

  def consultar(produto) when is_binary(produto) do
    GenServer.call(__MODULE__, {:consultar, produto})
  end

  # Provoca uma falha inesperada para fins de teste.
  def simular_falha do
    GenServer.cast(__MODULE__, :falhar)
  end

  @impl true
  def init(:ok) do
    # Recuperação: reaplica somente as operações de estoque do histórico via Stream.
    estoque =
      Estoque.Historico.stream(&(&1[:tipo] in [:entrada, :saida]))
      |> Enum.reduce(%{}, &aplicar/2)

    {:ok, estoque}
  end

  @impl true
  def handle_call({:operacao, tipo, produto, qtd}, _from, estoque) do
    atual = Map.get(estoque, produto, 0)

    if tipo == :saida and atual < qtd do
      {:reply, {:error, :estoque_insuficiente}, estoque}
    else
      op = %{tipo: tipo, produto: produto, quantidade: qtd}
      # Write-ahead: registra no histórico antes de alterar o estado.
      registro = Estoque.Historico.registrar(op)
      {:reply, {:ok, registro}, aplicar(op, estoque)}
    end
  end

  def handle_call({:consultar, produto}, _from, estoque) do
    {:reply, {:ok, Map.get(estoque, produto, 0)}, estoque}
  end

  @impl true
  def handle_cast(:falhar, _estoque) do
    raise "falha simulada no servidor de estoque"
  end

  defp aplicar(%{tipo: :entrada, produto: p, quantidade: q}, estoque) do
    Map.update(estoque, p, q, &(&1 + q))
  end

  defp aplicar(%{tipo: :saida, produto: p, quantidade: q}, estoque) do
    Map.update(estoque, p, -q, &(&1 - q))
  end
end

# Monitora o servidor: detecta encerramentos inesperados, registra falhas
# no histórico e se re-vincula ao novo processo após a recuperação.
defmodule Estoque.Monitor do
  use GenServer

  @intervalo_ms 50

  def start_link(_opts) do
    GenServer.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  @impl true
  def init(:ok) do
    send(self(), :vincular)
    {:ok, %{ref: nil, recuperando: false, tentativas: 0}}
  end

  @impl true
  def handle_info(:vincular, estado) do
    case Process.whereis(Estoque.Servidor) do
      nil ->
        # Servidor ainda não disponível: tenta novamente.
        Process.send_after(self(), :vincular, @intervalo_ms)
        {:noreply, %{estado | tentativas: estado.tentativas + 1}}

      pid ->
        ref = Process.monitor(pid)

        if estado.recuperando do
          Estoque.Historico.registrar(%{tipo: :recuperacao, tentativas: estado.tentativas + 1})
        end

        {:noreply, %{estado | ref: ref, recuperando: false, tentativas: 0}}
    end
  end

  # Encerramento inesperado detectado.
  def handle_info({:DOWN, ref, :process, _pid, motivo}, %{ref: ref} = estado) do
    Estoque.Historico.registrar(%{tipo: :falha, motivo: motivo})
    Process.send_after(self(), :vincular, @intervalo_ms)
    {:noreply, %{estado | ref: nil, recuperando: true}}
  end

  def handle_info(_msg, estado), do: {:noreply, estado}
end

# Supervisor: reinicia automaticamente os processos que falham.
# O Historico é iniciado primeiro e sobrevive às falhas do Servidor.
defmodule Estoque.Supervisor do
  use Supervisor

  def start_link(opts) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    children = [
      Estoque.Historico,
      Estoque.Servidor,
      Estoque.Monitor
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end

# Demonstração do funcionamento.
defmodule Estoque.Demo do
  def executar do
    {:ok, _sup} = Estoque.Supervisor.start_link([])

    {:ok, _} = Estoque.Servidor.entrada("camiseta", 10)
    {:ok, _} = Estoque.Servidor.saida("camiseta", 3)
    {:error, :estoque_insuficiente} = Estoque.Servidor.saida("camiseta", 100)

    IO.puts("Provocando falha no servidor...")
    Estoque.Servidor.simular_falha()
    Process.sleep(200)

    {:ok, saldo} = Estoque.Servidor.consultar("camiseta")
    IO.puts("Saldo após recuperação: #{saldo}")

    IO.puts("\nHistórico de eventos:")

    Estoque.Historico.stream()
    |> Stream.with_index(1)
    |> Stream.map(fn {reg, i} -> "#{i}. " <> formatar(reg) end)
    |> Enum.each(&IO.puts/1)

    IO.puts("\nFalhas registradas:")

    Estoque.Historico.stream(&(&1[:tipo] == :falha))
    |> Stream.map(&formatar/1)
    |> Enum.each(&IO.puts/1)
  end

  defp formatar(%{tipo: :falha, motivo: motivo}),
    do: "FALHA: #{inspect(motivo, limit: 3)}"

  defp formatar(%{tipo: :recuperacao, tentativas: n}),
    do: "RECUPERAÇÃO após #{n} tentativa(s)"

  defp formatar(%{tipo: tipo, produto: p, quantidade: q}),
    do: "#{tipo} #{p} x#{q}"
end

Estoque.Demo.executar()
