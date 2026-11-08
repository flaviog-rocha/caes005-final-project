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

# estoque.exs
# Gerenciamento de estoque tolerante a falhas.
# Requisitos: histórico de operações, detecção de encerramentos inesperados
# e recuperação de processos após falhas.
# Execução: elixir estoque.exs

defmodule Estoque.Dominio do
  # Regras de negócio puras (sem efeitos colaterais).
  # Como são puras, o estado pode ser reconstruído reaplicando o histórico.

  @type sku :: String.t()
  @type item :: %{nome: String.t(), quantidade: non_neg_integer()}
  @type estado :: %{optional(sku) => item}

  @spec estado_inicial() :: estado
  def estado_inicial, do: %{}

  # Aplica uma operação ao estado: retorna {:ok, novo_estado} ou {:error, motivo}.
  @spec aplicar(estado, term()) :: {:ok, estado} | {:error, atom()}
  def aplicar(estado, {:cadastro, sku, nome}) do
    if Map.has_key?(estado, sku) do
      {:error, :ja_cadastrado}
    else
      {:ok, Map.put(estado, sku, %{nome: nome, quantidade: 0})}
    end
  end

  def aplicar(estado, {:entrada, sku, qtd}) when is_integer(qtd) and qtd > 0 do
    atualizar(estado, sku, fn item -> {:ok, item.quantidade + qtd} end)
  end

  def aplicar(estado, {:saida, sku, qtd}) when is_integer(qtd) and qtd > 0 do
    atualizar(estado, sku, fn item ->
      if item.quantidade >= qtd,
        do: {:ok, item.quantidade - qtd},
        else: {:error, :estoque_insuficiente}
    end)
  end

  # Operação que provoca uma falha inesperada (usada para demonstrar recuperação)
  def aplicar(_estado, :falha_forcada), do: raise("falha simulada no processo de estoque")

  def aplicar(_estado, _op), do: {:error, :operacao_invalida}

  defp atualizar(estado, sku, fun) do
    case Map.fetch(estado, sku) do
      {:ok, item} ->
        case fun.(item) do
          {:ok, nova_qtd} -> {:ok, Map.put(estado, sku, %{item | quantidade: nova_qtd})}
          {:error, _} = erro -> erro
        end

      :error ->
        {:error, :nao_cadastrado}
    end
  end
end

defmodule Estoque.Processo do
  # Processo que mantém o estado vivo. Não é linkado ao gerenciador:
  # quem observa seu encerramento é o gerenciador, via monitor.
  use GenServer

  @spec start(Estoque.Dominio.estado()) :: GenServer.on_start()
  def start(estado), do: GenServer.start(__MODULE__, estado)

  @impl true
  def init(estado), do: {:ok, estado}

  @impl true
  def handle_call({:aplicar, op}, _from, estado) do
    case Estoque.Dominio.aplicar(estado, op) do
      {:ok, novo} -> {:reply, :ok, novo}
      {:error, _} = erro -> {:reply, erro, estado}
    end
  end

  def handle_call({:consultar, sku}, _from, estado) do
    resposta =
      case Map.fetch(estado, sku) do
        {:ok, item} -> {:ok, item}
        :error -> {:error, :nao_cadastrado}
      end

    {:reply, resposta, estado}
  end
end

defmodule Estoque.Gerenciador do
  # Gerenciador: mantém o histórico de operações (fonte da verdade),
  # monitora o processo de estoque e o recria a partir do histórico
  # quando ele encerra de forma inesperada.
  use GenServer

  alias Estoque.Dominio

  defstruct historico: [], eventos: [], seq: 0, worker: nil, ref: nil

  # ---------------------------------------------------------------------------
  # API pública
  # ---------------------------------------------------------------------------

  def start_link(_opts \\ []) do
    GenServer.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  @doc "Executa uma operação de estoque e registra o resultado no histórico."
  def executar(op), do: GenServer.call(__MODULE__, {:executar, op})

  @doc "Consulta um item pelo SKU."
  def consultar(sku), do: GenServer.call(__MODULE__, {:consultar, sku})

  @doc "Retorna um Stream das entradas do histórico, opcionalmente filtrado."
  def historico(filtro \\ fn _ -> true end) do
    __MODULE__
    |> GenServer.call(:historico)
    |> Stream.filter(filtro)
  end

  @doc "Retorna um Stream dos eventos de falha/recuperação."
  def eventos do
    __MODULE__
    |> GenServer.call(:eventos)
    |> Stream.map(& &1)
  end

  @doc "PID atual do processo de estoque (útil para testes de falha)."
  def processo, do: GenServer.call(__MODULE__, :processo)

  # ---------------------------------------------------------------------------
  # Callbacks
  # ---------------------------------------------------------------------------

  @impl true
  def init(:ok), do: {:ok, reiniciar(%__MODULE__{}, :inicializacao)}

  @impl true
  def handle_call({:executar, op}, _from, s) do
    case chamar_worker(s.worker, {:aplicar, op}) do
      {:exit, motivo} ->
        s =
          s
          |> reiniciar({:encerramento_durante_operacao, motivo})
          |> registrar(op, :falha, motivo)

        {:reply, {:error, :processo_encerrado_recuperado}, s}

      :ok ->
        {:reply, :ok, registrar(s, op, :ok, :ok)}

      {:error, _} = erro ->
        {:reply, erro, registrar(s, op, :erro, erro)}
    end
  end

  def handle_call({:consultar, sku}, _from, s) do
    case chamar_worker(s.worker, {:consultar, sku}) do
      {:exit, motivo} ->
        {:reply, {:error, :processo_encerrado_recuperado}, reiniciar(s, {:encerramento_durante_consulta, motivo})}

      resposta ->
        {:reply, resposta, s}
    end
  end

  def handle_call(:historico, _from, s), do: {:reply, Enum.reverse(s.historico), s}
  def handle_call(:eventos, _from, s), do: {:reply, Enum.reverse(s.eventos), s}
  def handle_call(:processo, _from, s), do: {:reply, s.worker, s}

  # Detecção de encerramento inesperado via monitor
  @impl true
  def handle_info({:DOWN, ref, :process, pid, motivo}, %{ref: ref} = s) do
    IO.puts("[monitor] processo #{inspect(pid)} encerrou inesperadamente: #{inspect(motivo)}")
    {:noreply, reiniciar(s, {:encerramento_inesperado, motivo})}
  end

  # DOWN de monitores antigos (já substituídos) são ignorados
  def handle_info(_msg, s), do: {:noreply, s}

  # ---------------------------------------------------------------------------
  # Funções privadas
  # ---------------------------------------------------------------------------

  # Chamada ao worker que converte a saída abrupta em um valor tratável
  defp chamar_worker(nil, _msg), do: {:exit, :sem_processo}

  defp chamar_worker(pid, msg) do
    GenServer.call(pid, msg)
  catch
    :exit, motivo -> {:exit, motivo}
  end

  # Adiciona uma entrada ao histórico (com sequência e timestamp)
  defp registrar(s, op, status, detalhe) do
    seq = s.seq + 1

    entrada = %{
      seq: seq,
      em: DateTime.utc_now(),
      op: op,
      status: status,
      detalhe: detalhe
    }

    %{s | seq: seq, historico: [entrada | s.historico]}
  end

  # Recria o processo de estoque a partir do histórico
  defp reiniciar(s, motivo) do
    if s.ref, do: Process.demonitor(s.ref, [:flush])
    # Garante o término de um processo possivelmente travado (no-op se já morreu)
    if s.worker, do: Process.exit(s.worker, :kill)

    estado = reconstruir(s.historico)
    {:ok, pid} = Estoque.Processo.start(estado)
    ref = Process.monitor(pid)

    evento = %{
      em: DateTime.utc_now(),
      motivo: motivo,
      itens_recuperados: map_size(estado)
    }

    IO.puts("[recuperacao] novo processo #{inspect(pid)} com #{map_size(estado)} item(ns)")

    %{s | worker: pid, ref: ref, eventos: [evento | s.eventos]}
  end

  # Reconstrói o estado reaplicando apenas as operações bem-sucedidas, em ordem.
  defp reconstruir(historico_reverso) do
    historico_reverso
    |> Enum.reverse()
    |> Stream.filter(&(&1.status == :ok))
    |> Stream.map(& &1.op)
    |> Stream.scan(Dominio.estado_inicial(), &avancar/2)
    |> Enum.reduce(Dominio.estado_inicial(), fn estado, _acc -> estado end)
  end

  defp avancar(op, estado) do
    case Dominio.aplicar(estado, op) do
      {:ok, novo} -> novo
      _ -> estado
    end
  end
end

defmodule Estoque.Demo do
  # Demonstração: operações normais, erros de negócio, falha forçada,
  # encerramento externo do processo e consulta do histórico.
  alias Estoque.Gerenciador, as: G

  def executar do
    {:ok, _} = G.start_link()

    ops = [
      {:cadastro, "CAM-01", "Camiseta"},
      {:entrada, "CAM-01", 10},
      {:saida, "CAM-01", 3},
      {:saida, "CAM-01", 50},
      {:entrada, "CAM-99", 1},
      :falha_forcada,
      {:entrada, "CAM-01", 5}
    ]

    IO.puts("== Operações ==")

    ops
    |> Stream.map(fn op -> {op, G.executar(op)} end)
    |> Stream.map(fn {op, resultado} ->
      IO.puts("#{inspect(op)} => #{inspect(resultado)}")
    end)
    |> Stream.run()

    IO.puts("\n== Encerramento externo do processo ==")
    Process.exit(G.processo(), :kill)
    # Dá tempo para o monitor detectar e recuperar o processo
    Process.sleep(50)

    IO.puts("\n== Consulta após recuperação ==")
    IO.puts("CAM-01 => #{inspect(G.consultar("CAM-01"))}")

    IO.puts("\n== Histórico de falhas/erros ==")

    G.historico(&(&1.status != :ok))
    |> Stream.map(fn e -> "##{e.seq} #{inspect(e.op)} [#{e.status}] #{inspect(e.detalhe)}" end)
    |> Stream.map(&IO.puts/1)
    |> Stream.run()

    IO.puts("\n== Eventos de recuperação ==")

    G.eventos()
    |> Stream.map(fn ev -> "#{inspect(ev.motivo)} (itens recuperados: #{ev.itens_recuperados})" end)
    |> Stream.map(&IO.puts/1)
    |> Stream.run()

    IO.puts("\nTotal de entradas no histórico: #{Enum.count(G.historico())}")
  end
end

Estoque.Demo.executar()
