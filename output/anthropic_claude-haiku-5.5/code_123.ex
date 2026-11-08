# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de votação online, atendendo somente aos seguintes requisitos:
#
# 1. Gerar notificações quando determinadas condições forem atingidas.
# 2. Permitir limitar a quantidade de operações simultâneas.
# 3. Garantir a consistência dos dados durante operações concorrentes.
# 4. Implementar novas tentativas após falhas temporárias.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - Task
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# sistema_votacao.exs
# Execute com: elixir sistema_votacao.exs

defmodule Votacao do
  @moduledoc """
  Sistema de votação online.

  - Notificações: assinantes recebem mensagens ao atingir a meta de 50% dos votos
    e ao encerramento da enquete.
  - Limite de concorrência: `processar_lote/2` usa `Task.async_stream/3` com `max_concurrency`.
  - Consistência: todo o estado é mantido em um único GenServer, que serializa as
    escritas. Assim, cada eleitor é contado no máximo uma vez e o limite de votos
    nunca é ultrapassado, mesmo com muitas tarefas concorrentes.
  - Novas tentativas: falhas temporárias (`{:error, :timeout}`) são repetidas com
    backoff exponencial.
  """

  use GenServer

  # =====================================================================
  # API do servidor (estado centralizado e consistente)
  # =====================================================================

  @doc "Inicia o servidor de votação."
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, :ok, Keyword.take(opts, [:name]) ++ [name: Keyword.get(opts, :name, __MODULE__)])
  end

  @doc "Cria uma enquete com opções e limite de votos."
  def criar_enquete(servidor \\ __MODULE__, id, opcoes, limite) do
    GenServer.call(servidor, {:criar, id, opcoes, limite})
  end

  @doc "Assina notificações de uma enquete. O pid recebe mensagens `{:votacao, evento, id, payload}`."
  def assinar(servidor \\ __MODULE__, id, pid \\ self()) do
    GenServer.call(servidor, {:assinar, id, pid})
  end

  @doc "Consulta o resultado atual de uma enquete."
  def resultado(servidor \\ __MODULE__, id) do
    GenServer.call(servidor, {:resultado, id})
  end

  # =====================================================================
  # Callbacks do GenServer
  # =====================================================================

  @impl true
  def init(:ok), do: {:ok, %{}}

  @impl true
  def handle_call({:criar, id, opcoes, limite}, _from, estado) do
    if Map.has_key?(estado, id) do
      {:reply, {:error, :ja_existe}, estado}
    else
      enquete = %{
        opcoes: Map.new(opcoes, &{&1, 0}),
        eleitores: MapSet.new(),
        limite: limite,
        status: :aberta,
        assinantes: [],
        meta_notificada: false
      }

      {:reply, :ok, Map.put(estado, id, enquete)}
    end
  end

  def handle_call({:assinar, id, pid}, _from, estado) do
    case buscar(estado, id) do
      {:ok, e} ->
        {:reply, :ok, Map.put(estado, id, %{e | assinantes: [pid | e.assinantes]})}

      {:error, motivo} ->
        {:reply, {:error, motivo}, estado}
    end
  end

  def handle_call({:resultado, id}, _from, estado) do
    case buscar(estado, id) do
      {:ok, e} -> {:reply, {:ok, e.opcoes, e.status}, estado}
      {:error, motivo} -> {:reply, {:error, motivo}, estado}
    end
  end

  def handle_call({:votar, id, eleitor, opcao}, _from, estado) do
    with {:ok, e} <- buscar(estado, id),
         :ok <- validar_aberta(e),
         :ok <- validar_opcao(e, opcao),
         :ok <- validar_eleitor(e, eleitor) do
      e = e |> registrar(eleitor, opcao) |> verificar(id)
      {:reply, {:ok, MapSet.size(e.eleitores)}, Map.put(estado, id, e)}
    else
      {:error, motivo} -> {:reply, {:error, motivo}, estado}
    end
  end

  # =====================================================================
  # Regras de negócio (funções puras, exceto o envio de notificações)
  # =====================================================================

  defp buscar(estado, id) do
    case Map.fetch(estado, id) do
      {:ok, e} -> {:ok, e}
      :error -> {:error, :nao_encontrada}
    end
  end

  defp validar_aberta(%{status: :aberta}), do: :ok
  defp validar_aberta(_), do: {:error, :encerrada}

  defp validar_opcao(e, opcao) do
    if Map.has_key?(e.opcoes, opcao), do: :ok, else: {:error, :opcao_invalida}
  end

  defp validar_eleitor(e, eleitor) do
    if MapSet.member?(e.eleitores, eleitor), do: {:error, :ja_votou}, else: :ok
  end

  defp registrar(e, eleitor, opcao) do
    %{
      e
      | opcoes: Map.update!(e.opcoes, opcao, &(&1 + 1)),
        eleitores: MapSet.put(e.eleitores, eleitor)
    }
  end

  # Verifica condições de notificação após cada voto
  defp verificar(e, id) do
    total = MapSet.size(e.eleitores)

    # Condição 1: meta parcial de 50% atingida (notifica uma única vez)
    e =
      if not e.meta_notificada and total * 2 >= e.limite do
        notificar(e, id, :meta_atingida, %{total: total, limite: e.limite})
        %{e | meta_notificada: true}
      else
        e
      end

    # Condição 2: limite de votos atingido (encerra a enquete)
    if total >= e.limite do
      notificar(e, id, :encerrada, e.opcoes)
      %{e | status: :encerrada}
    else
      e
    end
  end

  defp notificar(e, id, evento, payload) do
    Enum.each(e.assinantes, &send(&1, {:votacao, evento, id, payload}))
  end

  # =====================================================================
  # Processamento concorrente com limite de operações e novas tentativas
  # =====================================================================

  @doc """
  Processa uma lista de votos `{id, eleitor, opcao}` concorrentemente.

  Opções:
    * `:servidor` - nome/pid do servidor (padrão: `Votacao`)
    * `:max_concorrencia` - número máximo de tarefas simultâneas
    * `:tentativas` - número máximo de tentativas por voto
    * `:falha_simulada` - probabilidade (0.0 a 1.0) de falha temporária simulada da rede
  """
  def processar_lote(votos, opts \\ []) do
    servidor = Keyword.get(opts, :servidor, __MODULE__)
    max = Keyword.get(opts, :max_concorrencia, System.schedulers_online())
    tentativas = Keyword.get(opts, :tentativas, 3)
    falha = Keyword.get(opts, :falha_simulada, 0.0)

    votos
    |> Task.async_stream(
      fn {id, eleitor, opcao} = voto ->
        resultado =
          com_retry(
            fn -> chamada_com_falha(servidor, {:votar, id, eleitor, opcao}, falha) end,
            tentativas,
            50
          )

        {voto, resultado}
      end,
      max_concurrency: max,
      timeout: :infinity,
      ordered: true
    )
    |> Enum.map(fn {:ok, resultado} -> resultado end)
  end

  # Repete a função apenas em falhas temporárias, com backoff exponencial
  defp com_retry(fun, 1, _atraso), do: fun.()

  defp com_retry(fun, tentativas, atraso) do
    case fun.() do
      {:error, :timeout} ->
        Process.sleep(atraso)
        com_retry(fun, tentativas - 1, atraso * 2)

      outro ->
        outro
    end
  end

  # Simula instabilidade de rede e converte timeouts do GenServer em erro temporário
  defp chamada_com_falha(servidor, msg, probabilidade) do
    if :rand.uniform() < probabilidade do
      {:error, :timeout}
    else
      try do
        GenServer.call(servidor, msg, 2_000)
      catch
        :exit, _ -> {:error, :timeout}
      end
    end
  end

  # =====================================================================
  # Demonstração
  # =====================================================================

  def demo do
    {:ok, _pid} = start_link()
    :ok = criar_enquete("eleicao", ["A", "B", "C"], 20)
    :ok = assinar("eleicao")

    # 30 votos de 25 eleitores: há duplicidades e votos excedentes ao limite
    votos =
      for i <- 1..30 do
        {"eleicao", "eleitor#{rem(i - 1, 25) + 1}", Enum.random(["A", "B", "C"])}
      end

    resultados =
      processar_lote(votos,
        max_concorrencia: 5,
        tentativas: 5,
        falha_simulada: 0.3
      )

    IO.puts("\n== Resumo dos votos ==")
    resultados
    |> Enum.map(fn {_voto, r} -> r end)
    |> Enum.frequencies_by(fn
      {:ok, _total} -> :ok
      {:error, motivo} -> motivo
    end)
    |> IO.inspect()

    IO.puts("\n== Notificações recebidas ==")
    coletar = fn f, acc ->
      receive do
        {:votacao, evento, id, payload} -> f.(f, [{evento, id, payload} | acc])
      after
        100 -> Enum.reverse(acc)
      end
    end

    coletar.(coletar, []) |> Enum.each(&IO.inspect/1)

    IO.puts("\n== Resultado final ==")
    {:ok, opcoes, status} = resultado("eleicao")
    IO.inspect({opcoes, status})
  end
end

Votacao.demo()
