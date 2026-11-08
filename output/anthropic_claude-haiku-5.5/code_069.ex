# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de entregas e rastreamento, atendendo somente aos seguintes requisitos:
#
# 1. Gerar notificações quando determinadas condições forem atingidas.
# 2. Processar grandes volumes de dados sem carregá-los integralmente na memória.
# 3. Recuperar processos após falhas inesperadas.
# 4. Implementar novas tentativas após falhas temporárias.
# 5. Detectar encerramentos inesperados de processos.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - Agent
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# Gerenciamento de entregas e rastreamento (arquivo único autocontido)
# Execute com: elixir entregas.exs
#
# Requisitos atendidos:
# 1. Notificações: regras avaliadas a cada evento (condições) geram notificações.
# 2. Grande volume: arquivo lido via Stream em lotes, sem carregar tudo em memória.
# 3. Recuperação: Agent supervisionado (:permanent) reinicia e recarrega o checkpoint.
# 4. Novas tentativas: Retry com backoff exponencial e jitter para falhas temporárias.
# 5. Detecção de encerramentos inesperados: Process.spawn_monitor + tratamento de :DOWN.

defmodule Entregas do
  @moduledoc """
  Ponto de entrada da demonstração do sistema de entregas e rastreamento.
  """

  def executar do
    tmp = System.tmp_dir!()
    caminho = Path.join(tmp, "entregas_#{System.unique_integer([:positive])}.csv")
    checkpoint = Path.join(tmp, "entregas_checkpoint.bin")

    # Regras de notificação: funções que avaliam cada evento de entrega.
    regras = [
      {"entrega_atrasada", &(&1.status == "atrasada")},
      {"entrega_extraviada", &(&1.status == "extraviada")}
    ]

    # Supervisor que mantém o Agent de estado vivo após falhas.
    {:ok, sup} =
      Supervisor.start_link(
        [{Entregas.Estado, [checkpoint: checkpoint, regras: regras]}],
        strategy: :one_for_one
      )

    # Assinante das notificações enviadas em tempo real.
    Entregas.Estado.assinar(self())

    # Gera um arquivo grande sem mantê-lo em memória.
    gerar_csv(caminho, 50_000)

    resumo = Entregas.Processador.processar(caminho, lote: 1_000)
    IO.inspect(resumo, label: "lotes processados")
    IO.inspect(Entregas.Estado.contadores(), label: "contadores por status")

    # Simula falha inesperada do Agent e verifica a recuperação via checkpoint.
    pid_antigo = Process.whereis(Entregas.Estado)
    Process.exit(pid_antigo, :kill)
    aguardar_reinicio(pid_antigo)

    # O novo Agent recarrega o estado do checkpoint (assinantes são perdidos e
    # devem ser reassinados pelo cliente, como indicado em Entregas.Estado).
    IO.inspect(Entregas.Estado.progresso(), label: "progresso após recuperação")
    IO.inspect(Entregas.Estado.contadores(), label: "contadores após recuperação")

    # Demonstra supervisão de um processo que pode encerrar de forma inesperada.
    resultado_vigia =
      Entregas.Vigia.supervisionar(
        fn ->
          if :rand.uniform() < 0.5, do: exit(:falha_inesperada), else: :concluido
        end,
        max_reinicios: 5
      )

    IO.inspect(resultado_vigia, label: "resultado do vigia")
    IO.puts("notificações recebidas em tempo real: #{drenar(0)}")
    IO.inspect(Enum.take(Entregas.Estado.notificacoes(), 3), label: "últimas notificações")

    Supervisor.stop(sup)
    File.rm(caminho)
    File.rm(checkpoint)
    :ok
  end

  defp gerar_csv(caminho, n) do
    statuses = ~w(em_transito entregue atrasada extraviada)
    cabecalho = ["id,status,atualizado_em\n"]

    linhas =
      Stream.map(1..n, fn i ->
        "#{i},#{Enum.random(statuses)},#{DateTime.to_iso8601(DateTime.utc_now())}\n"
      end)

    cabecalho
    |> Stream.concat(linhas)
    |> Stream.into(File.stream!(caminho))
    |> Stream.run()
  end

  defp aguardar_reinicio(pid_antigo) do
    case Process.whereis(Entregas.Estado) do
      nil ->
        Process.sleep(10)
        aguardar_reinicio(pid_antigo)

      ^pid_antigo ->
        Process.sleep(10)
        aguardar_reinicio(pid_antigo)

      _novo ->
        :ok
    end
  end

  defp drenar(acc) do
    receive do
      {:entregas, _notificacao} -> drenar(acc + 1)
    after
      0 -> acc
    end
  end
end

defmodule Entregas.Estado do
  @moduledoc """
  Agent que mantém o estado agregado do rastreamento (contadores, progresso e
  notificações). Cada transição é persistida em checkpoint, permitindo que o
  supervisor reinicie o processo e recupere o estado.

  Observação: a lista de assinantes não é persistida; após uma recuperação,
  os clientes devem chamar `assinar/1` novamente.
  """

  @max_notificacoes 500

  def child_spec(opts) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [opts]},
      restart: :permanent,
      type: :worker
    }
  end

  def start_link(opts) do
    Agent.start_link(fn -> inicializar(opts) end, name: __MODULE__)
  end

  def assinar(pid) do
    Agent.update(__MODULE__, &%{&1 | assinantes: [pid | &1.assinantes]})
  end

  def progresso, do: Agent.get(__MODULE__, & &1.progresso)
  def contadores, do: Agent.get(__MODULE__, & &1.contadores)
  def notificacoes, do: Agent.get(__MODULE__, & &1.notificacoes)

  # Aplica um lote de eventos de forma idempotente: se o lote já foi aplicado
  # (ex.: reexecução após falha), nada é alterado.
  def aplicar_lote(eventos, inicio, linhas) do
    Agent.get_and_update(__MODULE__, fn estado ->
      fim = inicio + linhas

      cond do
        estado.progresso >= fim ->
          {:ok, estado}

        estado.progresso != inicio ->
          {{:error, :fora_de_ordem}, estado}

        true ->
          novo =
            eventos
            |> Enum.reduce(estado, &aplicar_evento/2)
            |> Map.put(:progresso, fim)
            |> persistir()

          {:ok, novo}
      end
    end)
  end

  # Notificação genérica (ex.: encerramentos inesperados).
  def notificar(tipo, dados) do
    Agent.update(__MODULE__, fn estado ->
      estado
      |> notificar_em(tipo, dados)
      |> persistir()
    end)
  end

  defp inicializar(opts) do
    checkpoint = Keyword.fetch!(opts, :checkpoint)
    snapshot = carregar(checkpoint)

    %{
      checkpoint: checkpoint,
      regras: Keyword.get(opts, :regras, []),
      assinantes: [],
      progresso: Map.get(snapshot, :progresso, 0),
      contadores: Map.get(snapshot, :contadores, %{}),
      notificacoes: Map.get(snapshot, :notificacoes, [])
    }
  end

  defp carregar(caminho) do
    case File.read(caminho) do
      {:ok, binario} -> :erlang.binary_to_term(binario)
      {:error, _} -> %{}
    end
  end

  # Escrita atômica: grava em arquivo temporário e renomeia.
  defp persistir(estado) do
    snapshot = Map.take(estado, [:progresso, :contadores, :notificacoes])
    tmp = estado.checkpoint <> ".tmp"
    File.write!(tmp, :erlang.term_to_binary(snapshot))
    File.rename!(tmp, estado.checkpoint)
    estado
  end

  defp aplicar_evento(evento, estado) do
    contadores = Map.update(estado.contadores, evento.status, 1, &(&1 + 1))
    estado = %{estado | contadores: contadores}

    Enum.reduce(estado.regras, estado, fn {nome, condicao}, acc ->
      if condicao.(evento) do
        notificar_em(acc, :regra, %{regra: nome, entrega: evento.id, status: evento.status})
      else
        acc
      end
    end)
  end

  defp notificar_em(estado, tipo, dados) do
    notificacao = Map.merge(%{tipo: tipo, em: DateTime.utc_now()}, Map.new(dados))
    Enum.each(estado.assinantes, &send(&1, {:entregas, notificacao}))

    %{
      estado
      | notificacoes: Enum.take([notificacao | estado.notificacoes], @max_notificacoes)
    }
  end
end

defmodule Entregas.Processador do
  @moduledoc """
  Processa arquivos grandes de forma preguiçosa (Stream), em lotes, retomando
  a partir do último progresso confirmado.
  """

  def processar(caminho, opts \\ []) do
    tamanho = Keyword.get(opts, :lote, 1_000)
    ja_processadas = Entregas.Estado.progresso()

    caminho
    |> File.stream!()
    # Pula o cabeçalho e as linhas já confirmadas em execuções anteriores.
    |> Stream.drop(1 + ja_processadas)
    |> Stream.chunk_every(tamanho)
    |> Stream.with_index()
    |> Enum.reduce(%{ok: 0, falhas: 0}, fn {lote, i}, acc ->
      inicio = ja_processadas + i * tamanho

      case processar_lote(lote, inicio) do
        :ok -> Map.update!(acc, :ok, &(&1 + 1))
        {:error, _motivo} -> Map.update!(acc, :falhas, &(&1 + 1))
      end
    end)
  end

  defp processar_lote(lote, inicio) do
    eventos = lote |> Enum.map(&parse_linha/1) |> Enum.reject(&is_nil/1)
    linhas = length(lote)

    # Cada lote roda em um processo monitorado; falhas inesperadas são
    # detectadas e o lote é reprocessado (aplicar_lote é idempotente).
    Entregas.Vigia.supervisionar(fn ->
      Entregas.Retry.executar(fn ->
        with :ok <- Entregas.Integracao.enviar(eventos) do
          Entregas.Estado.aplicar_lote(eventos, inicio, linhas)
        end
      end)
    end)
  end

  defp parse_linha(linha) do
    case linha |> String.trim() |> String.split(",") do
      [id, status, ts] -> %{id: id, status: status, atualizado_em: ts}
      _ -> nil
    end
  end
end

defmodule Entregas.Retry do
  @moduledoc """
  Repete a operação em caso de erro temporário ({:error, :temporario}),
  com backoff exponencial e jitter aleatório.
  """

  def executar(fun, opts \\ []) do
    max = Keyword.get(opts, :max, 5)
    base = Keyword.get(opts, :base_ms, 20)
    tentar(fun, 1, max, base)
  end

  defp tentar(fun, n, max, base) do
    case fun.() do
      {:error, :temporario} when n < max ->
        espera = base * Integer.pow(2, n - 1) + :rand.uniform(base)
        Process.sleep(espera)
        tentar(fun, n + 1, max, base)

      {:error, :temporario} ->
        {:error, :esgotado}

      outro ->
        outro
    end
  end
end

defmodule Entregas.Vigia do
  @moduledoc """
  Executa uma função em um processo monitorado. Se o processo encerrar de forma
  inesperada, a ocorrência é notificada e a função é reiniciada até o limite.
  """

  def supervisionar(fun, opts \\ []) do
    max = Keyword.get(opts, :max_reinicios, 3)
    do_supervisionar(fun, 0, max)
  end

  defp do_supervisionar(fun, reinicios, max) do
    parent = self()

    {pid, ref} =
      spawn_monitor(fn ->
        send(parent, {:resultado, self(), fun.()})
      end)

    receive do
      {:resultado, ^pid, valor} ->
        Process.demonitor(ref, [:flush])
        valor

      {:DOWN, ^ref, :process, ^pid, razao} ->
        Entregas.Estado.notificar(:encerramento_inesperado, %{
          razao: razao,
          reinicios: reinicios
        })

        if reinicios < max do
          do_supervisionar(fun, reinicios + 1, max)
        else
          {:error, {:excedeu_reinicios, razao}}
        end
    end
  end
end

defmodule Entregas.Integracao do
  @moduledoc """
  Simula uma integração externa (ex.: transportadora) com falhas temporárias.
  """

  def enviar(_eventos) do
    if :rand.uniform() < 0.2, do: {:error, :temporario}, else: :ok
  end
end

Entregas.executar()
