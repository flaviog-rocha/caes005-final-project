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

# Arquivo único autocontido: gerenciamento de entregas e rastreamento.
# Requisitos atendidos:
#   1. Notificações geradas por regras (condições) sobre as entregas.
#   2. Processamento em streaming (File.stream!/Stream), em lotes, sem carregar o arquivo inteiro.
#   3. Recuperação após falhas: snapshot em disco + reinício pelo Supervisor.
#   4. Novas tentativas com backoff exponencial para falhas temporárias.
#   5. Detecção de encerramentos inesperados via Process.monitor/2 e processo vigia.
# Recurso principal de estado: Agent.
# Para executar: Entregas.Demo.executar()

defmodule Entregas.Estado do
  @moduledoc """
  Agent que mantém o estado das entregas e persiste um snapshot em disco
  a cada alteração. Ao ser reiniciado pelo supervisor, recarrega o snapshot.
  """
  use Agent

  @nome __MODULE__

  def start_link(opts) do
    caminho = Keyword.fetch!(opts, :snapshot)
    Agent.start_link(fn -> carregar(caminho) end, name: @nome)
  end

  @doc "Registra (ou atualiza) um evento de rastreamento de uma entrega."
  def registrar_evento(id, status, prazo) do
    Agent.get_and_update(@nome, fn estado ->
      entrega = %{id: id, status: status, prazo: prazo}
      estado = persistir(%{estado | entregas: Map.put(estado.entregas, id, entrega)})
      {entrega, estado}
    end)
  end

  @doc "Quantidade de entregas conhecidas."
  def total, do: Agent.get(@nome, &map_size(&1.entregas))

  # Recuperação: carrega o último snapshot válido, se existir.
  defp carregar(caminho) do
    File.mkdir_p!(Path.dirname(caminho))

    entregas =
      case File.read(caminho) do
        {:ok, bin} -> binario_seguro(bin)
        {:error, _} -> %{}
      end

    %{caminho: caminho, entregas: entregas}
  end

  defp binario_seguro(bin) do
    :erlang.binary_to_term(bin, [:safe])
  rescue
    ArgumentError -> %{}
  end

  # Escrita atômica: grava em arquivo temporário e renomeia.
  defp persistir(%{caminho: caminho, entregas: entregas} = estado) do
    tmp = caminho <> ".tmp"
    File.write!(tmp, :erlang.term_to_binary(entregas))
    File.rename!(tmp, caminho)
    estado
  end
end

defmodule Entregas.Notificacoes do
  @moduledoc """
  Agent que avalia regras sobre as entregas e gera notificações quando
  as condições são atingidas. Mantém contadores e as notificações recentes.
  """
  use Agent

  @nome __MODULE__
  @limite_recentes 100

  def start_link(opts) do
    callback = Keyword.get(opts, :callback, &IO.inspect/1)
    regras = Keyword.get(opts, :regras, regras_padrao())

    Agent.start_link(
      fn -> %{callback: callback, regras: regras, recentes: [], total: 0} end,
      name: @nome
    )
  end

  # Cada regra é {nome, fn entrega, agora -> boolean end}.
  defp regras_padrao do
    [
      atrasada:
        fn %{status: s, prazo: p}, agora ->
          s in [:em_transito, :saiu_para_entrega] and agora > p
        end,
      entregue: fn %{status: s}, _agora -> s == :entregue end,
      extraviada: fn %{status: s}, _agora -> s == :extraviada end
    ]
  end

  @doc "Avalia todas as regras sobre a entrega e emite as notificações disparadas."
  def avaliar(entrega) do
    agora = System.system_time(:second)

    {callback, novas} =
      Agent.get_and_update(@nome, fn s ->
        novas =
          for {nome, regra} <- s.regras,
              regra.(entrega, agora),
              do: %{tipo: nome, entrega: entrega.id, em: agora}

        {{s.callback, novas}, acrescentar(s, novas)}
      end)

    # Emissão fora do Agent para não bloquear o estado.
    Enum.each(novas, callback)
  end

  @doc "Registra uma notificação de sistema (ex.: encerramento inesperado)."
  def registrar(notificacao) do
    callback =
      Agent.get_and_update(@nome, fn s ->
        {s.callback, acrescentar(s, [notificacao])}
      end)

    callback.(notificacao)
  end

  def total, do: Agent.get(@nome, & &1.total)

  def recentes, do: Agent.get(@nome, & &1.recentes)

  defp acrescentar(s, novas) do
    %{
      s
      | recentes: Enum.take(novas ++ s.recentes, @limite_recentes),
        total: s.total + length(novas)
    }
  end
end

defmodule Entregas.Retry do
  @moduledoc """
  Execução com novas tentativas e backoff exponencial para falhas temporárias.
  A função deve retornar:
    * {:ok, valor}
    * {:error, :temporario, motivo}  -> será reexecutada
    * {:error, motivo}               -> falha permanente, sem novas tentativas
  Exceções e saídas (exits) são tratadas como falhas temporárias.
  """

  def executar(fun, opts \\ []) do
    tentativas = Keyword.get(opts, :tentativas, 3)
    backoff = Keyword.get(opts, :backoff_ms, 50)
    do_executar(fun, 1, tentativas, backoff)
  end

  defp do_executar(fun, n, max, backoff) do
    case chamar(fun) do
      {:ok, valor} ->
        {:ok, valor}

      {:error, :temporario, _motivo} when n < max ->
        Process.sleep(backoff * Integer.pow(2, n - 1))
        do_executar(fun, n + 1, max, backoff)

      {:error, :temporario, motivo} ->
        {:error, {:esgotado, motivo}}

      {:error, motivo} ->
        {:error, motivo}
    end
  end

  defp chamar(fun) do
    fun.()
  rescue
    e -> {:error, :temporario, Exception.message(e)}
  catch
    :exit, motivo -> {:error, :temporario, {:exit, motivo}}
  end
end

defmodule Entregas.Vigia do
  @moduledoc """
  Processo de monitoramento. Usa Process.monitor/2 sobre os processos
  registrados (ex.: Agents do sistema) e gera notificação quando um deles
  termina de forma inesperada. Após o reinício pelo supervisor, rearma o
  monitor sobre o novo PID.
  """

  @nome __MODULE__
  @rearme_ms 50

  @doc "Inicia o vigia monitorando os nomes registrados informados."
  def iniciar(processos) do
    pid = spawn(fn -> loop(%{}) end)
    Process.register(pid, @nome)
    Enum.each(processos, &send(pid, {:monitorar, &1}))
    {:ok, pid}
  end

  defp loop(monitores) do
    receive do
      {:monitorar, nome} ->
        loop(armar(nome, monitores))

      {:DOWN, ref, :process, _pid, motivo} ->
        case Map.pop(monitores, ref) do
          {nil, restantes} ->
            loop(restantes)

          {nome, restantes} ->
            if motivo not in [:normal, :shutdown] do
              notificar_encerramento(nome, motivo)
            end

            # Aguarda o supervisor reiniciar o processo e rearma o monitor.
            Process.send_after(self(), {:monitorar, nome}, @rearme_ms)
            loop(restantes)
        end
    end
  end

  defp armar(nome, monitores) do
    case Process.whereis(nome) do
      nil ->
        Process.send_after(self(), {:monitorar, nome}, @rearme_ms)
        monitores

      pid ->
        ref = Process.monitor(pid)
        Map.put(monitores, ref, nome)
    end
  end

  defp notificar_encerramento(nome, motivo) do
    notificacao = %{
      tipo: :encerramento_inesperado,
      processo: nome,
      motivo: motivo,
      em: System.system_time(:second)
    }

    Entregas.Retry.executar(fn -> {:ok, Entregas.Notificacoes.registrar(notificacao)} end)
  end
end

defmodule Entregas.Rastreamento do
  @moduledoc """
  Processa grandes arquivos de eventos (formato "id,status,prazo") em streaming
  e em lotes. Cada evento é gravado com novas tentativas em caso de falha temporária.
  """
  alias Entregas.{Estado, Notificacoes, Retry}

  @tamanho_lote 500
  @status ~w(postado em_transito saiu_para_entrega entregue extraviada)a

  @doc "Processa o arquivo linha a linha, sem carregá-lo integralmente na memória."
  def processar_arquivo(caminho) do
    caminho
    |> File.stream!()
    |> Stream.with_index(1)
    |> Stream.map(&parse/1)
    |> Stream.chunk_every(@tamanho_lote)
    |> Enum.reduce(%{ok: 0, invalidos: 0, falhas: 0}, &processar_lote/2)
  end

  defp parse({linha, n}) do
    with [id, status_txt, prazo_txt] <- String.split(String.trim(linha), ","),
         {prazo, ""} <- Integer.parse(prazo_txt),
         status when status in @status <- Enum.find(@status, &(Atom.to_string(&1) == status_txt)) do
      {:ok, id, status, prazo}
    else
      _ -> {:invalido, n}
    end
  end

  defp processar_lote(lote, acc) do
    Enum.reduce(lote, acc, fn
      {:invalido, _n}, acc ->
        %{acc | invalidos: acc.invalidos + 1}

      {:ok, id, status, prazo}, acc ->
        case Retry.executar(fn -> registrar(id, status, prazo) end) do
          {:ok, _} -> %{acc | ok: acc.ok + 1}
          {:error, _} -> %{acc | falhas: acc.falhas + 1}
        end
    end)
  end

  defp registrar(id, status, prazo) do
    entrega = Estado.registrar_evento(id, status, prazo)
    Notificacoes.avaliar(entrega)
    {:ok, entrega}
  end
end

defmodule Entregas.Sistema do
  @moduledoc """
  Supervisor da aplicação. Estratégia :one_for_one: se um Agent falhar,
  apenas ele é reiniciado (e recarrega seu estado do snapshot).
  """
  use Supervisor

  def start_link(opts) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    filhos = [
      {Entregas.Estado, snapshot: Keyword.fetch!(opts, :snapshot)},
      {Entregas.Notificacoes, callback: Keyword.get(opts, :callback, &IO.inspect/1)}
    ]

    Supervisor.init(filhos, strategy: :one_for_one, max_restarts: 10, max_seconds: 5)
  end
end

defmodule Entregas.Demo do
  @moduledoc "Demonstração de ponta a ponta de todos os requisitos."
  alias Entregas.{Sistema, Estado, Notificacoes, Rastreamento, Vigia, Retry}

  def executar(total \\ 5_000) do
    dir = Path.join(System.tmp_dir!(), "entregas_#{System.unique_integer([:positive])}")
    snapshot = Path.join(dir, "estado.bin")
    eventos = Path.join(dir, "eventos.csv")
    File.mkdir_p!(dir)
    gerar_eventos(eventos, total)

    {:ok, _sup} = Sistema.start_link(snapshot: snapshot, callback: fn _ -> :ok end)
    {:ok, _vigia} = Vigia.iniciar([Estado, Notificacoes])

    resultado = Rastreamento.processar_arquivo(eventos)
    IO.inspect(resultado, label: "processamento")

    {:ok, antes} = Retry.executar(fn -> {:ok, Estado.total()} end)

    # Falha inesperada: derruba o Agent de estado.
    # O supervisor o reinicia, o estado é recarregado do snapshot
    # e o vigia emite a notificação de encerramento inesperado.
    Process.exit(Process.whereis(Estado), :kill)

    {:ok, depois} = Retry.executar(fn -> {:ok, Estado.total()} end)
    IO.inspect({antes, depois}, label: "recuperado_do_snapshot")

    IO.inspect(Rastreamento.processar_arquivo(eventos), label: "reprocessamento")

    Process.sleep(200)
    IO.inspect(Notificacoes.total(), label: "notificacoes_total")
    IO.inspect(Enum.take(Notificacoes.recentes(), 3), label: "notificacoes_recentes")
  end

  defp gerar_eventos(caminho, total) do
    agora = System.system_time(:second)
    statuses = ~w(postado em_transito saiu_para_entrega entregue extraviada)

    1..total
    |> Stream.map(fn i ->
      status = Enum.at(statuses, rem(i, 5))
      prazo = if rem(i, 3) == 0, do: agora - 3600, else: agora + 3600
      "ENT#{i},#{status},#{prazo}\n"
    end)
    |> Stream.into(File.stream!(caminho))
    |> Stream.run()
  end
end
