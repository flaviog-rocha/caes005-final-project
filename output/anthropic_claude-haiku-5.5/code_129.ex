# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de entregas e rastreamento, atendendo somente aos seguintes requisitos:
#
# 1. Recuperar processos após falhas inesperadas.
# 2. Permitir que diferentes entidades sejam processadas independentemente.
# 3. Implementar novas tentativas após falhas temporárias.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# Gerenciamento de entregas e rastreamento com OTP (biblioteca padrão).
#
# Requisitos atendidos:
#   1. Recuperação após falhas inesperadas: cada entrega é um GenServer supervisionado
#      que persiste seu estado em DETS antes de cada ação externa. Ao reiniciar (por crash
#      ou após reinício da VM), o estado é restaurado e o processamento é retomado.
#   2. Processamento independente: cada entrega roda em seu próprio processo, registrado
#      no Registry e supervisionado por um DynamicSupervisor (falhas são isoladas).
#   3. Novas tentativas: falhas temporárias são reagendadas com backoff exponencial
#      e jitter; falhas permanentes ou exceder o limite finalizam a entrega como :falhou.
#
# Execução: `elixir entregas.exs`

defmodule Entregas.Aplicacao do
  @moduledoc false
  # Árvore de supervisão raiz.
  use Application

  @impl true
  def start(_tipo, _args) do
    filhos = [
      # Armazenamento durável (DETS) - sobrevive a falhas de processos de entrega
      Entregas.Armazenamento,
      # Registro de nomes para localizar o processo de cada entrega
      {Registry, keys: :unique, name: Entregas.Registro},
      # Supervisor dinâmico: um filho por entrega, isolamento de falhas (one_for_one)
      {DynamicSupervisor,
       strategy: :one_for_one, name: Entregas.SupervisorEntidades, max_restarts: 100, max_seconds: 5},
      # Reprocessa entregas não finalizadas após inicialização (recuperação pós-falha)
      Supervisor.child_spec({Task, fn -> Entregas.recuperar_pendentes() end}, restart: :transient)
    ]

    Supervisor.start_link(filhos, strategy: :one_for_one, name: Entregas.SupervisorRaiz)
  end
end

defmodule Entregas.Armazenamento do
  @moduledoc false
  # Persistência durável em DETS. Estados são gravados como {id, estado}.
  use GenServer

  @tabela :entregas_armazenamento

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    arquivo =
      opts
      |> Keyword.get(:arquivo, Application.get_env(:entregas, :arquivo, "entregas.dets"))
      |> to_charlist()

    case :dets.open_file(@tabela, file: arquivo, type: :set) do
      {:ok, @tabela} -> {:ok, nil}
      {:error, motivo} -> {:stop, motivo}
    end
  end

  @impl true
  def terminate(_motivo, _estado) do
    :dets.close(@tabela)
  end

  # Grava o estado e garante escrita em disco antes de prosseguir
  def salvar(estado) do
    true = :dets.insert(@tabela, {estado.id, estado})
    :ok = :dets.sync(@tabela)
    estado
  end

  def buscar(id) do
    case :dets.lookup(@tabela, id) do
      [{^id, estado}] -> {:ok, estado}
      [] -> :error
    end
  end

  # Retorna os ids de entregas que ainda não foram finalizadas
  def listar_nao_finalizadas do
    :dets.foldl(
      fn {id, estado}, acc ->
        if estado.status in [:entregue, :falhou], do: acc, else: [id | acc]
      end,
      [],
      @tabela
    )
  end
end

defmodule Entregas.Transportadora do
  @moduledoc false
  # Simula a integração com um serviço externo de envio, com falhas aleatórias.
  # Em produção, substitua por uma chamada HTTP/fila real.

  @doc "Retorna :ok, {:error, :temporario, motivo} ou {:error, :permanente, motivo}."
  def enviar(_dados) do
    case :rand.uniform(100) do
      n when n <= 15 -> {:error, :temporario, "serviço indisponível"}
      n when n <= 17 -> {:error, :permanente, "endereço inválido"}
      # Falha inesperada: exceção que derruba o processo (testa a recuperação)
      n when n <= 18 -> raise "falha inesperada na transportadora"
      _ -> :ok
    end
  end
end

defmodule Entregas.Entrega do
  @moduledoc false
  # Processo de uma única entrega. Cada entidade é independente.
  # restart: :transient -> reinicia após falhas, mas não após término normal.
  use GenServer, restart: :transient

  @estados_finais [:entregue, :falhou]
  @max_tentativas 5
  @base_backoff_ms 200
  @max_backoff_ms 3_000

  def start_link({id, _dados} = arg) do
    GenServer.start_link(__MODULE__, arg, name: via(id))
  end

  defp via(id), do: {:via, Registry, {Entregas.Registro, id}}

  @impl true
  def init({id, dados}) do
    estado =
      case Entregas.Armazenamento.buscar(id) do
        # Recuperação: restaura o estado persistido
        {:ok, salvo} -> salvo
        # Nova entrega: cria e persiste
        :error -> novo(id, dados) |> Entregas.Armazenamento.salvar()
      end

    {:ok, estado, {:continue, :retomar}}
  end

  @impl true
  def handle_continue(:retomar, estado) do
    {:noreply, agendar_processamento(estado)}
  end

  @impl true
  def handle_info(:processar, %{status: status} = estado) when status in @estados_finais do
    {:noreply, estado}
  end

  def handle_info(:processar, estado) do
    tentativa = estado.tentativas + 1

    if tentativa > @max_tentativas do
      # Protege contra laços de crash infinitos (ex.: exceções repetidas)
      {:noreply, transicionar(estado, :falhou, "limite de tentativas excedido")}
    else
      # Persiste a intenção ANTES da ação externa: se o processo cair durante o envio,
      # a recuperação retoma a partir deste ponto.
      estado =
        %{estado | tentativas: tentativa}
        |> transicionar(:em_transito, "envio iniciado (tentativa #{tentativa})")

      case Entregas.Transportadora.enviar(estado.dados) do
        :ok ->
          {:noreply, transicionar(estado, :entregue, "entrega confirmada")}

        {:error, :permanente, motivo} ->
          {:noreply, transicionar(estado, :falhou, "falha permanente: #{motivo}")}

        {:error, :temporario, motivo} ->
          {:noreply, agendar_nova_tentativa(estado, motivo)}
      end
    end
  end

  @impl true
  def handle_call(:rastrear, _from, estado) do
    {:reply, {:ok, resumo(estado)}, estado}
  end

  # Agendamento de nova tentativa com backoff exponencial e jitter
  defp agendar_nova_tentativa(estado, motivo) do
    if estado.tentativas >= @max_tentativas do
      transicionar(estado, :falhou, "limite de tentativas atingido: #{motivo}")
    else
      atraso = backoff(estado.tentativas)

      estado =
        %{estado | proxima_tentativa_em: agora() + atraso}
        |> transicionar(
          :aguardando_retry,
          "falha temporária (#{motivo}); nova tentativa em #{atraso} ms"
        )

      Process.send_after(self(), :processar, atraso)
      estado
    end
  end

  # Ao (re)iniciar, calcula quanto falta para o próximo processamento
  defp agendar_processamento(%{status: status} = estado) when status in @estados_finais do
    estado
  end

  defp agendar_processamento(estado) do
    atraso = max(estado.proxima_tentativa_em - agora(), 0)
    Process.send_after(self(), :processar, atraso)
    estado
  end

  defp backoff(tentativas) do
    base = min(@base_backoff_ms * Integer.pow(2, tentativas - 1), @max_backoff_ms)
    # Jitter para evitar sincronização de retentativas
    base + :rand.uniform(div(base, 4) + 1) - 1
  end

  defp novo(id, dados) do
    %{
      id: id,
      dados: dados,
      status: :pendente,
      tentativas: 0,
      proxima_tentativa_em: agora(),
      historico: []
    }
    |> registrar_evento("entrega criada")
  end

  defp transicionar(estado, status, mensagem) do
    %{estado | status: status}
    |> registrar_evento(mensagem)
    |> Entregas.Armazenamento.salvar()
  end

  defp registrar_evento(estado, mensagem) do
    evento = {agora(), estado.status, mensagem}
    %{estado | historico: [evento | estado.historico]}
  end

  defp resumo(estado) do
    %{
      id: estado.id,
      status: estado.status,
      tentativas: estado.tentativas,
      historico: Enum.reverse(estado.historico)
    }
  end

  defp agora, do: System.system_time(:millisecond)
end

defmodule Entregas do
  @moduledoc """
  API pública para criar entregas, consultar rastreamento e recuperar pendências.
  """

  @doc "Cria uma nova entrega e inicia seu processamento independente."
  def adicionar(id, dados) do
    case Entregas.Armazenamento.buscar(id) do
      {:ok, _} ->
        {:error, :ja_existente}

      :error ->
        case DynamicSupervisor.start_child(
               Entregas.SupervisorEntidades,
               {Entregas.Entrega, {id, dados}}
             ) do
          {:ok, _pid} -> :ok
          {:error, {:already_started, _pid}} -> {:error, :ja_existente}
          outro -> outro
        end
    end
  end

  @doc "Consulta o rastreamento de uma entrega (processo ativo ou dados persistidos)."
  def rastrear(id) do
    case Registry.lookup(Entregas.Registro, id) do
      [{pid, _}] ->
        GenServer.call(pid, :rastrear)

      [] ->
        case Entregas.Armazenamento.buscar(id) do
          {:ok, e} ->
            {:ok,
             %{
               id: e.id,
               status: e.status,
               tentativas: e.tentativas,
               historico: Enum.reverse(e.historico)
             }}

          :error ->
            {:error, :nao_encontrada}
        end
    end
  end

  @doc "Reinicia o processamento de todas as entregas não finalizadas."
  def recuperar_pendentes do
    for id <- Entregas.Armazenamento.listar_nao_finalizadas() do
      # Dados vêm do armazenamento; o argumento `nil` é ignorado na recuperação
      DynamicSupervisor.start_child(Entregas.SupervisorEntidades, {Entregas.Entrega, {id, nil}})
    end

    :ok
  end
end

defmodule Entregas.Demo do
  @moduledoc false
  # Demonstração: cria entregas, simula falha de processo e consulta o rastreamento.

  def executar do
    {:ok, _} = Entregas.Aplicacao.start(:normal, [])

    ids = for _ <- 1..5, do: "PED-#{System.unique_integer([:positive])}"

    for id <- ids do
      :ok = Entregas.adicionar(id, %{destino: "Rua #{id}"})
    end

    # Simula uma falha inesperada em um processo de entrega
    [{pid, _}] = Registry.lookup(Entregas.Registro, hd(ids))
    Process.exit(pid, :kill)

    # Aguarda processamento, retentativas e recuperação
    Process.sleep(5_000)

    for id <- ids do
      IO.inspect(Entregas.rastrear(id), label: id)
    end
  end
end

Entregas.Demo.executar()
