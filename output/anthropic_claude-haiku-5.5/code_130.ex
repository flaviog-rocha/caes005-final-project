# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de entregas e rastreamento, atendendo somente aos seguintes requisitos:
#
# 1. Recuperar processos após falhas inesperadas.
# 2. Permitir que diferentes entidades sejam processadas independentemente.
# 3. Implementar novas tentativas após falhas temporárias.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule Entregas do
  @moduledoc """
  Gerenciamento de entregas e rastreamento tolerante a falhas.

  Requisitos atendidos:

    * Recuperação após falhas: cada entrega é um processo supervisionado
      (`restart: :transient`) cujo estado é persistido em DETS a cada transição.
      Após uma falha inesperada, o supervisor reinicia o processo, que recarrega
      o estado persistido e retoma o processamento. Após reinicialização do VM,
      `recuperar_pendentes/0` reativa as entregas não finalizadas.

    * Processamento independente: cada entrega roda em seu próprio processo,
      registrado no `Registry` e criado sob um `DynamicSupervisor`. Falhas em
      uma entrega não afetam as demais.

    * Novas tentativas: falhas temporárias agendam nova tentativa com backoff
      exponencial até o limite de tentativas; falhas permanentes finalizam a entrega.

  Uso:

      {:ok, _} = Entregas.start_link(arquivo: "entregas.dets")
      :ok = Entregas.criar("PED-1", %{destino: "Rua A, 10"})
      {:ok, estado} = Entregas.rastrear("PED-1")
  """

  use Supervisor

  # API pública

  @doc "Inicia o sistema (supervisor raiz). Opções: `:arquivo` (caminho do arquivo DETS)."
  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc "Cria uma entrega e inicia seu processamento independente."
  def criar(id, dados \\ %{}) do
    if Entregas.Store.buscar(id) do
      {:error, :ja_existe}
    else
      args = %{
        id: id,
        dados: dados,
        status: :pendente,
        tentativas: 0,
        eventos: [{agora(), :pendente, nil}]
      }

      case DynamicSupervisor.start_child(Entregas.Sup, {Entregas.Entrega, args}) do
        {:ok, _pid} -> :ok
        {:error, {:already_started, _pid}} -> {:error, :ja_existe}
        {:error, motivo} -> {:error, motivo}
      end
    end
  end

  @doc "Consulta o rastreamento (status, tentativas e histórico de eventos)."
  def rastrear(id) do
    case Entregas.Store.buscar(id) do
      nil -> {:error, :nao_encontrada}
      estado -> {:ok, estado}
    end
  end

  @doc "Reinicia o processamento de todas as entregas não finalizadas persistidas."
  def recuperar_pendentes do
    Enum.each(Entregas.Store.pendentes(), fn estado ->
      DynamicSupervisor.start_child(Entregas.Sup, {Entregas.Entrega, estado})
    end)
  end

  # Supervisor raiz

  @impl true
  def init(opts) do
    children = [
      {Entregas.Store, opts},
      {Registry, keys: :unique, name: Entregas.Registry},
      {DynamicSupervisor, strategy: :one_for_one, name: Entregas.Sup},
      Supervisor.child_spec({Task, &recuperar_pendentes/0},
        id: :recuperador,
        restart: :transient
      )
    ]

    # rest_for_one: se o Store cair (fechando o DETS), os processos dependentes reiniciam.
    Supervisor.init(children, strategy: :rest_for_one)
  end

  defp agora, do: System.system_time(:millisecond)
end

defmodule Entregas.Store do
  @moduledoc """
  Persistência do estado das entregas em DETS (biblioteca padrão).
  O estado é gravado a cada transição, permitindo recuperação após falhas.
  """
  use GenServer

  @tabela :entregas_store

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc "Grava (com sincronização em disco) o estado de uma entrega."
  def salvar(estado) do
    true = :dets.insert(@tabela, {estado.id, estado})
    :dets.sync(@tabela)
  end

  @doc "Busca o estado persistido de uma entrega."
  def buscar(id) do
    case :dets.lookup(@tabela, id) do
      [{_id, estado}] -> estado
      [] -> nil
    end
  end

  @doc "Lista os estados de entregas ainda não finalizadas."
  def pendentes do
    :dets.foldl(
      fn {_id, estado}, acc ->
        if Entregas.Entrega.final?(estado.status), do: acc, else: [estado | acc]
      end,
      [],
      @tabela
    )
  end

  @impl true
  def init(opts) do
    # Permite que terminate/2 feche o DETS corretamente em desligamentos.
    Process.flag(:trap_exit, true)
    arquivo = opts |> Keyword.get(:arquivo, "entregas.dets") |> String.to_charlist()
    {:ok, _} = :dets.open_file(@tabela, file: arquivo, type: :set)
    {:ok, nil}
  end

  @impl true
  def terminate(_motivo, _estado) do
    :dets.close(@tabela)
  end
end

defmodule Entregas.Entrega do
  @moduledoc """
  Processo de uma entrega individual.

  Restart `:transient`: é reiniciado após falhas inesperadas, mas não após
  término normal (entrega concluída ou falha definitiva).
  """
  use GenServer, restart: :transient

  @finais [:entregue, :falhou]
  @max_tentativas 5
  @backoff_base_ms 500
  @backoff_max_ms 30_000

  @doc "Indica se um status representa estado final."
  def final?(status), do: status in @finais

  def start_link(%{id: id} = estado) do
    GenServer.start_link(__MODULE__, estado, name: via(id))
  end

  def via(id), do: {:via, Registry, {Entregas.Registry, id}}

  @impl true
  def init(estado) do
    # Prefere o estado persistido: após uma falha, o supervisor reinicia com os
    # argumentos originais, e o progresso real está no DETS.
    estado = Entregas.Store.buscar(estado.id) || estado
    :ok = Entregas.Store.salvar(estado)
    {:ok, estado, {:continue, :processar}}
  end

  @impl true
  def handle_continue(:processar, %{status: status} = estado) when status in @finais do
    {:stop, :normal, estado}
  end

  def handle_continue(:processar, estado) do
    tentativa = estado.tentativas + 1

    # Persiste a tentativa ANTES de executá-la: se o processo cair durante a
    # chamada, a contagem de tentativas sobrevive à reinicialização.
    estado =
      atualizar(estado, %{status: :processando, tentativas: tentativa}, :tentativa, tentativa)

    case Entregas.Transporte.enviar(estado.id, estado.dados, tentativa) do
      {:ok, codigo} ->
        estado = atualizar(estado, %{status: :entregue}, :entregue, codigo)
        {:stop, :normal, estado}

      {:error, :permanente, motivo} ->
        estado = atualizar(estado, %{status: :falhou}, :falhou, motivo)
        {:stop, :normal, estado}

      {:error, :temporario, motivo} when tentativa >= @max_tentativas ->
        estado =
          atualizar(estado, %{status: :falhou}, :falhou, {:limite_de_tentativas, motivo})

        {:stop, :normal, estado}

      {:error, :temporario, motivo} ->
        atraso = backoff(tentativa)

        estado =
          atualizar(
            estado,
            %{status: :aguardando_nova_tentativa},
            :falha_temporaria,
            {motivo, atraso}
          )

        Process.send_after(self(), :nova_tentativa, atraso)
        {:noreply, estado}
    end
  end

  @impl true
  def handle_info(:nova_tentativa, estado) do
    {:noreply, estado, {:continue, :processar}}
  end

  # Atualiza o estado, registra o evento de rastreamento e persiste.
  defp atualizar(estado, mudancas, tipo, detalhe) do
    novo =
      estado
      |> Map.merge(mudancas)
      |> Map.update!(:eventos, &[{agora(), tipo, detalhe} | &1])

    :ok = Entregas.Store.salvar(novo)
    novo
  end

  # Backoff exponencial limitado: 500ms, 1s, 2s, 4s, ... até 30s.
  defp backoff(tentativa) do
    min(@backoff_base_ms * Integer.pow(2, tentativa - 1), @backoff_max_ms)
  end

  defp agora, do: System.system_time(:millisecond)
end

defmodule Entregas.Transporte do
  @moduledoc """
  Integração simulada com a transportadora. Substitua pela chamada real.

  Deve ser idempotente por `id`, pois uma tentativa pode ser repetida
  após falhas ou reinicializações do processo.
  """

  @doc "Retorna {:ok, código_rastreio} | {:error, :temporario | :permanente, motivo}."
  def enviar(id, _dados, _tentativa) do
    case :rand.uniform(10) do
      n when n <= 3 -> {:error, :temporario, :indisponivel}
      _ -> {:ok, "TRK-#{id}"}
    end
  end
end
