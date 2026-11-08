# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de pedidos de um e-commerce, atendendo somente aos seguintes requisitos:
#
# 1. Detectar encerramentos inesperados de processos.
# 2. Permitir comunicação entre diferentes processos.
# 3. Persistir os dados para recuperação após reinicializações.
# 4. Implementar expiração automática de registros.
# 5. Permitir que diferentes entidades sejam processadas independentemente.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# Arquivo único: gerenciamento de pedidos de e-commerce em Elixir.
#
# Requisitos atendidos:
#   1. Detecção de encerramentos inesperados: Pedidos.Monitor monitora cada
#      processo de pedido (Process.monitor/1) e trata qualquer motivo de saída
#      diferente de :normal/:shutdown como falha inesperada.
#   2. Comunicação entre processos: pub/sub com Registry em modo :duplicate
#      e chamadas síncronas/assíncronas entre GenServers.
#   3. Persistência: :dets (armazenamento em disco da OTP), reaberto na
#      reinicialização. Cada pedido é recuperado e reprocessado no boot.
#   4. Expiração automática: cada pedido agenda o próprio vencimento com
#      Process.send_after/3; ao expirar, o registro é removido do disco.
#   5. Processamento independente: cada pedido é um processo próprio, supervisionado
#      por um DynamicSupervisor e localizado via Registry pelo id.
#
# Requer apenas a biblioteca padrão (OTP/Elixir).
# Uso básico:
#   {:ok, _} = Pedidos.start_link(arquivo: "pedidos.dets")
#   Pedidos.assinar()
#   {:ok, _} = Pedidos.criar("p1", [%{sku: "A", qtd: 2}], 60_000)
#   :ok = Pedidos.pagar("p1")
#   {:ok, pedido} = Pedidos.consultar("p1")

defmodule Pedidos do
  @moduledoc """
  Supervisor raiz e API pública do sistema de gerenciamento de pedidos.

  Árvore de supervisão (rest_for_one):
    * Registry de processos (id -> pid de cada pedido)
    * Registry de pub/sub (assinantes de eventos)
    * Armazenamento (DETS)
    * DynamicSupervisor (um processo por pedido)
    * Monitor (detecção de falhas inesperadas)
  """

  use Supervisor

  @sup Pedidos.ProcessosSup
  @registro Pedidos.Registro
  @pubsub Pedidos.PubSub
  @ttl_padrao 3_600_000

  # ---------------------------------------------------------------------------
  # Supervisor raiz
  # ---------------------------------------------------------------------------

  @doc "Inicia a árvore de supervisão. Opções: `:arquivo` (caminho do DETS)."
  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    arquivo = Keyword.get(opts, :arquivo, "pedidos.dets")

    children = [
      # Registry de nomes: garante um único processo por id de pedido.
      {Registry, keys: :unique, name: @registro},
      # Registry de pub/sub: permite múltiplos assinantes por evento.
      {Registry, keys: :duplicate, name: @pubsub},
      # Persistência em disco.
      {Pedidos.Armazenamento, arquivo},
      # Supervisor dinâmico: cria um processo independente por pedido.
      {DynamicSupervisor, name: @sup, strategy: :one_for_one},
      # Monitor: recupera pedidos do disco e detecta falhas inesperadas.
      {Pedidos.Monitor, @sup}
    ]

    # rest_for_one: se o armazenamento cair, os processos que dependem dele
    # também são reiniciados, mantendo a consistência.
    Supervisor.init(children, strategy: :rest_for_one)
  end

  # ---------------------------------------------------------------------------
  # API pública
  # ---------------------------------------------------------------------------

  @doc """
  Cria um pedido com expiração em `ttl_ms` milissegundos.
  Retorna `{:error, :ja_existe}` se o id já estiver em uso.
  """
  def criar(id, itens, ttl_ms \\ @ttl_padrao) do
    agora = agora()

    dados = %{
      id: id,
      itens: itens,
      status: :aguardando_pagamento,
      criado_em: agora,
      expira_em: agora + ttl_ms
    }

    # A persistência vem antes do processo: se o processo cair, ele se recupera do disco.
    with :ok <- Pedidos.Armazenamento.inserir(id, dados),
         {:ok, _pid} <- DynamicSupervisor.start_child(@sup, {Pedidos.Processo, id}) do
      publicar({:pedido_criado, id})
      {:ok, dados}
    end
  end

  @doc "Marca o pedido como pago (apenas se estiver aguardando pagamento)."
  def pagar(id), do: chamar(id, :pagar)

  @doc "Cancela o pedido (apenas se estiver aguardando pagamento)."
  def cancelar(id), do: chamar(id, :cancelar)

  @doc "Consulta o estado atual do pedido."
  def consultar(id), do: chamar(id, :consultar)

  @doc "Provoca uma falha inesperada no processo do pedido (útil para testes)."
  def simular_falha(id) do
    GenServer.cast(Pedidos.Processo.via(id), :falhar)
  catch
    :exit, _ -> {:error, :nao_encontrado}
  end

  @doc "Assina os eventos do sistema. O assinante recebe `{:pedidos, evento}`."
  def assinar do
    Registry.register(@pubsub, :eventos, nil)
  end

  @doc false
  def publicar(evento) do
    Registry.dispatch(@pubsub, :eventos, fn entradas ->
      for {pid, _} <- entradas, do: send(pid, {:pedidos, evento})
    end)
  end

  @doc false
  def agora, do: System.system_time(:millisecond)

  defp chamar(id, msg) do
    GenServer.call(Pedidos.Processo.via(id), msg)
  catch
    :exit, _ -> {:error, :nao_encontrado}
  end
end

# -----------------------------------------------------------------------------
# Persistência em DETS (armazenamento em disco da OTP)
# -----------------------------------------------------------------------------

defmodule Pedidos.Armazenamento do
  @moduledoc """
  Persiste os pedidos em disco com :dets. Todas as escritas são sincronizadas
  antes de responder, garantindo recuperação após reinicializações.
  """

  use GenServer

  @tabela :pedidos_dets

  def start_link(arquivo) do
    GenServer.start_link(__MODULE__, arquivo, name: __MODULE__)
  end

  @doc false
  def inserir(id, dados), do: GenServer.call(__MODULE__, {:inserir, id, dados})
  @doc false
  def salvar(id, dados), do: GenServer.call(__MODULE__, {:salvar, id, dados})
  @doc false
  def ler(id), do: GenServer.call(__MODULE__, {:ler, id})
  @doc false
  def remover(id), do: GenServer.call(__MODULE__, {:remover, id})
  @doc false
  def ids, do: GenServer.call(__MODULE__, :ids)

  @impl true
  def init(arquivo) do
    case :dets.open_file(@tabela, file: String.to_charlist(arquivo), type: :set) do
      {:ok, tabela} -> {:ok, tabela}
      {:error, motivo} -> {:stop, motivo}
    end
  end

  @impl true
  def handle_call({:inserir, id, dados}, _from, tabela) do
    if :dets.member(tabela, id) do
      {:reply, {:error, :ja_existe}, tabela}
    else
      gravar(tabela, id, dados)
      {:reply, :ok, tabela}
    end
  end

  def handle_call({:salvar, id, dados}, _from, tabela) do
    gravar(tabela, id, dados)
    {:reply, :ok, tabela}
  end

  def handle_call({:ler, id}, _from, tabela) do
    resposta =
      case :dets.lookup(tabela, id) do
        [{^id, dados}] -> dados
        [] -> nil
      end

    {:reply, resposta, tabela}
  end

  def handle_call({:remover, id}, _from, tabela) do
    :ok = :dets.delete(tabela, id)
    :ok = :dets.sync(tabela)
    {:reply, :ok, tabela}
  end

  def handle_call(:ids, _from, tabela) do
    ids = :dets.foldl(fn {id, _}, acc -> [id | acc] end, [], tabela)
    {:reply, ids, tabela}
  end

  @impl true
  def terminate(_motivo, tabela) do
    :dets.close(tabela)
  end

  defp gravar(tabela, id, dados) do
    :ok = :dets.insert(tabela, {id, dados})
    :ok = :dets.sync(tabela)
  end
end

# -----------------------------------------------------------------------------
# Processo de um pedido (uma entidade = um processo independente)
# -----------------------------------------------------------------------------

defmodule Pedidos.Processo do
  @moduledoc """
  Processo que representa um único pedido.

  * `restart: :transient`: reinicia apenas em caso de falha, não após término normal.
  * Ao iniciar (inclusive após uma falha), carrega o estado do disco.
  * Agenda a própria expiração com `Process.send_after/3`.
  """

  use GenServer, restart: :transient

  require Logger

  def start_link(id) do
    GenServer.start_link(__MODULE__, id, name: via(id))
  end

  @doc false
  def via(id), do: {:via, Registry, {Pedidos.Registro, id}}

  @impl true
  def init(id) do
    case Pedidos.Armazenamento.ler(id) do
      # Registro inexistente (ex.: já expirado): não cria processo.
      nil ->
        :ignore

      dados ->
        # Expiração: se o prazo já passou (ex.: após reinicialização), dispara imediatamente.
        restante = max(dados.expira_em - Pedidos.agora(), 0)
        Process.send_after(self(), :expirar, restante)

        # Informa ao Monitor para observar este processo.
        GenServer.cast(Pedidos.Monitor, {:observar, id, self()})

        {:ok, dados}
    end
  end

  @impl true
  def handle_call(:pagar, _from, %{status: :aguardando_pagamento} = dados) do
    novo = %{dados | status: :pago}
    :ok = Pedidos.Armazenamento.salvar(novo.id, novo)
    Pedidos.publicar({:pedido_pago, novo.id})
    {:reply, :ok, novo}
  end

  def handle_call(:cancelar, _from, %{status: :aguardando_pagamento} = dados) do
    novo = %{dados | status: :cancelado}
    :ok = Pedidos.Armazenamento.salvar(novo.id, novo)
    Pedidos.publicar({:pedido_cancelado, novo.id})
    {:reply, :ok, novo}
  end

  def handle_call(op, _from, dados) when op in [:pagar, :cancelar] do
    {:reply, {:error, {:status_invalido, dados.status}}, dados}
  end

  def handle_call(:consultar, _from, dados) do
    {:reply, {:ok, dados}, dados}
  end

  @impl true
  def handle_cast(:falhar, dados) do
    # Falha intencional para demonstrar detecção e recuperação.
    raise "falha simulada no pedido #{dados.id}"
  end

  @impl true
  def handle_info(:expirar, dados) do
    :ok = Pedidos.Armazenamento.remover(dados.id)
    Pedidos.publicar({:pedido_expirado, dados.id})
    # Término normal: não é reiniciado pelo supervisor.
    {:stop, :normal, dados}
  end
end

# -----------------------------------------------------------------------------
# Monitor: recuperação no boot e detecção de encerramentos inesperados
# -----------------------------------------------------------------------------

defmodule Pedidos.Monitor do
  @moduledoc """
  Responsabilidades:
    * No boot, recria um processo para cada pedido persistido.
    * Monitora (Process.monitor/1) os processos de pedidos.
    * Ao receber um :DOWN com motivo anormal, registra o erro e publica
      o evento `{:processo_encerrado_inesperadamente, id, motivo}`.
      O supervisor reinicia o processo, que recupera o estado do disco.
  """

  use GenServer

  require Logger

  def start_link(sup) do
    GenServer.start_link(__MODULE__, sup, name: __MODULE__)
  end

  @impl true
  def init(sup) do
    # Recuperação após reinicialização: um processo por pedido persistido.
    Enum.each(Pedidos.Armazenamento.ids(), fn id ->
      DynamicSupervisor.start_child(sup, {Pedidos.Processo, id})
    end)

    {:ok, %{refs: %{}}}
  end

  @impl true
  def handle_cast({:observar, id, pid}, estado) do
    ref = Process.monitor(pid)
    {:noreply, %{estado | refs: Map.put(estado.refs, ref, id)}}
  end

  @impl true
  def handle_info({:DOWN, ref, :process, _pid, motivo}, estado) do
    {id, refs} = Map.pop(estado.refs, ref)
    estado = %{estado | refs: refs}

    cond do
      # Referência desconhecida: nada a fazer.
      is_nil(id) ->
        {:noreply, estado}

      # Encerramentos esperados.
      motivo in [:normal, :shutdown] or match?({:shutdown, _}, motivo) ->
        {:noreply, estado}

      # Encerramento inesperado: registra, notifica e deixa o supervisor reiniciar.
      true ->
        Logger.warning("Processo do pedido #{id} encerrou inesperadamente: #{inspect(motivo)}")
        Pedidos.publicar({:processo_encerrado_inesperadamente, id, motivo})
        {:noreply, estado}
    end
  end
end
