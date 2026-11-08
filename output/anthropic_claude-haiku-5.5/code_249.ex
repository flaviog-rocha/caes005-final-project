# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de troca de mensagens entre usuários, atendendo somente aos seguintes requisitos:
#
# 1. Implementar expiração automática de registros.
# 2. Persistir os dados para recuperação após reinicializações.
# 3. Executar determinadas operações periodicamente.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# Sistema de troca de mensagens entre usuários em Elixir (arquivo único, apenas biblioteca padrão).
#
# Requisitos atendidos:
#   1. Expiração automática: cada mensagem tem um TTL; mensagens expiradas são
#      ignoradas nas leituras e removidas periodicamente.
#   2. Persistência: os dados são gravados em disco via :dets e recuperados
#      automaticamente após reinicializações.
#   3. Operações periódicas: timers agendados com Process.send_after/3 executam
#      a limpeza de expirados e a sincronização do arquivo em disco.
#
# Uso (exemplo):
#
#   {:ok, _pid} = Mensageria.Servidor.start_link(arquivo: "msgs.dets", ttl_ms: 30_000)
#   {:ok, id} = Mensageria.Servidor.enviar(Mensageria.Servidor, "ana", "bruno", "oi")
#   Mensageria.Servidor.caixa_entrada(Mensageria.Servidor, "bruno")
#   Mensageria.Servidor.ler(Mensageria.Servidor, id)
#   Mensageria.Servidor.limpar_expirados(Mensageria.Servidor)

defmodule Mensageria.Servidor do
  @moduledoc """
  GenServer responsável por armazenar, expirar e persistir mensagens entre usuários.
  """

  use GenServer

  # Tempo de vida padrão de uma mensagem (ms).
  @ttl_padrao_ms 60_000
  # Intervalo padrão da limpeza periódica de mensagens expiradas (ms).
  @intervalo_limpeza_padrao_ms 5_000
  # Intervalo da sincronização periódica do arquivo DETS (ms).
  @intervalo_sync_ms 10_000
  # Arquivo padrão de persistência.
  @arquivo_padrao "mensageria.dets"

  # ---------------------------------------------------------------------------
  # API pública
  # ---------------------------------------------------------------------------

  @doc "Inicia o servidor. Opções: :name, :arquivo, :ttl_ms, :intervalo_limpeza_ms."
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))
  end

  @doc "Envia uma mensagem de `de` para `para`. `ttl_ms` é opcional."
  def enviar(servidor, de, para, corpo, ttl_ms \\ nil) do
    GenServer.call(servidor, {:enviar, de, para, corpo, ttl_ms})
  end

  @doc "Lista as mensagens ativas (não expiradas) recebidas pelo usuário."
  def caixa_entrada(servidor, usuario) do
    GenServer.call(servidor, {:caixa_entrada, usuario})
  end

  @doc "Marca a mensagem como lida e a retorna, se ainda estiver válida."
  def ler(servidor, id) do
    GenServer.call(servidor, {:ler, id})
  end

  @doc "Remove uma mensagem pelo id."
  def remover(servidor, id) do
    GenServer.call(servidor, {:remover, id})
  end

  @doc "Força a remoção imediata das mensagens expiradas. Retorna a quantidade removida."
  def limpar_expirados(servidor) do
    GenServer.call(servidor, :limpar_expirados)
  end

  # ---------------------------------------------------------------------------
  # Callbacks do GenServer
  # ---------------------------------------------------------------------------

  @impl true
  def init(opts) do
    # Permite que terminate/2 seja chamado em desligamentos controlados pelo supervisor.
    Process.flag(:trap_exit, true)

    arquivo = opts |> Keyword.get(:arquivo, @arquivo_padrao) |> to_charlist()

    case :dets.open_file(:mensageria_dets, file: arquivo, type: :set) do
      {:ok, tab} ->
        # Garante o contador de ids persistido (não sobrescreve um valor existente).
        :dets.insert_new(tab, {:seq, 0})

        intervalo = Keyword.get(opts, :intervalo_limpeza_ms, @intervalo_limpeza_padrao_ms)
        agendar(:limpar, intervalo)
        agendar(:sync, @intervalo_sync_ms)

        {:ok,
         %{
           tab: tab,
           ttl: Keyword.get(opts, :ttl_ms, @ttl_padrao_ms),
           intervalo: intervalo
         }}

      {:error, motivo} ->
        {:stop, motivo}
    end
  end

  @impl true
  def handle_call({:enviar, de, para, corpo, ttl_ms}, _from, s) do
    agora = agora_ms()
    ttl = ttl_ms || s.ttl
    # update_counter/3 é atômico e persistido no arquivo.
    id = :dets.update_counter(s.tab, :seq, 1)

    msg = %{
      id: id,
      de: de,
      para: para,
      corpo: corpo,
      enviada_em: agora,
      expira_em: agora + ttl,
      lida: false
    }

    :ok = :dets.insert(s.tab, {id, msg})
    {:reply, {:ok, id}, s}
  end

  def handle_call({:caixa_entrada, usuario}, _from, s) do
    agora = agora_ms()

    msgs =
      s.tab
      |> mensagens()
      |> Enum.filter(&(&1.para == usuario and &1.expira_em > agora))
      |> Enum.sort_by(& &1.id)

    {:reply, msgs, s}
  end

  def handle_call({:ler, id}, _from, s) do
    agora = agora_ms()

    case :dets.lookup(s.tab, id) do
      [{^id, %{expira_em: exp} = msg}] when exp > agora ->
        msg = %{msg | lida: true}
        :ok = :dets.insert(s.tab, {id, msg})
        {:reply, {:ok, msg}, s}

      [{^id, _expirada}] ->
        :ok = :dets.delete(s.tab, id)
        {:reply, {:error, :expirada}, s}

      [] ->
        {:reply, {:error, :nao_encontrada}, s}
    end
  end

  def handle_call({:remover, id}, _from, s) do
    :ok = :dets.delete(s.tab, id)
    {:reply, :ok, s}
  end

  def handle_call(:limpar_expirados, _from, s) do
    {:reply, {:ok, limpar(s.tab)}, s}
  end

  @impl true
  # Operação periódica: remoção de mensagens expiradas.
  def handle_info(:limpar, s) do
    limpar(s.tab)
    agendar(:limpar, s.intervalo)
    {:noreply, s}
  end

  # Operação periódica: sincronização do arquivo em disco.
  def handle_info(:sync, s) do
    :dets.sync(s.tab)
    agendar(:sync, @intervalo_sync_ms)
    {:noreply, s}
  end

  def handle_info(_msg, s), do: {:noreply, s}

  @impl true
  def terminate(_motivo, s) do
    :dets.sync(s.tab)
    :dets.close(s.tab)
    :ok
  end

  # ---------------------------------------------------------------------------
  # Funções auxiliares privadas
  # ---------------------------------------------------------------------------

  # Retorna todas as mensagens armazenadas (ignora o contador :seq).
  defp mensagens(tab) do
    :dets.foldl(
      fn
        {id, %{} = msg}, acc when is_integer(id) -> [msg | acc]
        _, acc -> acc
      end,
      [],
      tab
    )
  end

  # Remove as mensagens cujo prazo de expiração já passou e retorna a quantidade removida.
  defp limpar(tab) do
    agora = agora_ms()

    ids =
      :dets.foldl(
        fn
          {id, %{expira_em: exp}}, acc when is_integer(id) and exp <= agora -> [id | acc]
          _, acc -> acc
        end,
        [],
        tab
      )

    Enum.each(ids, &:dets.delete(tab, &1))
    length(ids)
  end

  defp agendar(msg, intervalo_ms) do
    Process.send_after(self(), msg, intervalo_ms)
  end

  # Tempo de parede em milissegundos: estável entre reinicializações (necessário para persistência).
  defp agora_ms, do: System.system_time(:millisecond)
end
