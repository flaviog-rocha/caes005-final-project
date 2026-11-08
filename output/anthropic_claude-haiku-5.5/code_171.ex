# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de empréstimos de livros, atendendo somente aos seguintes requisitos:
#
# 1. Recuperar processos após falhas inesperadas.
# 2. Detectar encerramentos inesperados de processos.
# 3. Garantir a consistência dos dados durante operações concorrentes.
# 4. Permitir cadastrar, consultar, atualizar e remover registros.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# Arquivo único: sistema de empréstimos de livros tolerante a falhas.
#
# Requisitos atendidos:
#   1. Recuperação após falhas: Biblioteca.Sup (Supervisor :one_for_one) reinicia o
#      Acervo; o estado é recarregado do DETS (persistência em disco) no init/1.
#   2. Detecção de encerramentos inesperados: cada empréstimo monitora o processo
#      do leitor (Process.monitor/1). Ao receber {:DOWN, ...}, o empréstimo é liberado.
#   3. Consistência concorrente: todas as operações de escrita/leitura passam pelo
#      GenServer, que as processa uma a uma. Verificação e alteração ocorrem na mesma
#      mensagem, sem janelas de corrida (ex.: dois leitores não pegam o último exemplar).
#   4. CRUD: cadastrar, consultar, atualizar e remover livros; emprestar, consultar,
#      listar e devolver empréstimos.
#
# Uso:
#   {:ok, _} = Biblioteca.Sup.start_link(arquivo: "biblioteca.dets")
#   {:ok, livro} = Biblioteca.Acervo.cadastrar_livro(%{titulo: "Dom Casmurro", autor: "Machado de Assis", exemplares: 2})
#   {:ok, emp} = Biblioteca.Acervo.emprestar(livro.id, "Maria")
#   {:ok, _} = Biblioteca.Acervo.devolver(emp.id)

defmodule Biblioteca.Sup do
  @moduledoc false
  # Supervisor responsável por reiniciar o Acervo após falhas inesperadas.
  use Supervisor

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    children = [{Biblioteca.Acervo, opts}]
    # Limite de reinícios para evitar loops infinitos de falha.
    Supervisor.init(children, strategy: :one_for_one, max_restarts: 5, max_seconds: 10)
  end
end

defmodule Biblioteca.Acervo do
  @moduledoc false
  # Processo dono do estado (livros e empréstimos), com persistência em DETS.

  use GenServer
  require Logger

  @intervalo_expiracao 60_000
  @dias_padrao 14
  @tabela :biblioteca_dets

  # ---------------------------------------------------------------------------
  # API pública
  # ---------------------------------------------------------------------------

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  # CRUD de livros
  def cadastrar_livro(attrs), do: chamar({:cadastrar_livro, attrs})
  def consultar_livro(id), do: chamar({:consultar_livro, id})
  def listar_livros, do: chamar(:listar_livros)
  def atualizar_livro(id, attrs), do: chamar({:atualizar_livro, id, attrs})
  def remover_livro(id), do: chamar({:remover_livro, id})

  # Empréstimos: o processo chamador é monitorado como responsável pelo empréstimo.
  def emprestar(livro_id, leitor, opts \\ []) do
    chamar({:emprestar, livro_id, leitor, self(), opts})
  end

  def devolver(emprestimo_id), do: chamar({:devolver, emprestimo_id})
  def consultar_emprestimo(id), do: chamar({:consultar_emprestimo, id})
  def listar_emprestimos, do: chamar(:listar_emprestimos)

  # Chamada com novas tentativas caso o Acervo esteja reiniciando.
  defp chamar(msg, tentativas \\ 3) do
    GenServer.call(__MODULE__, msg)
  catch
    :exit, _ when tentativas > 1 ->
      Process.sleep(100)
      chamar(msg, tentativas - 1)

    :exit, _ ->
      {:error, :servico_indisponivel}
  end

  # ---------------------------------------------------------------------------
  # Inicialização, recuperação e encerramento
  # ---------------------------------------------------------------------------

  @impl true
  def init(opts) do
    arquivo = opts |> Keyword.get(:arquivo, "biblioteca.dets") |> String.to_charlist()
    {:ok, tab} = :dets.open_file(@tabela, file: arquivo, type: :set)

    # Recupera o estado persistido antes de aceitar requisições.
    estado = carregar(tab)
    Process.send_after(self(), :expirar, @intervalo_expiracao)

    # monitores: %{referência_do_monitor => id_do_emprestimo}
    {:ok, Map.merge(estado, %{tab: tab, monitores: %{}})}
  end

  @impl true
  def terminate(_motivo, st) do
    :dets.close(st.tab)
  end

  defp carregar(tab) do
    vazio = %{livros: %{}, emprestimos: %{}, proximo_id: 1}

    :dets.foldl(
      fn
        {{:livro, id}, livro}, acc -> %{acc | livros: Map.put(acc.livros, id, livro)}
        {{:emprestimo, id}, emp}, acc -> %{acc | emprestimos: Map.put(acc.emprestimos, id, emp)}
        {:meta, proximo}, acc -> %{acc | proximo_id: proximo}
      end,
      vazio,
      tab
    )
  end

  # ---------------------------------------------------------------------------
  # Callbacks de chamadas síncronas
  # ---------------------------------------------------------------------------

  @impl true
  def handle_call({:cadastrar_livro, attrs}, _from, st) do
    case validar_livro(attrs) do
      {:ok, dados} ->
        {id, st} = proximo_id(st)
        livro = Map.put(dados, :id, id)
        st = %{st | livros: Map.put(st.livros, id, livro)}
        {:reply, {:ok, com_disponibilidade(st, livro)}, persistir(st, [{{:livro, id}, livro}])}

      erro ->
        {:reply, erro, st}
    end
  end

  def handle_call({:consultar_livro, id}, _from, st) do
    resposta =
      with {:ok, livro} <- buscar(st.livros, id, :livro_nao_encontrado) do
        {:ok, com_disponibilidade(st, livro)}
      end

    {:reply, resposta, st}
  end

  def handle_call(:listar_livros, _from, st) do
    livros =
      st.livros
      |> Map.values()
      |> Enum.sort_by(& &1.id)
      |> Enum.map(&com_disponibilidade(st, &1))

    {:reply, {:ok, livros}, st}
  end

  def handle_call({:atualizar_livro, id, attrs}, _from, st) do
    campos = if is_map(attrs), do: Map.take(attrs, [:titulo, :autor, :exemplares]), else: %{}

    with {:ok, atual} <- buscar(st.livros, id, :livro_nao_encontrado),
         {:ok, dados} <- validar_livro(Map.merge(atual, campos)),
         :ok <- nao_reduz_abaixo_do_emprestado(st, id, dados.exemplares) do
      livro = Map.put(dados, :id, id)
      st = %{st | livros: Map.put(st.livros, id, livro)}
      {:reply, {:ok, com_disponibilidade(st, livro)}, persistir(st, [{{:livro, id}, livro}])}
    else
      erro -> {:reply, erro, st}
    end
  end

  def handle_call({:remover_livro, id}, _from, st) do
    with {:ok, _} <- buscar(st.livros, id, :livro_nao_encontrado),
         :ok <- if(emprestados(st, id) == 0, do: :ok, else: {:error, :livro_emprestado}) do
      # Remove o livro e seu histórico (todos os empréstimos já encerrados).
      historico = for {eid, %{livro_id: lid}} <- st.emprestimos, lid == id, do: eid

      st = %{
        st
        | livros: Map.delete(st.livros, id),
          emprestimos: Map.drop(st.emprestimos, historico)
      }

      apagar = [{:livro, id} | Enum.map(historico, &{:emprestimo, &1})]
      {:reply, :ok, persistir(st, [], apagar)}
    else
      erro -> {:reply, erro, st}
    end
  end

  def handle_call({:emprestar, livro_id, leitor, pid, opts}, _from, st) do
    # Verificação e registro ocorrem na mesma mensagem: sem corrida entre leitores.
    with :ok <- validar_leitor(leitor),
         {:ok, livro} <- buscar(st.livros, livro_id, :livro_nao_encontrado),
         :ok <- verificar_disponibilidade(st, livro) do
      {id, st} = proximo_id(st)
      agora = DateTime.utc_now()
      dias = Keyword.get(opts, :dias, @dias_padrao)

      emp = %{
        id: id,
        livro_id: livro_id,
        leitor: leitor,
        status: :ativo,
        emprestado_em: agora,
        devolucao_prevista: DateTime.add(agora, dias * 86_400, :second),
        encerrado_em: nil
      }

      # Detecta encerramento inesperado do processo que pegou o livro.
      ref = Process.monitor(pid)

      st = %{
        st
        | emprestimos: Map.put(st.emprestimos, id, emp),
          monitores: Map.put(st.monitores, ref, id)
      }

      {:reply, {:ok, emp}, persistir(st, [{{:emprestimo, id}, emp}])}
    else
      erro -> {:reply, erro, st}
    end
  end

  def handle_call({:devolver, id}, _from, st) do
    case Map.fetch(st.emprestimos, id) do
      {:ok, %{status: :ativo} = emp} ->
        {emp, st} = encerrar(st, emp, :devolvido)
        {:reply, {:ok, emp}, st}

      {:ok, _} ->
        {:reply, {:error, :ja_encerrado}, st}

      :error ->
        {:reply, {:error, :emprestimo_nao_encontrado}, st}
    end
  end

  def handle_call({:consultar_emprestimo, id}, _from, st) do
    resposta =
      with {:ok, emp} <- buscar(st.emprestimos, id, :emprestimo_nao_encontrado) do
        {:ok, emp}
      end

    {:reply, resposta, st}
  end

  def handle_call(:listar_emprestimos, _from, st) do
    emprestimos = st.emprestimos |> Map.values() |> Enum.sort_by(& &1.id)
    {:reply, {:ok, emprestimos}, st}
  end

  # ---------------------------------------------------------------------------
  # Callbacks de mensagens assíncronas
  # ---------------------------------------------------------------------------

  @impl true
  def handle_info({:DOWN, ref, :process, _pid, motivo}, st) do
    case Map.pop(st.monitores, ref) do
      {nil, _} ->
        {:noreply, st}

      {emp_id, monitores} ->
        st = %{st | monitores: monitores}

        case st.emprestimos[emp_id] do
          %{status: :ativo} = emp ->
            Logger.warning(
              "Leitor terminou inesperadamente (#{inspect(motivo)}); liberando empréstimo #{emp_id}"
            )

            {_emp, st} = encerrar(st, emp, :liberado_por_falha)
            {:noreply, st}

          _ ->
            {:noreply, st}
        end
    end
  end

  # Expira empréstimos vencidos que não possuem mais responsável monitorado
  # (ex.: restaurados após uma reinicialização do Acervo).
  def handle_info(:expirar, st) do
    agora = DateTime.utc_now()
    monitorados = MapSet.new(Map.values(st.monitores))

    st =
      Enum.reduce(st.emprestimos, st, fn {id, emp}, acc ->
        vencido? = DateTime.compare(agora, emp.devolucao_prevista) == :gt

        if emp.status == :ativo and vencido? and not MapSet.member?(monitorados, id) do
          {_emp, acc} = encerrar(acc, emp, :expirado)
          acc
        else
          acc
        end
      end)

    Process.send_after(self(), :expirar, @intervalo_expiracao)
    {:noreply, st}
  end

  # ---------------------------------------------------------------------------
  # Funções auxiliares (regras de negócio e persistência)
  # ---------------------------------------------------------------------------

  defp validar_livro(%{titulo: t, autor: a, exemplares: e})
       when is_binary(t) and is_binary(a) and is_integer(e) and e >= 0,
       do: {:ok, %{titulo: t, autor: a, exemplares: e}}

  defp validar_livro(_), do: {:error, :dados_invalidos}

  defp validar_leitor(leitor) when is_binary(leitor) and leitor != "", do: :ok
  defp validar_leitor(_), do: {:error, :leitor_invalido}

  defp buscar(mapa, chave, erro) do
    case Map.fetch(mapa, chave) do
      {:ok, valor} -> {:ok, valor}
      :error -> {:error, erro}
    end
  end

  defp emprestados(st, livro_id) do
    Enum.count(st.emprestimos, fn {_, e} -> e.livro_id == livro_id and e.status == :ativo end)
  end

  defp verificar_disponibilidade(st, livro) do
    if emprestados(st, livro.id) < livro.exemplares,
      do: :ok,
      else: {:error, :sem_exemplares}
  end

  defp nao_reduz_abaixo_do_emprestado(st, livro_id, exemplares) do
    if exemplares >= emprestados(st, livro_id),
      do: :ok,
      else: {:error, :exemplares_em_uso}
  end

  defp com_disponibilidade(st, livro) do
    Map.put(livro, :disponiveis, livro.exemplares - emprestados(st, livro.id))
  end

  defp proximo_id(st), do: {st.proximo_id, %{st | proximo_id: st.proximo_id + 1}}

  # Encerra um empréstimo (devolução ou liberação por falha) e persiste a mudança.
  defp encerrar(st, emp, status) do
    st = desmonitorar(st, emp.id)
    emp = %{emp | status: status, encerrado_em: DateTime.utc_now()}
    st = %{st | emprestimos: Map.put(st.emprestimos, emp.id, emp)}
    {emp, persistir(st, [{{:emprestimo, emp.id}, emp}])}
  end

  defp desmonitorar(st, emp_id) do
    case Enum.find(st.monitores, fn {_ref, id} -> id == emp_id end) do
      {ref, _} ->
        Process.demonitor(ref, [:flush])
        %{st | monitores: Map.delete(st.monitores, ref)}

      nil ->
        st
    end
  end

  # Grava as alterações no DETS e força a sincronização em disco.
  defp persistir(st, gravar \\ [], apagar \\ []) do
    :ok = :dets.insert(st.tab, [{:meta, st.proximo_id} | gravar])
    Enum.each(apagar, fn chave -> :ok = :dets.delete(st.tab, chave) end)
    :ok = :dets.sync(st.tab)
    st
  end
end
