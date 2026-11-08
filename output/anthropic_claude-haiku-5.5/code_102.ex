# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de empréstimos de livros, atendendo somente aos seguintes requisitos:
#
# 1. Permitir comunicação entre diferentes processos.
# 2. Executar determinadas operações periodicamente.
# 3. Manter um histórico das operações realizadas.
# 4. Permitir cadastrar, consultar, atualizar e remover registros.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - DETS
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# Gerenciamento de empréstimos de livros com GenServer e DETS.
# Uso: {:ok, _pid} = Biblioteca.Emprestimos.start_link(diretorio: "dados", intervalo_ms: 60_000)

defmodule Biblioteca.Emprestimos do
  use GenServer

  # Prazo padrão de devolução, em dias
  @prazo_dias 14
  # Intervalo padrão da tarefa periódica (verificação de atrasos e sync do DETS)
  @intervalo_padrao_ms 60_000

  ## ---------------------------------------------------------------
  ## API pública (comunicação com o processo via mensagens)
  ## ---------------------------------------------------------------

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  # CRUD de livros
  def cadastrar_livro(titulo, autor),
    do: GenServer.call(__MODULE__, {:cadastrar_livro, titulo, autor})

  def consultar_livro(id),
    do: GenServer.call(__MODULE__, {:consultar_livro, id})

  def listar_livros,
    do: GenServer.call(__MODULE__, :listar_livros)

  def atualizar_livro(id, attrs),
    do: GenServer.call(__MODULE__, {:atualizar_livro, id, attrs})

  def remover_livro(id),
    do: GenServer.call(__MODULE__, {:remover_livro, id})

  # Empréstimos
  def emprestar(livro_id, usuario),
    do: GenServer.call(__MODULE__, {:emprestar, livro_id, usuario})

  def devolver(emprestimo_id),
    do: GenServer.call(__MODULE__, {:devolver, emprestimo_id})

  def consultar_emprestimo(id),
    do: GenServer.call(__MODULE__, {:consultar_emprestimo, id})

  def listar_emprestimos,
    do: GenServer.call(__MODULE__, :listar_emprestimos)

  # Histórico de operações (mais recentes primeiro)
  def historico(limite \\ 50),
    do: GenServer.call(__MODULE__, {:historico, limite})

  # Assinatura de eventos: o processo chamador passa a receber
  # mensagens {:biblioteca, tipo, registro} a cada operação realizada
  def assinar,
    do: GenServer.call(__MODULE__, :assinar)

  def cancelar_assinatura,
    do: GenServer.call(__MODULE__, :cancelar_assinatura)

  ## ---------------------------------------------------------------
  ## Callbacks do GenServer
  ## ---------------------------------------------------------------

  @impl true
  def init(opts) do
    # Permite executar terminate/2 e fechar as tabelas DETS corretamente
    Process.flag(:trap_exit, true)

    dir = Keyword.get(opts, :diretorio, "dados")
    intervalo = Keyword.get(opts, :intervalo_ms, @intervalo_padrao_ms)
    File.mkdir_p!(dir)

    with {:ok, livros} <- abrir(dir, :livros),
         {:ok, emprestimos} <- abrir(dir, :emprestimos),
         {:ok, historico} <- abrir(dir, :historico) do
      estado = %{
        livros: livros,
        emprestimos: emprestimos,
        historico: historico,
        # Contadores recuperados do próprio arquivo, mantendo IDs únicos após reinício
        prox_livro: proximo_id(livros),
        prox_emprestimo: proximo_id(emprestimos),
        prox_historico: proximo_id(historico),
        assinantes: MapSet.new(),
        intervalo: intervalo
      }

      agendar(intervalo)
      {:ok, estado}
    else
      {:error, motivo} -> {:stop, motivo}
    end
  end

  # --- Livros ---

  @impl true
  def handle_call({:cadastrar_livro, titulo, autor}, _from, e) do
    id = e.prox_livro
    livro = %{id: id, titulo: titulo, autor: autor, status: :disponivel}
    :ok = :dets.insert(e.livros, {id, livro})

    e =
      %{e | prox_livro: id + 1}
      |> registrar(:livro_cadastrado, %{id: id})

    {:reply, {:ok, livro}, e}
  end

  def handle_call({:consultar_livro, id}, _from, e) do
    {:reply, buscar(e.livros, id), e}
  end

  def handle_call(:listar_livros, _from, e) do
    {:reply, listar(e.livros), e}
  end

  def handle_call({:atualizar_livro, id, attrs}, _from, e) do
    case buscar(e.livros, id) do
      {:ok, livro} ->
        atualizado = Map.merge(livro, Map.take(attrs, @campos_livro))
        :ok = :dets.insert(e.livros, {id, atualizado})
        e = registrar(e, :livro_atualizado, %{id: id})
        {:reply, {:ok, atualizado}, e}

      erro ->
        {:reply, erro, e}
    end
  end

  def handle_call({:remover_livro, id}, _from, e) do
    case buscar(e.livros, id) do
      # Não remove livro que está emprestado
      {:ok, %{status: :emprestado}} ->
        {:reply, {:error, :emprestado}, e}

      {:ok, _livro} ->
        :ok = :dets.delete(e.livros, id)
        e = registrar(e, :livro_removido, %{id: id})
        {:reply, :ok, e}

      erro ->
        {:reply, erro, e}
    end
  end

  # --- Empréstimos ---

  def handle_call({:emprestar, livro_id, usuario}, _from, e) do
    with {:ok, livro} <- buscar(e.livros, livro_id),
         :ok <- disponivel?(livro) do
      hoje = Date.utc_today()
      id = e.prox_emprestimo

      emprestimo = %{
        id: id,
        livro_id: livro_id,
        usuario: usuario,
        data_emprestimo: hoje,
        prazo: Date.add(hoje, @prazo_dias),
        data_devolucao: nil,
        status: :ativo
      }

      :ok = :dets.insert(e.emprestimos, {id, emprestimo})
      :ok = :dets.insert(e.livros, {livro_id, %{livro | status: :emprestado}})

      e =
        %{e | prox_emprestimo: id + 1}
        |> registrar(:emprestimo_realizado, %{id: id, livro_id: livro_id, usuario: usuario})

      {:reply, {:ok, emprestimo}, e}
    else
      erro -> {:reply, erro, e}
    end
  end

  def handle_call({:devolver, emp_id}, _from, e) do
    case buscar(e.emprestimos, emp_id) do
      {:ok, %{status: :devolvido}} ->
        {:reply, {:error, :ja_devolvido}, e}

      {:ok, emp} ->
        {:ok, livro} = buscar(e.livros, emp.livro_id)
        atualizado = %{emp | status: :devolvido, data_devolucao: Date.utc_today()}

        :ok = :dets.insert(e.emprestimos, {emp_id, atualizado})
        :ok = :dets.insert(e.livros, {livro.id, %{livro | status: :disponivel}})

        e = registrar(e, :devolucao_realizada, %{id: emp_id, livro_id: livro.id})
        {:reply, {:ok, atualizado}, e}

      erro ->
        {:reply, erro, e}
    end
  end

  def handle_call({:consultar_emprestimo, id}, _from, e) do
    {:reply, buscar(e.emprestimos, id), e}
  end

  def handle_call(:listar_emprestimos, _from, e) do
    {:reply, listar(e.emprestimos), e}
  end

  # --- Histórico ---

  def handle_call({:historico, limite}, _from, e) do
    recentes =
      e.historico
      |> listar()
      |> Enum.reverse()
      |> Enum.take(limite)

    {:reply, recentes, e}
  end

  # --- Assinaturas (comunicação entre processos) ---

  def handle_call(:assinar, {pid, _tag}, e) do
    Process.monitor(pid)
    {:reply, :ok, %{e | assinantes: MapSet.put(e.assinantes, pid)}}
  end

  def handle_call(:cancelar_assinatura, {pid, _tag}, e) do
    {:reply, :ok, %{e | assinantes: MapSet.delete(e.assinantes, pid)}}
  end

  # --- Tarefa periódica: marca empréstimos atrasados e sincroniza o DETS ---

  @impl true
  def handle_info(:verificar_atrasos, e) do
    hoje = Date.utc_today()

    atrasados =
      :dets.foldl(
        fn
          {_, %{status: :ativo, prazo: prazo} = emp}, acc ->
            if Date.compare(prazo, hoje) == :lt, do: [emp | acc], else: acc

          _, acc ->
            acc
        end,
        [],
        e.emprestimos
      )

    e =
      Enum.reduce(atrasados, e, fn emp, acc ->
        :ok = :dets.insert(acc.emprestimos, {emp.id, %{emp | status: :atrasado}})
        registrar(acc, :emprestimo_atrasado, %{id: emp.id, usuario: emp.usuario})
      end)

    Enum.each([e.livros, e.emprestimos, e.historico], &:dets.sync/1)

    agendar(e.intervalo)
    {:noreply, e}
  end

  # Remove assinante que terminou
  def handle_info({:DOWN, _ref, :process, pid, _motivo}, e) do
    {:noreply, %{e | assinantes: MapSet.delete(e.assinantes, pid)}}
  end

  def handle_info(_msg, e), do: {:noreply, e}

  @impl true
  def terminate(_motivo, e) do
    Enum.each([e.livros, e.emprestimos, e.historico], &:dets.close/1)
    :ok
  end

  ## ---------------------------------------------------------------
  ## Funções auxiliares privadas
  ## ---------------------------------------------------------------

  # Abre uma tabela DETS em arquivo dentro do diretório informado
  defp abrir(dir, nome) do
    arquivo = dir |> Path.join("#{nome}.dets") |> String.to_charlist()
    :dets.open_file(:"biblioteca_#{nome}", file: arquivo, type: :set)
  end

  # Calcula o próximo ID a partir da maior chave existente
  defp proximo_id(tabela) do
    :dets.foldl(fn {id, _}, max -> max(id, max) end, 0, tabela) + 1
  end

  defp buscar(tabela, id) do
    case :dets.lookup(tabela, id) do
      [{^id, valor}] -> {:ok, valor}
      [] -> {:error, :nao_encontrado}
    end
  end

  # Retorna todos os valores da tabela ordenados por ID
  defp listar(tabela) do
    :dets.foldl(fn {_, valor}, acc -> [valor | acc] end, [], tabela)
    |> Enum.sort_by(& &1.id)
  end

  defp disponivel?(%{status: :disponivel}), do: :ok
  defp disponivel?(_livro), do: {:error, :indisponivel}

  # Registra uma operação no histórico e notifica os assinantes
  defp registrar(e, tipo, detalhes) do
    id = e.prox_historico

    registro = %{
      id: id,
      em: DateTime.utc_now(),
      tipo: tipo,
      detalhes: detalhes
    }

    :ok = :dets.insert(e.historico, {id, registro})
    Enum.each(e.assinantes, &send(&1, {:biblioteca, tipo, registro}))

    %{e | prox_historico: id + 1}
  end

  # Agenda a próxima execução da tarefa periódica
  defp agendar(intervalo) do
    Process.send_after(self(), :verificar_atrasos, intervalo)
  end
end
