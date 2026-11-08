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
#
# Uso básico:
#   {:ok, _} = Biblioteca.Emprestimos.start_link(arquivo: "biblioteca", intervalo: 60_000)
#   Biblioteca.Emprestimos.assinar()
#   {:ok, id} = Biblioteca.Emprestimos.cadastrar("Dom Casmurro", "Ana")
#   {:ok, emp} = Biblioteca.Emprestimos.consultar(id)
#   {:ok, _} = Biblioteca.Emprestimos.atualizar(id, %{status: :devolvido})
#   :ok = Biblioteca.Emprestimos.remover(id)
#   Biblioteca.Emprestimos.historico(10)
#
# Processos assinantes recebem mensagens {:emprestimos, operacao, detalhes}.

defmodule Biblioteca.Emprestimos do
  @moduledoc """
  Gerenciador de empréstimos de livros.

  * Comunicação entre processos: API síncrona via GenServer.call e
    notificações assíncronas enviadas aos processos assinantes.
  * Operações periódicas: verificação automática de empréstimos atrasados.
  * Histórico: toda operação é registrada em uma tabela DETS ordenada.
  * CRUD: cadastrar, consultar, atualizar e remover empréstimos.
  * Persistência: três tabelas DETS (empréstimos, histórico e metadados).
  """

  use GenServer

  @tabela_emprestimos :biblioteca_emprestimos
  @tabela_historico :biblioteca_historico
  @tabela_meta :biblioteca_meta

  @status_validos [:emprestado, :atrasado, :devolvido]
  @campos_atualizaveis [:titulo, :usuario, :data_prevista, :status]
  @prazo_padrao_dias 14
  @intervalo_padrao :timer.minutes(1)

  # ---------------------------------------------------------------------------
  # API pública
  # ---------------------------------------------------------------------------

  @doc "Inicia o processo gerenciador. Opções: :name, :arquivo e :intervalo (ms)."
  def start_link(opts \\ []) do
    nome = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: nome)
  end

  @doc "Cadastra um novo empréstimo e retorna {:ok, id}."
  def cadastrar(titulo, usuario, dias \\ @prazo_padrao_dias)
      when is_binary(titulo) and is_binary(usuario) and is_integer(dias) and dias > 0 do
    GenServer.call(__MODULE__, {:cadastrar, titulo, usuario, dias})
  end

  @doc "Consulta um empréstimo pelo id."
  def consultar(id), do: GenServer.call(__MODULE__, {:consultar, id})

  @doc "Lista empréstimos, opcionalmente filtrados por status."
  def listar(status \\ nil), do: GenServer.call(__MODULE__, {:listar, status})

  @doc "Atualiza campos permitidos de um empréstimo."
  def atualizar(id, mudancas) when is_map(mudancas) do
    GenServer.call(__MODULE__, {:atualizar, id, mudancas})
  end

  @doc "Remove um empréstimo pelo id."
  def remover(id), do: GenServer.call(__MODULE__, {:remover, id})

  @doc "Retorna as operações mais recentes do histórico."
  def historico(limite \\ 20), do: GenServer.call(__MODULE__, {:historico, limite})

  @doc "Registra um processo para receber notificações de operações."
  def assinar(pid \\ self()), do: GenServer.call(__MODULE__, {:assinar, pid})

  @doc "Cancela a assinatura de notificações."
  def cancelar_assinatura(pid \\ self()), do: GenServer.call(__MODULE__, {:cancelar, pid})

  # ---------------------------------------------------------------------------
  # Callbacks do GenServer
  # ---------------------------------------------------------------------------

  @impl true
  def init(opts) do
    # Permite que terminate/2 feche as tabelas DETS em desligamentos controlados.
    Process.flag(:trap_exit, true)

    base = Keyword.get(opts, :arquivo, "biblioteca")
    intervalo = Keyword.get(opts, :intervalo, @intervalo_padrao)

    with :ok <- abrir(@tabela_emprestimos, base, :set),
         :ok <- abrir(@tabela_historico, base, :ordered_set),
         :ok <- abrir(@tabela_meta, base, :set) do
      :dets.insert_new(@tabela_meta, {:proximo_id, 1})
      agendar_verificacao(intervalo)
      {:ok, %{intervalo: intervalo, assinantes: %{}}}
    else
      {:error, motivo} -> {:stop, motivo}
    end
  end

  @impl true
  def handle_call({:cadastrar, titulo, usuario, dias}, _from, estado) do
    id = proximo_id()
    hoje = Date.utc_today()

    emprestimo = %{
      id: id,
      titulo: titulo,
      usuario: usuario,
      data_emprestimo: hoje,
      data_prevista: Date.add(hoje, dias),
      data_devolucao: nil,
      status: :emprestado
    }

    :ok = :dets.insert(@tabela_emprestimos, {id, emprestimo})
    registrar(estado, :cadastrar, %{id: id, titulo: titulo, usuario: usuario})
    {:reply, {:ok, id}, estado}
  end

  def handle_call({:consultar, id}, _from, estado) do
    {:reply, buscar(id), estado}
  end

  def handle_call({:listar, status}, _from, estado) do
    lista =
      :dets.foldl(
        fn {_id, emp}, acc ->
          if is_nil(status) or emp.status == status, do: [emp | acc], else: acc
        end,
        [],
        @tabela_emprestimos
      )

    {:reply, Enum.sort_by(lista, & &1.id), estado}
  end

  def handle_call({:atualizar, id, mudancas}, _from, estado) do
    with {:ok, emp} <- buscar(id),
         {:ok, validas} <- validar_mudancas(mudancas) do
      atualizado = Map.merge(emp, validas)
      :ok = :dets.insert(@tabela_emprestimos, {id, atualizado})
      registrar(estado, :atualizar, %{id: id, mudancas: validas})
      {:reply, {:ok, atualizado}, estado}
    else
      erro -> {:reply, erro, estado}
    end
  end

  def handle_call({:remover, id}, _from, estado) do
    case buscar(id) do
      {:ok, _emp} ->
        :ok = :dets.delete(@tabela_emprestimos, id)
        registrar(estado, :remover, %{id: id})
        {:reply, :ok, estado}

      erro ->
        {:reply, erro, estado}
    end
  end

  def handle_call({:historico, limite}, _from, estado) do
    # Chaves ordenadas ascendentemente; ao acumular com prepend, a lista fica decrescente.
    entradas =
      :dets.foldl(fn {_chave, entrada}, acc -> [entrada | acc] end, [], @tabela_historico)

    {:reply, Enum.take(entradas, limite), estado}
  end

  def handle_call({:assinar, pid}, _from, estado) do
    estado =
      if Map.has_key?(estado.assinantes, pid) do
        estado
      else
        ref = Process.monitor(pid)
        %{estado | assinantes: Map.put(estado.assinantes, pid, ref)}
      end

    {:reply, :ok, estado}
  end

  def handle_call({:cancelar, pid}, _from, estado) do
    case Map.fetch(estado.assinantes, pid) do
      {:ok, ref} -> Process.demonitor(ref, [:flush])
      :error -> :ok
    end

    {:reply, :ok, %{estado | assinantes: Map.delete(estado.assinantes, pid)}}
  end

  @impl true
  def handle_info(:verificar_atrasos, estado) do
    # Operação periódica: marca como atrasados os empréstimos vencidos.
    hoje = Date.utc_today()

    vencidos =
      :dets.foldl(
        fn {id, emp}, acc ->
          if emp.status == :emprestado and Date.compare(emp.data_prevista, hoje) == :lt,
            do: [id | acc],
            else: acc
        end,
        [],
        @tabela_emprestimos
      )

    Enum.each(vencidos, fn id ->
      {:ok, emp} = buscar(id)
      :ok = :dets.insert(@tabela_emprestimos, {id, %{emp | status: :atrasado}})
      registrar(estado, :marcar_atrasado, %{id: id, data_prevista: emp.data_prevista})
    end)

    agendar_verificacao(estado.intervalo)
    {:noreply, estado}
  end

  def handle_info({:DOWN, _ref, :process, pid, _motivo}, estado) do
    # Remove assinantes que terminaram.
    {:noreply, %{estado | assinantes: Map.delete(estado.assinantes, pid)}}
  end

  def handle_info(_msg, estado), do: {:noreply, estado}

  @impl true
  def terminate(_motivo, _estado) do
    Enum.each([@tabela_emprestimos, @tabela_historico, @tabela_meta], &:dets.close/1)
    :ok
  end

  # ---------------------------------------------------------------------------
  # Funções privadas
  # ---------------------------------------------------------------------------

  defp abrir(nome, base, tipo) do
    arquivo = String.to_charlist("#{base}_#{nome}.dets")

    case :dets.open_file(nome, file: arquivo, type: tipo) do
      {:ok, ^nome} -> :ok
      {:error, motivo} -> {:error, motivo}
    end
  end

  defp agendar_verificacao(intervalo) do
    Process.send_after(self(), :verificar_atrasos, intervalo)
  end

  defp proximo_id do
    [{:proximo_id, id}] = :dets.lookup(@tabela_meta, :proximo_id)
    :ok = :dets.insert(@tabela_meta, {:proximo_id, id + 1})
    id
  end

  defp buscar(id) do
    case :dets.lookup(@tabela_emprestimos, id) do
      [{^id, emp}] -> {:ok, emp}
      [] -> {:error, :nao_encontrado}
    end
  end

  defp validar_mudancas(mudancas) do
    invalidos = Map.keys(mudancas) -- @campos_atualizaveis

    cond do
      invalidos != [] ->
        {:error, {:campos_invalidos, invalidos}}

      Map.has_key?(mudancas, :status) and mudancas.status not in @status_validos ->
        {:error, :status_invalido}

      mudancas[:status] == :devolvido ->
        {:ok, Map.put_new(mudancas, :data_devolucao, Date.utc_today())}

      true ->
        {:ok, mudancas}
    end
  end

  # Grava no histórico e notifica os processos assinantes.
  defp registrar(estado, operacao, detalhes) do
    chave = {System.os_time(:microsecond), System.unique_integer([:positive, :monotonic])}

    entrada = %{operacao: operacao, detalhes: detalhes, em: DateTime.utc_now()}
    :ok = :dets.insert(@tabela_historico, {chave, entrada})

    Enum.each(estado.assinantes, fn {pid, _ref} ->
      send(pid, {:emprestimos, operacao, detalhes})
    end)

    :ok
  end
end
