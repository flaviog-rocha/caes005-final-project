# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de empréstimos de livros, atendendo somente aos seguintes requisitos:
#
# 1. Diferenciar erros de validação de falhas operacionais.
# 2. Executar determinadas operações periodicamente.
# 3. Persistir os dados para recuperação após reinicializações.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule Biblioteca.Emprestimos do
  @moduledoc """
  Gerenciamento de empréstimos de livros.

  Classificação de erros:

    * `{:error, {:validation, motivo}}` - entrada inválida ou violação de regra de
      negócio. É responsabilidade do chamador corrigir a requisição antes de repeti-la.

    * `{:error, {:operational, motivo}}` - falha do sistema (I/O, arquivo corrompido,
      servidor indisponível, falha de persistência). Pode ser reintentada.

  Tarefas periódicas:

    * A cada `:intervalo_verificacao` milissegundos, empréstimos vencidos são marcados
      como `:atrasado`.

  Persistência:

    * Cada mutação é gravada de forma atômica (escrita em arquivo temporário seguida de
      renomeação) antes de ser confirmada ao chamador. Se a gravação falhar, o estado em
      memória não é alterado.

    * Na inicialização, o estado é recarregado do arquivo.
  """

  use GenServer
  require Logger

  @versao_schema 1
  @max_emprestimos_por_membro 3
  @dias_min 1
  @dias_max 30
  @segundos_por_dia 86_400
  @intervalo_verificacao_padrao :timer.minutes(10)
  @timeout_chamada 5_000
  @arquivo_padrao "biblioteca.dat"

  # ---------------------------------------------------------------------------
  # API pública
  # ---------------------------------------------------------------------------

  @doc """
  Inicia o servidor.

  Opções:
    * `:arquivo` - caminho do arquivo de persistência (padrão: `"biblioteca.dat"`)
    * `:intervalo_verificacao` - intervalo em ms das tarefas periódicas
    * `:name` - nome do processo registrado (padrão: módulo)
  """
  def start_link(opts \\ []) do
    nome = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: nome)
  end

  def cadastrar_livro(titulo, isbn, servidor \\ __MODULE__),
    do: chamar(servidor, {:cadastrar_livro, titulo, isbn})

  def cadastrar_membro(nome, servidor \\ __MODULE__),
    do: chamar(servidor, {:cadastrar_membro, nome})

  def emprestar(livro_id, membro_id, dias, servidor \\ __MODULE__),
    do: chamar(servidor, {:emprestar, livro_id, membro_id, dias})

  def devolver(emprestimo_id, servidor \\ __MODULE__),
    do: chamar(servidor, {:devolver, emprestimo_id})

  def listar_atrasados(servidor \\ __MODULE__),
    do: chamar(servidor, :listar_atrasados)

  # Executa a chamada convertendo falhas de processo em erro operacional.
  defp chamar(servidor, mensagem) do
    GenServer.call(servidor, mensagem, @timeout_chamada)
  catch
    :exit, motivo -> {:error, {:operational, {:servidor_indisponivel, motivo}}}
  end

  # ---------------------------------------------------------------------------
  # Callbacks do GenServer
  # ---------------------------------------------------------------------------

  @impl true
  def init(opts) do
    caminho = Keyword.get(opts, :arquivo, @arquivo_padrao)
    intervalo = Keyword.get(opts, :intervalo_verificacao, @intervalo_verificacao_padrao)

    case carregar(caminho) do
      {:ok, dados} ->
        estado = Map.merge(dados, %{caminho: caminho, intervalo: intervalo})
        agendar(intervalo)
        {:ok, estado}

      {:error, motivo} ->
        Logger.error("não foi possível carregar o estado: #{inspect(motivo)}")
        {:stop, motivo}
    end
  end

  @impl true
  def handle_call({:cadastrar_livro, titulo, isbn}, _from, estado),
    do: executar(estado, &cadastrar_livro_puro(&1, titulo, isbn))

  def handle_call({:cadastrar_membro, nome}, _from, estado),
    do: executar(estado, &cadastrar_membro_puro(&1, nome))

  def handle_call({:emprestar, livro_id, membro_id, dias}, _from, estado),
    do: executar(estado, &emprestar_puro(&1, livro_id, membro_id, dias))

  def handle_call({:devolver, emprestimo_id}, _from, estado),
    do: executar(estado, &devolver_puro(&1, emprestimo_id))

  def handle_call(:listar_atrasados, _from, estado) do
    atrasados =
      estado.emprestimos
      |> Map.values()
      |> Enum.filter(&(&1.status == :atrasado))

    {:reply, {:ok, atrasados}, estado}
  end

  # Tarefa periódica: marca empréstimos vencidos como atrasados.
  @impl true
  def handle_info(:verificar_atrasos, estado) do
    {atualizado, quantidade} = marcar_atrasos(estado, agora())

    novo_estado =
      if quantidade == 0 do
        estado
      else
        case persistir(atualizado) do
          :ok ->
            Logger.warning("#{quantidade} empréstimo(s) marcado(s) como atrasado(s)")
            atualizado

          {:error, motivo} ->
            # Falha operacional: mantém o estado anterior; a próxima execução tenta novamente.
            Logger.error("falha ao persistir verificação de atrasos: #{inspect(motivo)}")
            estado
        end
      end

    agendar(estado.intervalo)
    {:noreply, novo_estado}
  end

  def handle_info(_mensagem, estado), do: {:noreply, estado}

  # ---------------------------------------------------------------------------
  # Execução de operações com persistência
  # ---------------------------------------------------------------------------

  # Executa uma operação pura. Só confirma o novo estado após persistência bem-sucedida.
  defp executar(estado, operacao) do
    case operacao.(estado) do
      {:ok, novo, valor} -> confirmar(estado, novo, valor)
      {:error, _} = erro -> {:reply, erro, estado}
    end
  end

  defp confirmar(antigo, novo, valor) do
    case persistir(novo) do
      :ok ->
        {:reply, {:ok, valor}, novo}

      {:error, motivo} ->
        Logger.error("falha ao persistir estado: #{inspect(motivo)}")
        {:reply, {:error, {:operational, {:persistencia, motivo}}}, antigo}
    end
  end

  # ---------------------------------------------------------------------------
  # Operações puras de domínio
  # Retornam {:ok, novo_estado, valor} ou {:error, {:validation, motivo}}
  # ---------------------------------------------------------------------------

  defp cadastrar_livro_puro(estado, titulo, isbn) do
    with :ok <- validar_texto(titulo, :titulo_invalido),
         {:ok, isbn_limpo} <- validar_isbn(isbn),
         :ok <- validar_isbn_unico(estado, isbn_limpo) do
      {id, estado} = proximo_id(estado)
      livro = %{id: id, titulo: String.trim(titulo), isbn: isbn_limpo}
      {:ok, %{estado | livros: Map.put(estado.livros, id, livro)}, id}
    end
  end

  defp cadastrar_membro_puro(estado, nome) do
    with :ok <- validar_texto(nome, :nome_invalido) do
      {id, estado} = proximo_id(estado)
      membro = %{id: id, nome: String.trim(nome)}
      {:ok, %{estado | membros: Map.put(estado.membros, id, membro)}, id}
    end
  end

  defp emprestar_puro(estado, livro_id, membro_id, dias) do
    with :ok <- validar_prazo(dias),
         {:ok, _livro} <- buscar(estado.livros, livro_id, :livro_inexistente),
         {:ok, _membro} <- buscar(estado.membros, membro_id, :membro_inexistente),
         :ok <- validar_disponibilidade(estado, livro_id),
         :ok <- validar_limite(estado, membro_id) do
      agora = agora()
      {id, estado} = proximo_id(estado)

      emprestimo = %{
        id: id,
        livro_id: livro_id,
        membro_id: membro_id,
        emprestado_em: agora,
        vence_em: agora + dias * @segundos_por_dia,
        devolvido_em: nil,
        status: :ativo
      }

      {:ok, %{estado | emprestimos: Map.put(estado.emprestimos, id, emprestimo)}, id}
    end
  end

  defp devolver_puro(estado, emprestimo_id) do
    with {:ok, emprestimo} <- buscar(estado.emprestimos, emprestimo_id, :emprestimo_inexistente),
         :ok <- validar_pendente(emprestimo) do
      atualizado = %{emprestimo | status: :devolvido, devolvido_em: agora()}
      emprestimos = Map.put(estado.emprestimos, emprestimo_id, atualizado)
      {:ok, %{estado | emprestimos: emprestimos}, atualizado}
    end
  end

  # ---------------------------------------------------------------------------
  # Validações (erros de validação)
  # ---------------------------------------------------------------------------

  defp validar_texto(valor, motivo) do
    if is_binary(valor) and String.trim(valor) != "" do
      :ok
    else
      {:error, {:validation, motivo}}
    end
  end

  defp validar_isbn(isbn) do
    limpo = if is_binary(isbn), do: String.replace(isbn, "-", ""), else: ""

    if Regex.match?(~r/\A(\d{10}|\d{13})\z/, limpo) do
      {:ok, limpo}
    else
      {:error, {:validation, :isbn_invalido}}
    end
  end

  defp validar_isbn_unico(estado, isbn) do
    if Enum.any?(Map.values(estado.livros), &(&1.isbn == isbn)) do
      {:error, {:validation, :isbn_duplicado}}
    else
      :ok
    end
  end

  defp validar_prazo(dias) do
    if is_integer(dias) and dias >= @dias_min and dias <= @dias_max do
      :ok
    else
      {:error, {:validation, {:prazo_invalido, @dias_min..@dias_max}}}
    end
  end

  defp validar_disponibilidade(estado, livro_id) do
    if Enum.any?(emprestimos_ativos(estado), &(&1.livro_id == livro_id)) do
      {:error, {:validation, :livro_indisponivel}}
    else
      :ok
    end
  end

  defp validar_limite(estado, membro_id) do
    ativos = Enum.count(emprestimos_ativos(estado), &(&1.membro_id == membro_id))

    if ativos < @max_emprestimos_por_membro do
      :ok
    else
      {:error, {:validation, :limite_emprestimos_atingido}}
    end
  end

  defp validar_pendente(%{status: :devolvido}), do: {:error, {:validation, :ja_devolvido}}
  defp validar_pendente(_emprestimo), do: :ok

  defp buscar(mapa, chave, motivo) do
    case Map.fetch(mapa, chave) do
      {:ok, valor} -> {:ok, valor}
      :error -> {:error, {:validation, motivo}}
    end
  end

  # ---------------------------------------------------------------------------
  # Auxiliares
  # ---------------------------------------------------------------------------

  defp emprestimos_ativos(estado) do
    estado.emprestimos
    |> Map.values()
    |> Enum.filter(&(&1.status in [:ativo, :atrasado]))
  end

  defp marcar_atrasos(estado, agora) do
    {emprestimos, quantidade} =
      Enum.reduce(estado.emprestimos, {%{}, 0}, fn {id, emp}, {acc, n} ->
        if emp.status == :ativo and emp.vence_em < agora do
          {Map.put(acc, id, %{emp | status: :atrasado}), n + 1}
        else
          {Map.put(acc, id, emp), n}
        end
      end)

    {%{estado | emprestimos: emprestimos}, quantidade}
  end

  defp proximo_id(estado) do
    {estado.proximo_id, %{estado | proximo_id: estado.proximo_id + 1}}
  end

  defp agora, do: System.system_time(:second)

  defp agendar(intervalo) do
    Process.send_after(self(), :verificar_atrasos, intervalo)
  end

  defp estado_inicial do
    %{livros: %{}, membros: %{}, emprestimos: %{}, proximo_id: 1}
  end

  # ---------------------------------------------------------------------------
  # Persistência
  # ---------------------------------------------------------------------------

  # Escrita atômica: grava em arquivo temporário e depois renomeia.
  # Em caso de falha, o arquivo anterior permanece íntegro.
  defp persistir(estado) do
    dados = Map.take(estado, [:livros, :membros, :emprestimos, :proximo_id])
    binario = :erlang.term_to_binary({@versao_schema, dados})
    temporario = estado.caminho <> ".tmp"

    with :ok <- File.write(temporario, binario),
         :ok <- File.rename(temporario, estado.caminho) do
      :ok
    end
  end

  defp carregar(caminho) do
    case File.read(caminho) do
      {:ok, binario} -> decodificar(binario)
      {:error, :enoent} -> {:ok, estado_inicial()}
      {:error, motivo} -> {:error, {:operational, {:leitura, motivo}}}
    end
  end

  defp decodificar(binario) do
    case decodificar_seguro(binario) do
      {:ok, {@versao_schema, dados}} when is_map(dados) ->
        {:ok, Map.merge(estado_inicial(), dados)}

      {:ok, _outro} ->
        {:error, {:operational, :versao_incompativel}}

      :error ->
        {:error, {:operational, :arquivo_corrompido}}
    end
  end

  defp decodificar_seguro(binario) do
    {:ok, :erlang.binary_to_term(binario, [:safe])}
  rescue
    ArgumentError -> :error
  end
end
