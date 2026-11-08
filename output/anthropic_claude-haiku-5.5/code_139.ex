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

  Requisitos atendidos:

    1. Erros são classificados em dois grupos:
       - `{:error, {:validacao, motivo}}`: a entrada ou a regra de negócio foi
         rejeitada (ex.: livro inexistente, livro já emprestado). O chamador
         deve corrigir a requisição.
       - `{:error, {:operacional, motivo}}`: falha de infraestrutura (ex.: erro
         de I/O ao persistir). O chamador pode tentar novamente.

    2. Verificação periódica de atrasos, agendada com `Process.send_after/3`.

    3. Persistência em disco a cada mutação. A escrita é atômica (arquivo
       temporário + `File.rename/2`), e o estado é recarregado na inicialização.
  """

  use GenServer
  require Logger

  @prazo_dias 14
  @intervalo_padrao_ms 60_000
  @arquivo_padrao "biblioteca_emprestimos.dat"

  # ---------------------------------------------------------------------------
  # API pública
  # ---------------------------------------------------------------------------

  @doc "Inicia o servidor. Opções: `:arquivo` e `:intervalo_ms`."
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc "Cadastra um novo livro."
  def cadastrar_livro(livro_id, titulo) do
    GenServer.call(__MODULE__, {:cadastrar_livro, livro_id, titulo})
  end

  @doc "Registra o empréstimo de um livro para um usuário."
  def emprestar(livro_id, usuario_id, data \\ Date.utc_today()) do
    GenServer.call(__MODULE__, {:emprestar, livro_id, usuario_id, data})
  end

  @doc "Registra a devolução de um livro, informando eventual atraso."
  def devolver(livro_id, data \\ Date.utc_today()) do
    GenServer.call(__MODULE__, {:devolver, livro_id, data})
  end

  @doc "Consulta os dados de um livro."
  def consultar(livro_id) do
    GenServer.call(__MODULE__, {:consultar, livro_id})
  end

  @doc "Lista os empréstimos vencidos em relação à data informada."
  def listar_atrasados(data \\ Date.utc_today()) do
    GenServer.call(__MODULE__, {:listar_atrasados, data})
  end

  # ---------------------------------------------------------------------------
  # Callbacks do GenServer
  # ---------------------------------------------------------------------------

  @impl true
  def init(opts) do
    arquivo = Keyword.get(opts, :arquivo, @arquivo_padrao)
    intervalo = Keyword.get(opts, :intervalo_ms, @intervalo_padrao_ms)

    # Recuperação: se o arquivo existir, o estado é restaurado; se não existir,
    # começamos vazio. Um arquivo corrompido ou ilegível interrompe a inicialização
    # com erro operacional, evitando sobrescrever dados válidos.
    case carregar(arquivo) do
      {:ok, livros} ->
        agendar_verificacao(intervalo)
        {:ok, %{livros: livros, arquivo: arquivo, intervalo: intervalo}}

      {:error, motivo} ->
        {:stop, motivo}
    end
  end

  @impl true
  def handle_call({:cadastrar_livro, id, titulo}, _from, estado) do
    responder(estado, &regra_cadastrar(&1, id, titulo))
  end

  def handle_call({:emprestar, livro_id, usuario_id, data}, _from, estado) do
    responder(estado, &regra_emprestar(&1, livro_id, usuario_id, data))
  end

  def handle_call({:devolver, livro_id, data}, _from, estado) do
    responder(estado, &regra_devolver(&1, livro_id, data))
  end

  # Consultas são somente leitura: não passam pela persistência.
  def handle_call({:consultar, livro_id}, _from, estado) do
    {:reply, consultar_livro(estado.livros, livro_id), estado}
  end

  def handle_call({:listar_atrasados, data}, _from, estado) do
    atrasados =
      for {id, %{emprestimo: %{} = emp}} <- estado.livros,
          atrasado?(emp, data),
          do: {id, emp.usuario_id, emp.vencimento}

    {:reply, {:ok, atrasados}, estado}
  end

  @impl true
  def handle_info(:verificar_atrasos, estado) do
    # Tarefa periódica: marca empréstimos vencidos como :atrasado.
    # Falhas operacionais são registradas em log e não derrubam o servidor;
    # o estado anterior é mantido e a próxima execução é agendada normalmente.
    hoje = Date.utc_today()

    novo_estado =
      case executar(estado, &marcar_atrasos(&1, hoje)) do
        {:ok, novo, 0} ->
          novo

        {:ok, novo, n} ->
          Logger.info("#{n} empréstimo(s) marcado(s) como atrasado(s)")
          novo

        {:error, motivo} ->
          Logger.error("Falha na verificação periódica: #{inspect(motivo)}")
          estado
      end

    agendar_verificacao(novo_estado.intervalo)
    {:noreply, novo_estado}
  end

  # ---------------------------------------------------------------------------
  # Regras de negócio (funções puras sobre o mapa de livros)
  # Retornam {:ok, novos_livros, resposta} ou {:error, {tipo, motivo}}
  # ---------------------------------------------------------------------------

  defp regra_cadastrar(livros, id, titulo) do
    with :ok <- validar_id(id),
         :ok <- validar_titulo(titulo),
         :ok <- nao_cadastrado(livros, id) do
      {:ok, Map.put(livros, id, %{titulo: titulo, emprestimo: nil}), :cadastrado}
    end
  end

  defp regra_emprestar(livros, livro_id, usuario_id, data) do
    with :ok <- validar_id(livro_id),
         :ok <- validar_id(usuario_id),
         :ok <- validar_data(data),
         {:ok, livro} <- buscar_livro(livros, livro_id),
         :ok <- disponivel(livro) do
      emp = %{
        usuario_id: usuario_id,
        data: data,
        vencimento: Date.add(data, @prazo_dias),
        status: :ativo
      }

      {:ok, Map.put(livros, livro_id, %{livro | emprestimo: emp}), emp}
    end
  end

  defp regra_devolver(livros, livro_id, data) do
    with :ok <- validar_id(livro_id),
         :ok <- validar_data(data),
         {:ok, livro} <- buscar_livro(livros, livro_id),
         {:ok, emp} <- emprestado(livro) do
      atraso = max(Date.diff(data, emp.vencimento), 0)
      novos = Map.put(livros, livro_id, %{livro | emprestimo: nil})
      {:ok, novos, %{usuario_id: emp.usuario_id, atraso_dias: atraso}}
    end
  end

  defp marcar_atrasos(livros, hoje) do
    {novos, total} =
      Enum.map_reduce(livros, 0, fn
        {id, %{emprestimo: %{status: :ativo} = emp} = livro}, acc ->
          if atrasado?(emp, hoje) do
            livro_atualizado = %{livro | emprestimo: %{emp | status: :atrasado}}
            {{id, livro_atualizado}, acc + 1}
          else
            {{id, livro}, acc}
          end

        par, acc ->
          {par, acc}
      end)

    {:ok, Map.new(novos), total}
  end

  # ---------------------------------------------------------------------------
  # Validações: retornam :ok ou {:error, {:validacao, motivo}}
  # ---------------------------------------------------------------------------

  defp validar_id(id) when is_integer(id) and id > 0, do: :ok
  defp validar_id(_), do: {:error, {:validacao, :id_invalido}}

  defp validar_titulo(titulo) when is_binary(titulo) and byte_size(titulo) > 0, do: :ok
  defp validar_titulo(_), do: {:error, {:validacao, :titulo_invalido}}

  defp validar_data(%Date{}), do: :ok
  defp validar_data(_), do: {:error, {:validacao, :data_invalida}}

  defp nao_cadastrado(livros, id) do
    if Map.has_key?(livros, id),
      do: {:error, {:validacao, :livro_ja_cadastrado}},
      else: :ok
  end

  defp buscar_livro(livros, id) do
    case Map.fetch(livros, id) do
      {:ok, livro} -> {:ok, livro}
      :error -> {:error, {:validacao, :livro_inexistente}}
    end
  end

  defp disponivel(%{emprestimo: nil}), do: :ok
  defp disponivel(_), do: {:error, {:validacao, :livro_indisponivel}}

  defp emprestado(%{emprestimo: nil}), do: {:error, {:validacao, :livro_nao_emprestado}}
  defp emprestado(%{emprestimo: emp}), do: {:ok, emp}

  defp consultar_livro(livros, id) do
    with :ok <- validar_id(id),
         {:ok, livro} <- buscar_livro(livros, id) do
      {:ok, livro}
    end
  end

  defp atrasado?(%{vencimento: venc}, data), do: Date.compare(data, venc) == :gt

  # ---------------------------------------------------------------------------
  # Execução de mutações com persistência
  # ---------------------------------------------------------------------------

  # Só o estado em memória é alterado se a persistência for bem-sucedida.
  # Assim, uma falha operacional não deixa memória e disco divergentes.
  defp executar(%{livros: livros, arquivo: arquivo} = estado, regra) do
    case regra.(livros) do
      {:ok, novos_livros, resposta} ->
        case persistir(arquivo, novos_livros) do
          :ok -> {:ok, %{estado | livros: novos_livros}, resposta}
          {:error, _} = erro -> erro
        end

      {:error, _} = erro ->
        erro
    end
  end

  defp responder(estado, regra) do
    case executar(estado, regra) do
      {:ok, novo_estado, resposta} -> {:reply, {:ok, resposta}, novo_estado}
      {:error, _} = erro -> {:reply, erro, estado}
    end
  end

  # ---------------------------------------------------------------------------
  # Persistência
  # ---------------------------------------------------------------------------

  # Escrita atômica: grava em arquivo temporário e renomeia ao final.
  # Se o processo cair no meio, o arquivo original permanece íntegro.
  defp persistir(arquivo, livros) do
    tmp = arquivo <> ".tmp"
    conteudo = :erlang.term_to_binary(%{versao: 1, livros: livros})

    with :ok <- File.write(tmp, conteudo),
         :ok <- File.rename(tmp, arquivo) do
      :ok
    else
      {:error, motivo} -> {:error, {:operacional, {:persistencia, motivo}}}
    end
  end

  defp carregar(arquivo) do
    case File.read(arquivo) do
      {:ok, binario} ->
        try do
          %{versao: 1, livros: livros} = :erlang.binary_to_term(binario, [:safe])
          {:ok, livros}
        rescue
          _ -> {:error, {:operacional, {:persistencia, :arquivo_corrompido}}}
        end

      {:error, :enoent} ->
        {:ok, %{}}

      {:error, motivo} ->
        {:error, {:operacional, {:persistencia, motivo}}}
    end
  end

  # ---------------------------------------------------------------------------
  # Agendamento
  # ---------------------------------------------------------------------------

  defp agendar_verificacao(intervalo) do
    Process.send_after(self(), :verificar_atrasos, intervalo)
  end
end
