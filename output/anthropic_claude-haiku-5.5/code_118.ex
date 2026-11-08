# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de empréstimos de livros, atendendo somente aos seguintes requisitos:
#
# 1. Persistir os dados para recuperação após reinicializações.
# 2. Validar os dados recebidos antes de realizar operações.
# 3. Permitir cadastrar, consultar, atualizar e remover registros.
# 4. Executar determinadas operações periodicamente.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - Task
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# Exemplo de uso (em outro arquivo ou no iex):
#
#   {:ok, _pid} = Biblioteca.Emprestimos.start_link(arquivo: "emprestimos.dat", intervalo: :timer.minutes(30))
#   {:ok, e} = Biblioteca.Emprestimos.cadastrar(%{titulo: "Dom Casmurro", leitor: "Ana", devolucao_prevista: ~D[2030-01-10]})
#   {:ok, e} = Biblioteca.Emprestimos.consultar(e.id)
#   {:ok, e} = Biblioteca.Emprestimos.atualizar(e.id, %{devolvido_em: ~D[2030-01-05]})
#   :ok = Biblioteca.Emprestimos.remover(e.id)
#   {:ok, lista} = Biblioteca.Emprestimos.listar()

defmodule Biblioteca.Emprestimos do
  @moduledoc """
  Gerenciamento de empréstimos de livros.

  - Persiste o estado em disco (termos Erlang) a cada alteração, com escrita
    atômica (arquivo temporário + rename), permitindo recuperação após reinício.
  - Valida todos os dados antes de qualquer operação de escrita.
  - Oferece cadastro, consulta, atualização e remoção de empréstimos.
  - Uma Task periódica verifica empréstimos atrasados e atualiza seus status.
  """

  use GenServer

  @arquivo_padrao "emprestimos.dat"
  @intervalo_padrao :timer.minutes(60)
  @campos_atualizaveis [:titulo, :leitor, :emprestado_em, :devolucao_prevista, :devolvido_em]

  # ---------------------------------------------------------------------------
  # API pública
  # ---------------------------------------------------------------------------

  @doc """
  Inicia o servidor. Opções:
    * `:name` - nome do processo (padrão: `__MODULE__`)
    * `:arquivo` - caminho do arquivo de persistência
    * `:intervalo` - intervalo em ms da verificação periódica de atrasos (`nil` desabilita)
  """
  def start_link(opts \\ []) do
    nome = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: nome)
  end

  def cadastrar(attrs, servidor \\ __MODULE__), do: GenServer.call(servidor, {:cadastrar, attrs})

  def consultar(id, servidor \\ __MODULE__), do: GenServer.call(servidor, {:consultar, id})

  def listar(servidor \\ __MODULE__), do: GenServer.call(servidor, :listar)

  def atualizar(id, attrs, servidor \\ __MODULE__),
    do: GenServer.call(servidor, {:atualizar, id, attrs})

  def remover(id, servidor \\ __MODULE__), do: GenServer.call(servidor, {:remover, id})

  @doc "Recalcula os status dos empréstimos e retorna `{:ok, quantidade_atrasados}`."
  def verificar_atrasos(servidor \\ __MODULE__), do: GenServer.call(servidor, :verificar_atrasos)

  # ---------------------------------------------------------------------------
  # Callbacks do GenServer
  # ---------------------------------------------------------------------------

  @impl true
  def init(opts) do
    arquivo = Keyword.get(opts, :arquivo, @arquivo_padrao)
    intervalo = Keyword.get(opts, :intervalo, @intervalo_padrao)

    %{proximo_id: proximo_id, emprestimos: emprestimos} = carregar(arquivo)

    estado = %{arquivo: arquivo, proximo_id: proximo_id, emprestimos: emprestimos}

    # Task responsável pela execução periódica de verificação de atrasos
    if intervalo, do: iniciar_verificacao_periodica(self(), intervalo)

    {:ok, estado}
  end

  @impl true
  def handle_call({:cadastrar, attrs}, _from, estado) do
    case validar_criacao(attrs) do
      {:ok, dados} ->
        id = estado.proximo_id
        emprestimo = Map.put(dados, :id, id)
        emprestimo = %{emprestimo | status: calcular_status(emprestimo, hoje())}

        novo = %{
          estado
          | proximo_id: id + 1,
            emprestimos: Map.put(estado.emprestimos, id, emprestimo)
        }

        confirmar(estado, novo, {:ok, emprestimo})

      {:error, _} = erro ->
        {:reply, erro, estado}
    end
  end

  def handle_call({:consultar, id}, _from, estado) do
    {:reply, buscar(estado, id), estado}
  end

  def handle_call(:listar, _from, estado) do
    lista =
      estado.emprestimos
      |> Map.values()
      |> Enum.sort_by(& &1.id)

    {:reply, {:ok, lista}, estado}
  end

  def handle_call({:atualizar, id, attrs}, _from, estado) do
    with {:ok, atual} <- buscar(estado, id),
         {:ok, dados} <- validar_atualizacao(atual, attrs) do
      emprestimo = %{dados | status: calcular_status(dados, hoje())}
      novo = %{estado | emprestimos: Map.put(estado.emprestimos, id, emprestimo)}
      confirmar(estado, novo, {:ok, emprestimo})
    else
      erro -> {:reply, erro, estado}
    end
  end

  def handle_call({:remover, id}, _from, estado) do
    case buscar(estado, id) do
      {:ok, _} ->
        novo = %{estado | emprestimos: Map.delete(estado.emprestimos, id)}
        confirmar(estado, novo, :ok)

      erro ->
        {:reply, erro, estado}
    end
  end

  def handle_call(:verificar_atrasos, _from, estado) do
    data = hoje()

    atualizados =
      Map.new(estado.emprestimos, fn {id, e} ->
        {id, %{e | status: calcular_status(e, data)}}
      end)

    atrasados =
      atualizados
      |> Map.values()
      |> Enum.count(&(&1.status == :atrasado))

    novo = %{estado | emprestimos: atualizados}
    confirmar(estado, novo, {:ok, atrasados})
  end

  # ---------------------------------------------------------------------------
  # Tarefa periódica (Task)
  # ---------------------------------------------------------------------------

  defp iniciar_verificacao_periodica(servidor, intervalo) do
    Task.start_link(fn -> laco_verificacao(servidor, intervalo) end)
  end

  defp laco_verificacao(servidor, intervalo) do
    Process.sleep(intervalo)
    _ = verificar_atrasos(servidor)
    laco_verificacao(servidor, intervalo)
  end

  # ---------------------------------------------------------------------------
  # Validação
  # ---------------------------------------------------------------------------

  defp validar_criacao(attrs) when is_map(attrs) do
    %{
      titulo: Map.get(attrs, :titulo),
      leitor: Map.get(attrs, :leitor),
      emprestado_em: Map.get(attrs, :emprestado_em, hoje()),
      devolucao_prevista: Map.get(attrs, :devolucao_prevista),
      devolvido_em: nil
    }
    |> validar()
  end

  defp validar_criacao(_), do: {:error, [{:geral, "atributos devem ser um mapa"}]}

  defp validar_atualizacao(atual, attrs) when is_map(attrs) do
    alteracoes = Map.take(attrs, @campos_atualizaveis)
    atual |> Map.merge(alteracoes) |> validar()
  end

  defp validar_atualizacao(_atual, _attrs),
    do: {:error, [{:geral, "atributos devem ser um mapa"}]}

  defp validar(dados) do
    erros =
      []
      |> checar(texto_valido?(dados.titulo, 200), :titulo,
        "título deve ser texto não vazio com até 200 caracteres")
      |> checar(texto_valido?(dados.leitor, 100), :leitor,
        "leitor deve ser texto não vazio com até 100 caracteres")
      |> checar(data?(dados.emprestado_em), :emprestado_em,
        "data de empréstimo inválida")
      |> checar(data?(dados.devolucao_prevista), :devolucao_prevista,
        "data de devolução prevista inválida")
      |> checar(is_nil(dados.devolvido_em) or data?(dados.devolvido_em), :devolvido_em,
        "data de devolução inválida")
      |> checar_ordem(dados)

    case erros do
      [] -> {:ok, dados}
      _ -> {:error, Enum.reverse(erros)}
    end
  end

  defp checar_ordem(erros, %{
         emprestado_em: %Date{} = emprestado,
         devolucao_prevista: %Date{} = prevista,
         devolvido_em: devolvido
       }) do
    erros
    |> checar(Date.compare(prevista, emprestado) == :gt, :devolucao_prevista,
      "deve ser posterior à data de empréstimo")
    |> checar(
      is_nil(devolvido) or (is_struct(devolvido, Date) and Date.compare(devolvido, emprestado) != :lt),
      :devolvido_em,
      "não pode ser anterior à data de empréstimo"
    )
  end

  defp checar_ordem(erros, _dados), do: erros

  defp checar(erros, true, _campo, _mensagem), do: erros
  defp checar(erros, _condicao, campo, mensagem), do: [{campo, mensagem} | erros]

  defp texto_valido?(valor, max) when is_binary(valor) do
    String.trim(valor) != "" and String.length(valor) <= max
  end

  defp texto_valido?(_valor, _max), do: false

  defp data?(valor), do: match?(%Date{}, valor)

  # ---------------------------------------------------------------------------
  # Regras de negócio auxiliares
  # ---------------------------------------------------------------------------

  defp calcular_status(%{devolvido_em: %Date{}}, _hoje), do: :devolvido

  defp calcular_status(%{devolucao_prevista: prevista}, hoje) do
    if Date.compare(prevista, hoje) == :lt, do: :atrasado, else: :ativo
  end

  defp hoje, do: Date.utc_today()

  defp buscar(estado, id) do
    case Map.fetch(estado.emprestimos, id) do
      {:ok, emprestimo} -> {:ok, emprestimo}
      :error -> {:error, :nao_encontrado}
    end
  end

  # ---------------------------------------------------------------------------
  # Persistência
  # ---------------------------------------------------------------------------

  # Confirma a mudança somente se a persistência em disco for bem-sucedida.
  defp confirmar(estado_antigo, novo_estado, resposta) do
    case persistir(novo_estado) do
      :ok ->
        {:reply, resposta, novo_estado}

      {:error, motivo} ->
        {:reply, {:error, {:persistencia, motivo}}, estado_antigo}
    end
  end

  # Escrita atômica: grava em arquivo temporário e renomeia em seguida.
  defp persistir(estado) do
    binario =
      :erlang.term_to_binary(%{
        proximo_id: estado.proximo_id,
        emprestimos: estado.emprestimos
      })

    tmp = estado.arquivo <> ".tmp"

    with :ok <- File.write(tmp, binario),
         :ok <- File.rename(tmp, estado.arquivo) do
      :ok
    end
  end

  # Carrega o estado persistido. Arquivo inexistente gera estado inicial.
  # Arquivo corrompido gera exceção para evitar sobrescrita acidental de dados.
  defp carregar(arquivo) do
    case File.read(arquivo) do
      {:ok, binario} ->
        %{proximo_id: _, emprestimos: _} = dados = :erlang.binary_to_term(binario, [:safe])
        dados

      {:error, :enoent} ->
        %{proximo_id: 1, emprestimos: %{}}

      {:error, motivo} ->
        raise "não foi possível ler #{arquivo}: #{inspect(motivo)}"
    end
  end
end
