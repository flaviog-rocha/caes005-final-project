# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de empréstimos de livros, atendendo somente aos seguintes requisitos:
#
# 1. Permitir cadastrar, consultar, atualizar e remover registros.
# 2. Persistir os dados para recuperação após reinicializações.
# 3. Recuperar processos após falhas inesperadas.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - Task.Supervisor
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule Biblioteca.Emprestimos do
  @moduledoc """
  Gerenciamento de empréstimos de livros.

  Árvore de supervisão (:rest_for_one):
    * Task.Supervisor (Biblioteca.TarefasSup) executa as gravações em disco de forma isolada.
    * Biblioteca.Emprestimos.Armazenamento mantém o estado, valida operações e persiste os dados.

  Persistência:
    * Cada alteração é gravada em um arquivo temporário, sincronizada com o disco e
      só então promovida (rename atômico). Antes disso, o arquivo anterior é copiado
      para um backup (.bak).
    * Na inicialização, o estado é recarregado do arquivo principal ou, se ele estiver
      ausente/corrompido, do backup.

  Recuperação de falhas:
    * Se a gravação falhar ou expirar, a operação é rejeitada e o estado anterior é mantido.
    * Se o processo de armazenamento travar, o supervisor o reinicia e ele recarrega os dados do disco.
    * Se a tarefa de gravação travar, ela é isolada pelo Task.Supervisor e não derruba o armazenamento.
  """

  use Supervisor

  alias Biblioteca.Emprestimos.Armazenamento, as: Store

  @doc "Inicia a árvore de supervisão. Opções: `arquivo:` (caminho do arquivo de dados)."
  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    filhos = [
      {Task.Supervisor, name: Biblioteca.TarefasSup},
      {Store, opts}
    ]

    # :rest_for_one garante que, se o Task.Supervisor cair, o armazenamento também
    # seja reiniciado, mantendo a consistência entre os dois processos.
    Supervisor.init(filhos, strategy: :rest_for_one)
  end

  @doc "Cadastra um empréstimo. Campos obrigatórios: :livro e :usuario."
  def cadastrar(attrs), do: GenServer.call(Store, {:cadastrar, attrs})

  @doc "Consulta um empréstimo pelo id."
  def consultar(id), do: GenServer.call(Store, {:consultar, id})

  @doc "Lista todos os empréstimos ordenados por id."
  def listar, do: GenServer.call(Store, :listar)

  @doc "Atualiza campos de um empréstimo existente."
  def atualizar(id, mudancas), do: GenServer.call(Store, {:atualizar, id, mudancas})

  @doc "Remove um empréstimo pelo id."
  def remover(id), do: GenServer.call(Store, {:remover, id})
end

defmodule Biblioteca.Emprestimos.Armazenamento do
  @moduledoc false

  use GenServer
  require Logger

  @nome __MODULE__
  @tarefas Biblioteca.TarefasSup
  @arquivo_padrao "emprestimos.dat"
  @timeout_persistencia 3_000
  @prazo_dias 14
  @campos_editaveis [
    :livro,
    :usuario,
    :data_emprestimo,
    :data_devolucao_prevista,
    :devolvido_em,
    :status
  ]
  @status_validos [:ativo, :devolvido]

  # ---------------------------------------------------------------------------
  # Inicialização
  # ---------------------------------------------------------------------------

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: @nome)
  end

  @impl true
  def init(opts) do
    arquivo = Keyword.get(opts, :arquivo, @arquivo_padrao)
    dados = carregar(arquivo)
    {:ok, Map.put(dados, :arquivo, arquivo)}
  end

  # ---------------------------------------------------------------------------
  # Callbacks
  # ---------------------------------------------------------------------------

  @impl true
  def handle_call({:cadastrar, attrs}, _from, estado) do
    id = estado.proximo_id

    case criar_registro(id, attrs) do
      {:ok, registro} ->
        novo = %{
          estado
          | registros: Map.put(estado.registros, id, registro),
            proximo_id: id + 1
        }

        confirmar({:ok, registro}, estado, novo)

      {:error, _} = erro ->
        {:reply, erro, estado}
    end
  end

  def handle_call({:consultar, id}, _from, estado) do
    {:reply, buscar(estado, id), estado}
  end

  def handle_call(:listar, _from, estado) do
    registros =
      estado.registros
      |> Map.values()
      |> Enum.sort_by(& &1.id)

    {:reply, {:ok, registros}, estado}
  end

  def handle_call({:atualizar, id, mudancas}, _from, estado) when is_map(mudancas) do
    with {:ok, atual} <- buscar(estado, id),
         atualizado = Map.merge(atual, Map.take(mudancas, @campos_editaveis)),
         {:ok, registro} <- validar(atualizado) do
      novo = %{estado | registros: Map.put(estado.registros, id, registro)}
      confirmar({:ok, registro}, estado, novo)
    else
      erro -> {:reply, erro, estado}
    end
  end

  def handle_call({:atualizar, _id, _mudancas}, _from, estado) do
    {:reply, {:error, :atributos_invalidos}, estado}
  end

  def handle_call({:remover, id}, _from, estado) do
    case buscar(estado, id) do
      {:ok, _} ->
        novo = %{estado | registros: Map.delete(estado.registros, id)}
        confirmar(:ok, estado, novo)

      erro ->
        {:reply, erro, estado}
    end
  end

  @impl true
  def handle_info(msg, estado) do
    # Mensagens inesperadas são registradas e ignoradas para não derrubar o processo.
    Logger.warning("Mensagem inesperada ignorada: #{inspect(msg)}")
    {:noreply, estado}
  end

  # ---------------------------------------------------------------------------
  # Regras de negócio
  # ---------------------------------------------------------------------------

  defp criar_registro(id, attrs) when is_map(attrs) do
    hoje = Date.utc_today()

    padrao = %{
      id: id,
      data_emprestimo: hoje,
      data_devolucao_prevista: Date.add(hoje, @prazo_dias),
      devolvido_em: nil,
      status: :ativo
    }

    padrao
    |> Map.merge(Map.take(attrs, @campos_editaveis))
    |> validar()
  end

  defp criar_registro(_id, _attrs), do: {:error, :atributos_invalidos}

  defp validar(registro) do
    cond do
      not texto?(Map.get(registro, :livro)) ->
        {:error, {:invalido, :livro}}

      not texto?(Map.get(registro, :usuario)) ->
        {:error, {:invalido, :usuario}}

      Map.get(registro, :status) not in @status_validos ->
        {:error, {:invalido, :status}}

      true ->
        {:ok, registro}
    end
  end

  defp texto?(valor), do: is_binary(valor) and String.trim(valor) != ""

  defp buscar(estado, id) do
    case Map.fetch(estado.registros, id) do
      {:ok, registro} -> {:ok, registro}
      :error -> {:error, :nao_encontrado}
    end
  end

  # ---------------------------------------------------------------------------
  # Confirmação com persistência
  # ---------------------------------------------------------------------------

  # Só confirma o novo estado se a gravação em disco for bem-sucedida.
  defp confirmar(resposta, estado_anterior, novo_estado) do
    dados = Map.take(novo_estado, [:registros, :proximo_id])

    case persistir(novo_estado.arquivo, dados) do
      :ok ->
        {:reply, resposta, novo_estado}

      {:error, motivo} ->
        Logger.error("Falha ao persistir dados: #{inspect(motivo)}")
        {:reply, {:error, {:persistencia, motivo}}, estado_anterior}
    end
  end

  # ---------------------------------------------------------------------------
  # Persistência (executada em tarefa supervisionada)
  # ---------------------------------------------------------------------------

  defp persistir(arquivo, dados) do
    tarefa =
      Task.Supervisor.async_nolink(@tarefas, fn ->
        gravar(arquivo, dados)
      end)

    case Task.yield(tarefa, @timeout_persistencia) || Task.shutdown(tarefa, :brutal_kill) do
      {:ok, :ok} -> :ok
      {:ok, {:error, motivo}} -> {:error, motivo}
      {:ok, outro} -> {:error, outro}
      {:exit, motivo} -> {:error, {:falha_tarefa, motivo}}
      nil -> {:error, :timeout}
    end
  end

  defp gravar(arquivo, dados) do
    tmp = arquivo <> ".tmp"
    bin = :erlang.term_to_binary(dados)

    with {:ok, io} <- File.open(tmp, [:write, :binary]),
         :ok <- IO.binwrite(io, bin),
         :ok <- :file.sync(io),
         :ok <- File.close(io),
         :ok <- backup(arquivo),
         :ok <- File.rename(tmp, arquivo) do
      :ok
    else
      {:error, motivo} -> {:error, motivo}
      outro -> {:error, outro}
    end
  end

  defp backup(arquivo) do
    if File.exists?(arquivo) do
      File.cp(arquivo, arquivo <> ".bak")
    else
      :ok
    end
  end

  # ---------------------------------------------------------------------------
  # Carregamento / recuperação
  # ---------------------------------------------------------------------------

  defp carregar(arquivo) do
    padrao = %{registros: %{}, proximo_id: 1}

    Enum.reduce_while([arquivo, arquivo <> ".bak"], padrao, fn caminho, acc ->
      case ler(caminho) do
        {:ok, dados} ->
          {:halt, dados}

        {:erro, :inexistente} ->
          {:cont, acc}

        {:erro, motivo} ->
          Logger.warning("Não foi possível carregar #{caminho}: #{inspect(motivo)}")
          {:cont, acc}
      end
    end)
  end

  defp ler(caminho) do
    case File.read(caminho) do
      {:ok, bin} ->
        try do
          dados = :erlang.binary_to_term(bin, [:safe])

          if dados_validos?(dados) do
            {:ok, Map.take(dados, [:registros, :proximo_id])}
          else
            {:erro, :formato_invalido}
          end
        rescue
          ArgumentError -> {:erro, :corrompido}
        end

      {:error, :enoent} ->
        {:erro, :inexistente}

      {:error, motivo} ->
        {:erro, motivo}
    end
  end

  defp dados_validos?(%{registros: registros, proximo_id: proximo})
       when is_map(registros) and is_integer(proximo),
       do: true

  defp dados_validos?(_), do: false
end
