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

# Gerenciamento de empréstimos de livros em Elixir.
#
# Requisitos atendidos:
# 1. CRUD de registros de empréstimo (cadastrar, consultar, listar, atualizar, remover).
# 2. Persistência em disco: cada alteração é gravada de forma atômica
#    (escrita em arquivo temporário + fsync + rename) antes de ser confirmada ao cliente.
# 3. Recuperação: o Store é supervisionado (:one_for_one) e recarrega o último
#    estado persistido ao reiniciar. Falhas na escrita não corrompem o estado.
#
# Uso (exemplo):
#   Application.put_env(:emprestimos, :arquivo, "/tmp/emprestimos.dat")
#   {:ok, _} = Emprestimos.Application.start(:normal, [])
#   {:ok, reg} = Emprestimos.cadastrar(%{livro: "Dom Casmurro", usuario: "Ana"})
#   {:ok, ^reg} = Emprestimos.consultar(reg.id)
#   {:ok, _} = Emprestimos.atualizar(reg.id, %{devolvido_em: Date.utc_today()})
#   {:ok, _} = Emprestimos.remover(reg.id)

defmodule Emprestimos.Application do
  @moduledoc false
  use Application

  @impl true
  def start(_tipo, _args) do
    filhos = [
      # Supervisor de tarefas usado para executar a persistência em processos isolados.
      {Task.Supervisor, name: Emprestimos.TaskSupervisor},
      # Processo que mantém o estado e coordena leituras/escritas.
      Emprestimos.Store
    ]

    # :one_for_one: se o Store cair, apenas ele é reiniciado e recarrega do disco.
    Supervisor.start_link(filhos, strategy: :one_for_one, name: Emprestimos.Supervisor)
  end
end

defmodule Emprestimos.Store do
  @moduledoc false
  # GenServer dono do estado. Regras:
  # - Leituras são atendidas com o último estado confirmado (persistido).
  # - Escritas são serializadas: uma por vez. A gravação ocorre em uma Task
  #   supervisionada; só após sucesso no disco o estado é trocado e o cliente
  #   recebe a resposta ("durabilidade antes da confirmação").
  # - Se a gravação falhar, o estado em memória permanece o anterior.

  use GenServer

  @nome __MODULE__
  @supervisor Emprestimos.TaskSupervisor
  @campos [:livro, :usuario, :emprestado_em, :devolvido_em]
  @timeout 15_000

  # ---------------------------------------------------------------------------
  # API interna
  # ---------------------------------------------------------------------------

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: @nome)
  end

  def escrever(operacao), do: GenServer.call(@nome, {:escrever, operacao}, @timeout)
  def ler(operacao), do: GenServer.call(@nome, {:ler, operacao}, @timeout)

  # ---------------------------------------------------------------------------
  # Callbacks
  # ---------------------------------------------------------------------------

  @impl true
  def init(opts) do
    arquivo =
      Keyword.get(
        opts,
        :arquivo,
        Application.get_env(:emprestimos, :arquivo, "emprestimos.dat")
      )

    estado = %{
      arquivo: arquivo,
      snapshot: carregar(arquivo),
      # Operação em andamento: {ref_da_task, from, resposta, novo_snapshot} ou nil
      busy: nil,
      # Fila de escritas pendentes: {from, operacao}
      fila: :queue.new()
    }

    {:ok, estado}
  end

  @impl true
  def handle_call({:ler, operacao}, _from, estado) do
    {:reply, executar_leitura(operacao, estado.snapshot), estado}
  end

  def handle_call({:escrever, operacao}, from, estado) do
    estado = %{estado | fila: :queue.in({from, operacao}, estado.fila)}
    {:noreply, proximo(estado)}
  end

  # Resultado da Task de persistência (sucesso ou erro retornado).
  @impl true
  def handle_info({ref, resultado}, %{busy: {ref, from, resposta, novo}} = estado) do
    Process.demonitor(ref, [:flush])
    estado = %{estado | busy: nil}

    estado =
      case resultado do
        :ok ->
          GenServer.reply(from, {:ok, resposta})
          %{estado | snapshot: novo}

        {:error, motivo} ->
          GenServer.reply(from, {:error, {:persistencia, motivo}})
          estado
      end

    {:noreply, proximo(estado)}
  end

  # Task de persistência caiu inesperadamente: o estado anterior é mantido.
  def handle_info({:DOWN, ref, :process, _pid, motivo}, %{busy: {ref, from, _, _}} = estado) do
    GenServer.reply(from, {:error, {:persistencia, motivo}})
    {:noreply, proximo(%{estado | busy: nil})}
  end

  def handle_info(_mensagem, estado), do: {:noreply, estado}

  # ---------------------------------------------------------------------------
  # Fila de escritas
  # ---------------------------------------------------------------------------

  defp proximo(%{busy: nil} = estado) do
    case :queue.out(estado.fila) do
      {:empty, _} ->
        estado

      {{:value, {from, operacao}}, fila} ->
        iniciar(%{estado | fila: fila}, from, operacao)
    end
  end

  defp proximo(estado), do: estado

  defp iniciar(estado, from, operacao) do
    case aplicar(operacao, estado.snapshot) do
      {:error, _} = erro ->
        GenServer.reply(from, erro)
        proximo(estado)

      {:ok, resposta, novo} ->
        arquivo = estado.arquivo

        tarefa =
          Task.Supervisor.async_nolink(@supervisor, fn ->
            persistir(arquivo, novo)
          end)

        %{estado | busy: {tarefa.ref, from, resposta, novo}}
    end
  end

  # ---------------------------------------------------------------------------
  # Operações de domínio (funções puras sobre o snapshot)
  # ---------------------------------------------------------------------------

  defp executar_leitura(:listar, snap) do
    lista =
      snap.emprestimos
      |> Map.values()
      |> Enum.sort_by(& &1.id)

    {:ok, lista}
  end

  defp executar_leitura({:consultar, id}, snap), do: buscar(snap, id)

  defp aplicar({:cadastrar, attrs}, snap) when is_map(attrs) do
    registro =
      Map.merge(
        %{emprestado_em: Date.utc_today(), devolvido_em: nil},
        normalizar(attrs)
      )

    with :ok <- validar(registro) do
      id = snap.proximo_id
      registro = Map.put(registro, :id, id)

      novo = %{
        snap
        | proximo_id: id + 1,
          emprestimos: Map.put(snap.emprestimos, id, registro)
      }

      {:ok, registro, novo}
    end
  end

  defp aplicar({:atualizar, id, attrs}, snap) when is_map(attrs) do
    with {:ok, atual} <- buscar(snap, id),
         registro = Map.merge(atual, normalizar(attrs)),
         :ok <- validar(registro) do
      novo = %{snap | emprestimos: Map.put(snap.emprestimos, id, registro)}
      {:ok, registro, novo}
    end
  end

  defp aplicar({:remover, id}, snap) do
    with {:ok, registro} <- buscar(snap, id) do
      novo = %{snap | emprestimos: Map.delete(snap.emprestimos, id)}
      {:ok, registro, novo}
    end
  end

  defp aplicar(_operacao, _snap), do: {:error, :argumentos_invalidos}

  defp buscar(snap, id) do
    case Map.fetch(snap.emprestimos, id) do
      {:ok, registro} -> {:ok, registro}
      :error -> {:error, :nao_encontrado}
    end
  end

  defp normalizar(attrs), do: Map.take(attrs, @campos)

  defp validar(registro) do
    cond do
      not texto?(registro.livro) ->
        {:error, :livro_invalido}

      not texto?(registro.usuario) ->
        {:error, :usuario_invalido}

      not match?(%Date{}, registro.emprestado_em) ->
        {:error, :data_emprestimo_invalida}

      not (is_nil(registro.devolvido_em) or match?(%Date{}, registro.devolvido_em)) ->
        {:error, :data_devolucao_invalida}

      true ->
        :ok
    end
  end

  defp texto?(valor), do: is_binary(valor) and String.trim(valor) != ""

  # ---------------------------------------------------------------------------
  # Persistência
  # ---------------------------------------------------------------------------

  # Executado dentro de uma Task. Escrita atômica: temporário + fsync + rename.
  # Se qualquer passo falhar, o arquivo oficial permanece íntegro.
  defp persistir(arquivo, snap) do
    tmp = arquivo <> ".tmp"
    binario = :erlang.term_to_binary(snap)

    with {:ok, io} <- File.open(tmp, [:write, :binary]),
         :ok <- IO.binwrite(io, binario),
         :ok <- :file.sync(io),
         :ok <- File.close(io),
         :ok <- File.rename(tmp, arquivo) do
      :ok
    end
  end

  defp carregar(arquivo) do
    case File.read(arquivo) do
      {:ok, binario} ->
        snap = :erlang.binary_to_term(binario, [:safe])
        true = match?(%{proximo_id: _, emprestimos: _}, snap)
        snap

      {:error, :enoent} ->
        %{proximo_id: 1, emprestimos: %{}}

      {:error, motivo} ->
        raise "não foi possível ler #{arquivo}: #{inspect(motivo)}"
    end
  end
end

defmodule Emprestimos do
  @moduledoc false
  # API pública do sistema de empréstimos.

  alias Emprestimos.Store

  @doc "Cadastra um novo empréstimo. Campos: :livro, :usuario (obrigatórios), :emprestado_em, :devolvido_em."
  def cadastrar(attrs), do: Store.escrever({:cadastrar, attrs})

  @doc "Consulta um empréstimo pelo id."
  def consultar(id), do: Store.ler({:consultar, id})

  @doc "Lista todos os empréstimos ordenados por id."
  def listar, do: Store.ler(:listar)

  @doc "Atualiza campos de um empréstimo existente."
  def atualizar(id, attrs), do: Store.escrever({:atualizar, id, attrs})

  @doc "Remove um empréstimo pelo id."
  def remover(id), do: Store.escrever({:remover, id})
end
