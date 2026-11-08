# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de empréstimos de livros, atendendo somente aos seguintes requisitos:
#
# 1. Recuperar processos após falhas inesperadas.
# 2. Detectar encerramentos inesperados de processos.
# 3. Garantir a consistência dos dados durante operações concorrentes.
# 4. Permitir cadastrar, consultar, atualizar e remover registros.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# Sistema de gerenciamento de empréstimos de livros.
#
# Requisitos atendidos:
#   1. Recuperação após falhas: supervisão (:one_for_one) com reinício automático e
#      persistência em :dets. Ao reiniciar, o estado é recarregado do disco.
#   2. Detecção de encerramentos inesperados: cada reserva é monitorada pelo processo
#      cliente. Se ele morrer antes de confirmar, a reserva é cancelada e o motivo é registrado.
#   3. Consistência em concorrência: todas as operações passam pelo mesmo GenServer,
#      que as serializa. Verificações e alterações ocorrem atomicamente dentro de um
#      único handle_call, e cada mutação é gravada em disco antes da resposta.
#   4. CRUD: cadastrar, consultar, atualizar e remover livros; reservar, confirmar,
#      devolver e consultar empréstimos.
#
# Execução: elixir emprestimos.exs

defmodule Emprestimos.Store do
  @moduledoc """
  Processo dono do estado de livros e empréstimos.

  Ciclo de vida de um empréstimo:
    reservar/2 -> :pendente  (monitorado pelo cliente)
    confirmar/1 -> :emprestado (monitor removido)
    devolver/1 -> :devolvido
    cliente morre antes de confirmar -> :cancelado (motivo registrado)
  """

  use GenServer
  require Logger

  @nome __MODULE__
  @tabela :emprestimos_dets
  @arquivo_padrao ~c"emprestimos.dets"

  # ---------------------------------------------------------------------------
  # API pública
  # ---------------------------------------------------------------------------

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: @nome)
  end

  # CRUD de livros
  def cadastrar_livro(attrs), do: GenServer.call(@nome, {:cadastrar_livro, attrs})
  def consultar_livro(id), do: GenServer.call(@nome, {:consultar_livro, id})
  def listar_livros, do: GenServer.call(@nome, :listar_livros)
  def atualizar_livro(id, attrs), do: GenServer.call(@nome, {:atualizar_livro, id, attrs})
  def remover_livro(id), do: GenServer.call(@nome, {:remover_livro, id})

  # Empréstimos
  def reservar(livro_id, membro), do: GenServer.call(@nome, {:reservar, livro_id, membro})
  def confirmar(emprestimo_id), do: GenServer.call(@nome, {:confirmar, emprestimo_id})
  def devolver(emprestimo_id), do: GenServer.call(@nome, {:devolver, emprestimo_id})
  def consultar_emprestimo(id), do: GenServer.call(@nome, {:consultar_emprestimo, id})
  def listar_emprestimos, do: GenServer.call(@nome, :listar_emprestimos)

  # ---------------------------------------------------------------------------
  # Inicialização e recuperação
  # ---------------------------------------------------------------------------

  @impl true
  def init(opts) do
    # Necessário para que terminate/2 seja chamado no encerramento pelo supervisor
    Process.flag(:trap_exit, true)

    arquivo = Keyword.get(opts, :arquivo, @arquivo_padrao)

    case :dets.open_file(@tabela, file: arquivo, type: :set) do
      {:ok, tab} -> {:ok, carregar(tab)}
      {:error, motivo} -> {:stop, {:falha_ao_abrir_dets, motivo}}
    end
  end

  @impl true
  def terminate(_motivo, st) do
    :dets.close(st.dets)
  end

  # Recarrega o estado do disco após um reinício.
  defp carregar(tab) do
    estado0 = %{dets: tab, livros: %{}, emprestimos: %{}, monitores: %{}, proximo_id: 1}

    estado =
      :dets.foldl(
        fn
          {{:livro, id}, livro}, acc ->
            %{acc | livros: Map.put(acc.livros, id, livro)}

          {{:emprestimo, id}, emp}, acc ->
            %{acc | emprestimos: Map.put(acc.emprestimos, id, emp)}

          {{:meta, :proximo_id}, n}, acc ->
            %{acc | proximo_id: n}
        end,
        estado0,
        tab
      )

    # Reservas pendentes perderam seus monitores com a falha.
    # Como não há mais dono para confirmá-las, são canceladas.
    cancelamentos =
      for {id, %{status: :pendente} = emp} <- estado.emprestimos do
        {{:emprestimo, id}, %{emp | status: :cancelado, motivo: :falha_do_servico}}
      end

    true = :dets.insert(tab, cancelamentos)
    :ok = :dets.sync(tab)

    emprestimos =
      Enum.reduce(cancelamentos, estado.emprestimos, fn {{:emprestimo, id}, emp}, acc ->
        Map.put(acc, id, emp)
      end)

    %{estado | emprestimos: emprestimos}
  end

  # ---------------------------------------------------------------------------
  # Callbacks de chamadas síncronas
  # ---------------------------------------------------------------------------

  @impl true
  def handle_call({:cadastrar_livro, attrs}, _from, st) do
    case validar(attrs) do
      {:ok, dados} ->
        id = st.proximo_id
        livro = Map.put(dados, :id, id)
        st = %{st | livros: Map.put(st.livros, id, livro), proximo_id: id + 1}
        st = gravar(st, [{{:livro, id}, livro}, {{:meta, :proximo_id}, id + 1}])
        {:reply, {:ok, livro}, st}

      erro ->
        {:reply, erro, st}
    end
  end

  def handle_call({:consultar_livro, id}, _from, st) do
    {:reply, buscar(st.livros, id), st}
  end

  def handle_call(:listar_livros, _from, st) do
    livros = st.livros |> Map.values() |> Enum.sort_by(& &1.id)
    {:reply, livros, st}
  end

  def handle_call({:atualizar_livro, id, attrs}, _from, st) do
    resultado =
      with {:ok, livro} <- buscar(st.livros, id),
           {:ok, dados} <- validar(Map.merge(livro, Map.take(attrs, [:titulo, :autor]))) do
        {:ok, Map.put(dados, :id, id)}
      end

    case resultado do
      {:ok, atualizado} ->
        st = %{st | livros: Map.put(st.livros, id, atualizado)}
        st = gravar(st, [{{:livro, id}, atualizado}])
        {:reply, {:ok, atualizado}, st}

      erro ->
        {:reply, erro, st}
    end
  end

  def handle_call({:remover_livro, id}, _from, st) do
    cond do
      not Map.has_key?(st.livros, id) ->
        {:reply, {:error, :nao_encontrado}, st}

      not disponivel?(st, id) ->
        {:reply, {:error, :livro_emprestado}, st}

      true ->
        st = %{st | livros: Map.delete(st.livros, id)}
        st = gravar(st, [], [{:livro, id}])
        {:reply, :ok, st}
    end
  end

  def handle_call({:reservar, livro_id, membro}, {pid, _tag}, st) do
    cond do
      not Map.has_key?(st.livros, livro_id) ->
        {:reply, {:error, :livro_nao_encontrado}, st}

      not valido_texto?(membro) ->
        {:reply, {:error, :dados_invalidos}, st}

      not disponivel?(st, livro_id) ->
        {:reply, {:error, :indisponivel}, st}

      true ->
        id = st.proximo_id
        emp = %{id: id, livro_id: livro_id, membro: membro, status: :pendente, motivo: nil}

        # Monitora o processo cliente para detectar encerramento inesperado
        ref = Process.monitor(pid)

        st = %{
          st
          | emprestimos: Map.put(st.emprestimos, id, emp),
            monitores: Map.put(st.monitores, id, ref),
            proximo_id: id + 1
        }

        st = gravar(st, [{{:emprestimo, id}, emp}, {{:meta, :proximo_id}, id + 1}])
        {:reply, {:ok, emp}, st}
    end
  end

  def handle_call({:confirmar, emp_id}, _from, st) do
    case Map.fetch(st.emprestimos, emp_id) do
      {:ok, %{status: :pendente} = emp} ->
        {ref, monitores} = Map.pop(st.monitores, emp_id)
        Process.demonitor(ref, [:flush])

        emp = %{emp | status: :emprestado}
        st = %{st | emprestimos: Map.put(st.emprestimos, emp_id, emp), monitores: monitores}
        st = gravar(st, [{{:emprestimo, emp_id}, emp}])
        {:reply, {:ok, emp}, st}

      {:ok, _} ->
        {:reply, {:error, :estado_invalido}, st}

      :error ->
        {:reply, {:error, :nao_encontrado}, st}
    end
  end

  def handle_call({:devolver, emp_id}, _from, st) do
    case Map.fetch(st.emprestimos, emp_id) do
      {:ok, %{status: :emprestado} = emp} ->
        emp = %{emp | status: :devolvido}
        st = %{st | emprestimos: Map.put(st.emprestimos, emp_id, emp)}
        st = gravar(st, [{{:emprestimo, emp_id}, emp}])
        {:reply, {:ok, emp}, st}

      {:ok, _} ->
        {:reply, {:error, :estado_invalido}, st}

      :error ->
        {:reply, {:error, :nao_encontrado}, st}
    end
  end

  def handle_call({:consultar_emprestimo, id}, _from, st) do
    {:reply, buscar(st.emprestimos, id), st}
  end

  def handle_call(:listar_emprestimos, _from, st) do
    emprestimos = st.emprestimos |> Map.values() |> Enum.sort_by(& &1.id)
    {:reply, emprestimos, st}
  end

  # Qualquer requisição inválida retorna erro em vez de derrubar o processo
  def handle_call(_msg, _from, st) do
    {:reply, {:error, :requisicao_invalida}, st}
  end

  # ---------------------------------------------------------------------------
  # Mensagens assíncronas (detecção de encerramento de clientes)
  # ---------------------------------------------------------------------------

  @impl true
  def handle_info({:DOWN, ref, :process, _pid, motivo}, st) do
    case Enum.find(st.monitores, fn {_id, r} -> r == ref end) do
      nil ->
        {:noreply, st}

      {emp_id, _ref} ->
        if motivo != :normal do
          Logger.warning(
            "Cliente da reserva #{emp_id} encerrou inesperadamente: #{inspect(motivo)}"
          )
        end

        emp = %{Map.fetch!(st.emprestimos, emp_id) | status: :cancelado, motivo: motivo}

        st = %{
          st
          | monitores: Map.delete(st.monitores, emp_id),
            emprestimos: Map.put(st.emprestimos, emp_id, emp)
        }

        {:noreply, gravar(st, [{{:emprestimo, emp_id}, emp}])}
    end
  end

  def handle_info(_msg, st), do: {:noreply, st}

  # ---------------------------------------------------------------------------
  # Funções auxiliares
  # ---------------------------------------------------------------------------

  # Grava inserções e remoções no disco antes de a resposta ser enviada.
  # Se a gravação falhar, o processo cai e é recarregado do último estado persistido.
  defp gravar(st, insercoes, remocoes \\ []) do
    true = :dets.insert(st.dets, insercoes)
    Enum.each(remocoes, fn chave -> :ok = :dets.delete(st.dets, chave) end)
    :ok = :dets.sync(st.dets)
    st
  end

  defp buscar(mapa, id) do
    case Map.fetch(mapa, id) do
      {:ok, valor} -> {:ok, valor}
      :error -> {:error, :nao_encontrado}
    end
  end

  defp validar(attrs) when is_map(attrs) do
    dados = Map.take(attrs, [:titulo, :autor])

    if valido_texto?(dados[:titulo]) and valido_texto?(dados[:autor]) do
      {:ok, dados}
    else
      {:error, :dados_invalidos}
    end
  end

  defp validar(_), do: {:error, :dados_invalidos}

  defp valido_texto?(texto), do: is_binary(texto) and String.trim(texto) != ""

  defp disponivel?(st, livro_id) do
    not Enum.any?(st.emprestimos, fn {_id, emp} ->
      emp.livro_id == livro_id and emp.status in [:pendente, :emprestado]
    end)
  end
end

defmodule Emprestimos.Supervisor do
  @moduledoc """
  Supervisor que reinicia o Store automaticamente após falhas.
  """

  use Supervisor

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    children = [
      {Emprestimos.Store, opts}
    ]

    Supervisor.init(children, strategy: :one_for_one, max_restarts: 5, max_seconds: 10)
  end
end

defmodule Emprestimos.Demo do
  @moduledoc """
  Demonstração dos requisitos: CRUD, concorrência, detecção de falhas e recuperação.
  """

  alias Emprestimos.Store

  def executar do
    {:ok, sup} = Emprestimos.Supervisor.start_link(arquivo: ~c"demo.dets")

    # CRUD de livros
    {:ok, livro} = Store.cadastrar_livro(%{titulo: "Dom Casmurro", autor: "Machado de Assis"})
    {:ok, livro} = Store.atualizar_livro(livro.id, %{autor: "Machado de Assis (ed. revisada)"})
    IO.inspect(Store.consultar_livro(livro.id), label: "livro atualizado")

    # Concorrência: 20 clientes disputam o mesmo livro; apenas um deve conseguir
    sucessos =
      1..20
      |> Task.async_stream(
        fn i ->
          case Store.reservar(livro.id, "Leitor #{i}") do
            {:ok, emp} -> Store.confirmar(emp.id)
            erro -> erro
          end
        end,
        max_concurrency: 20
      )
      |> Enum.map(fn {:ok, res} -> res end)
      |> Enum.count(&match?({:ok, _}, &1))

    IO.puts("Empréstimos concorrentes bem-sucedidos (esperado 1): #{sucessos}")

    # Detecção de encerramento inesperado de cliente
    {:ok, livro2} = Store.cadastrar_livro(%{titulo: "Memórias Póstumas", autor: "Machado de Assis"})
    parent = self()

    cliente =
      spawn(fn ->
        send(parent, {:reservado, Store.reservar(livro2.id, "Ana")})
        Process.sleep(:infinity)
      end)

    emp =
      receive do
        {:reservado, {:ok, emp}} -> emp
      end

    Process.exit(cliente, :kill)
    Process.sleep(50)
    IO.inspect(Store.consultar_emprestimo(emp.id), label: "reserva após cliente morrer")

    # Recuperação após falha do processo principal
    antigo = Process.whereis(Store)
    Process.exit(antigo, :kill)
    Process.sleep(50)
    novo = Process.whereis(Store)

    IO.puts("Store reiniciado pelo supervisor: #{antigo != novo}")
    IO.inspect(Store.listar_livros() |> length(), label: "livros recuperados do disco")

    # Remoção
    IO.inspect(Store.remover_livro(livro.id), label: "remover livro emprestado")

    Supervisor.stop(sup)
    File.rm("demo.dets")
    :ok
  end
end

Emprestimos.Demo.executar()
