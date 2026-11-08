# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de empréstimos de livros, atendendo somente aos seguintes requisitos:
#
# 1. Permitir consultar o estado atual das operações em andamento.
# 2. Executar determinadas operações periodicamente.
# 3. Quando fizer sentido para o domínio, considerar a comunicação com um banco de dados fictício e, opcionalmente, o uso de Ecto.Migration, sem tornar isso obrigatório.
# 4. Permitir a execução de operações simultâneas.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# Sistema de gerenciamento de empréstimos de livros.
# Requisitos atendidos:
#   1. Consulta do estado atual das operações (Biblioteca.Emprestimos.status/0)
#   2. Operações periódicas (verificação de atrasos a cada intervalo)
#   3. Banco de dados fictício em memória (Agent). Um equivalente com Ecto
#      seria uma migration com a tabela "emprestimos" (livro_id, usuario,
#      prazo, atrasado) e um índice único em (livro_id, usuario).
#   4. Operações simultâneas via Task.Supervisor.
# Execução: elixir arquivo.exs

defmodule Biblioteca.BancoFalso do
  @moduledoc """
  Banco de dados fictício em memória. Usa um Agent, que serializa o acesso
  e garante que cada operação de leitura e escrita seja atômica.
  """
  use Agent

  def start_link(_opts) do
    estado = %{
      livros: %{
        1 => %{titulo: "Dom Casmurro", copias: 2},
        2 => %{titulo: "1984", copias: 1}
      },
      # Chave: {livro_id, usuario} -> %{prazo: DateTime, atrasado: boolean}
      emprestimos: %{}
    }

    Agent.start_link(fn -> estado end, name: __MODULE__)
  end

  @doc "Registra um empréstimo de forma atômica, validando regras de negócio."
  def registrar_emprestimo(livro_id, usuario, dias) do
    Agent.get_and_update(__MODULE__, fn estado ->
      chave = {livro_id, usuario}

      cond do
        not Map.has_key?(estado.livros, livro_id) ->
          {{:error, :livro_inexistente}, estado}

        Map.has_key?(estado.emprestimos, chave) ->
          {{:error, :ja_emprestado}, estado}

        ativos(estado, livro_id) >= estado.livros[livro_id].copias ->
          {{:error, :sem_exemplares}, estado}

        true ->
          prazo = DateTime.add(DateTime.utc_now(), dias * 86_400, :second)
          registro = %{prazo: prazo, atrasado: false}
          {{:ok, registro}, %{estado | emprestimos: Map.put(estado.emprestimos, chave, registro)}}
      end
    end)
  end

  @doc "Registra a devolução de um livro."
  def devolver(livro_id, usuario) do
    Agent.get_and_update(__MODULE__, fn estado ->
      chave = {livro_id, usuario}

      if Map.has_key?(estado.emprestimos, chave) do
        {{:ok, chave}, %{estado | emprestimos: Map.delete(estado.emprestimos, chave)}}
      else
        {{:error, :nao_encontrado}, estado}
      end
    end)
  end

  @doc "Marca como atrasados os empréstimos vencidos. Retorna quantos foram marcados agora."
  def marcar_atrasos(agora) do
    Agent.get_and_update(__MODULE__, fn estado ->
      {novos, emprestimos} =
        Enum.reduce(estado.emprestimos, {0, estado.emprestimos}, fn
          {chave, %{prazo: prazo, atrasado: false} = registro}, {n, acc} ->
            if DateTime.compare(agora, prazo) == :gt do
              {n + 1, Map.put(acc, chave, %{registro | atrasado: true})}
            else
              {n, acc}
            end

          _, acc ->
            acc
        end)

      {novos, %{estado | emprestimos: emprestimos}}
    end)
  end

  # Conta os empréstimos ativos de um livro.
  defp ativos(estado, livro_id) do
    Enum.count(estado.emprestimos, fn {{l, _usuario}, _} -> l == livro_id end)
  end
end

defmodule Biblioteca.Emprestimos do
  @moduledoc """
  Coordena as operações de empréstimo. Cada operação roda em uma Task
  supervisionada, o que permite execução simultânea sem bloquear o GenServer.
  Uma verificação periódica de atrasos é agendada com Process.send_after/3.
  """
  use GenServer

  @intervalo_verificacao_ms 1_000
  @limite_concluidas 100

  ## API pública

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc "Inicia um empréstimo de forma assíncrona. Retorna {:ok, id_operacao} imediatamente."
  def emprestar(livro_id, usuario, dias \\ 14) do
    GenServer.call(__MODULE__, {:iniciar, :emprestimo, {livro_id, usuario, dias}})
  end

  @doc "Inicia uma devolução de forma assíncrona. Retorna {:ok, id_operacao} imediatamente."
  def devolver(livro_id, usuario) do
    GenServer.call(__MODULE__, {:iniciar, :devolucao, {livro_id, usuario}})
  end

  @doc "Consulta o estado atual: operações em andamento e concluídas recentes."
  def status do
    GenServer.call(__MODULE__, :status)
  end

  ## Callbacks do GenServer

  @impl true
  def init(_opts) do
    agendar_verificacao()
    {:ok, %{operacoes: %{}, tarefas: %{}, concluidas: []}}
  end

  @impl true
  def handle_call({:iniciar, tipo, args}, _from, state) do
    {id, state} = iniciar_operacao(state, tipo, args)
    {:reply, {:ok, id}, state}
  end

  def handle_call(:status, _from, state) do
    resumo = %{
      em_andamento: Map.values(state.operacoes),
      concluidas_recentes: Enum.take(state.concluidas, 10)
    }

    {:reply, resumo, state}
  end

  @impl true
  def handle_info(:verificar_atrasos, state) do
    agendar_verificacao()
    {_id, state} = iniciar_operacao(state, :verificacao, nil)
    {:noreply, state}
  end

  # Resultado de uma Task concluída com sucesso.
  def handle_info({ref, resultado}, state) when is_reference(ref) do
    Process.demonitor(ref, [:flush])
    {:noreply, concluir_tarefa(state, ref, resultado)}
  end

  # Task que falhou (crash).
  def handle_info({:DOWN, ref, :process, _pid, motivo}, state) do
    {:noreply, concluir_tarefa(state, ref, {:error, {:falha_interna, motivo}})}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  ## Funções privadas

  defp iniciar_operacao(state, tipo, args) do
    id = System.unique_integer([:positive, :monotonic])

    tarefa =
      Task.Supervisor.async_nolink(Biblioteca.TaskSup, fn ->
        executar(tipo, args)
      end)

    op = %{
      id: id,
      tipo: tipo,
      args: args,
      status: :em_andamento,
      iniciada_em: DateTime.utc_now()
    }

    state = %{
      state
      | operacoes: Map.put(state.operacoes, id, op),
        tarefas: Map.put(state.tarefas, tarefa.ref, id)
    }

    {id, state}
  end

  defp concluir_tarefa(state, ref, resultado) do
    case Map.pop(state.tarefas, ref) do
      {nil, _} ->
        state

      {id, tarefas} ->
        {op, operacoes} = Map.pop(state.operacoes, id)

        status = if match?({:ok, _}, resultado), do: :concluida, else: :falhou

        finalizada =
          Map.merge(op, %{
            status: status,
            resultado: resultado,
            finalizada_em: DateTime.utc_now()
          })

        %{
          state
          | tarefas: tarefas,
            operacoes: operacoes,
            concluidas: Enum.take([finalizada | state.concluidas], @limite_concluidas)
        }
    end
  end

  # Executa a lógica de cada tipo de operação (roda dentro da Task).
  defp executar(:emprestimo, {livro_id, usuario, dias}) do
    simular_latencia()
    Biblioteca.BancoFalso.registrar_emprestimo(livro_id, usuario, dias)
  end

  defp executar(:devolucao, {livro_id, usuario}) do
    simular_latencia()
    Biblioteca.BancoFalso.devolver(livro_id, usuario)
  end

  defp executar(:verificacao, _args) do
    {:ok, Biblioteca.BancoFalso.marcar_atrasos(DateTime.utc_now())}
  end

  # Simula latência de I/O de um banco real.
  defp simular_latencia, do: Process.sleep(Enum.random(200..600))

  defp agendar_verificacao do
    Process.send_after(self(), :verificar_atrasos, @intervalo_verificacao_ms)
  end
end

defmodule Biblioteca.Aplicacao do
  @moduledoc "Inicia a árvore de supervisão e executa uma demonstração."

  def executar_demo do
    filhos = [
      {Task.Supervisor, name: Biblioteca.TaskSup},
      Biblioteca.BancoFalso,
      Biblioteca.Emprestimos
    ]

    {:ok, _sup} = Supervisor.start_link(filhos, strategy: :one_for_one)

    # Operações disparadas em sequência, mas executadas concorrentemente.
    for usuario <- ["ana", "bruno", "carla"] do
      Biblioteca.Emprestimos.emprestar(1, usuario)
    end

    # Empréstimo com prazo negativo: já nasce atrasado (demonstra a verificação periódica).
    Biblioteca.Emprestimos.emprestar(2, "diego", -1)
    Biblioteca.Emprestimos.devolver(1, "ana")

    IO.inspect(Biblioteca.Emprestimos.status(), label: "Estado logo após os pedidos")

    Process.sleep(2_500)

    IO.inspect(Biblioteca.Emprestimos.status(), label: "Estado após processamento e verificações")
  end
end

Biblioteca.Aplicacao.executar_demo()
