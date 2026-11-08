# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de votação online, atendendo somente aos seguintes requisitos:
#
# 1. Garantir a consistência dos dados durante operações concorrentes.
# 2. Permitir consultar o estado atual das operações em andamento.
# 3. Permitir limitar a quantidade de operações simultâneas.
# 4. Permitir cadastrar, consultar, atualizar e remover registros.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule VotacaoOnline do
  @moduledoc """
  Sistema de votação online com GenServer.

  Consistência: todo o estado (enquetes e votos) fica em um único processo.
  Cada comando é aplicado atomicamente, evitando condições de corrida como
  votos duplicados ou alterações concorrentes na mesma enquete.

  Operações em andamento: cada operação recebe um identificador e pode ser
  consultada com `operacoes/1`, que lista as que estão executando e as que
  aguardam na fila.

  Limite de concorrência: no máximo `:max_concorrencia` operações ativas ao
  mesmo tempo. As excedentes aguardam em fila FIFO. Se o processo chamador
  morrer, sua vaga é liberada automaticamente via monitor.

  CRUD: enquetes podem ser criadas, consultadas, atualizadas e removidas.
  """

  use GenServer

  # ---------------------------------------------------------------
  # API pública
  # ---------------------------------------------------------------

  @doc "Inicia o sistema. Opções: :name (padrão __MODULE__) e :max_concorrencia (padrão 10)."
  def start_link(opts \\ []) do
    {nome, opts} = Keyword.pop(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: nome)
  end

  @doc "Cadastra uma enquete. Retorna {:ok, id} ou {:error, motivo}."
  def criar(servidor \\ __MODULE__, titulo, opcoes) do
    executar(servidor, :criar, {:criar, titulo, opcoes})
  end

  @doc "Consulta uma enquete pelo id, incluindo a contagem de votos."
  def consultar(servidor \\ __MODULE__, id) do
    executar(servidor, :consultar, {:consultar, id})
  end

  @doc "Lista todas as enquetes com seus resumos."
  def listar(servidor \\ __MODULE__) do
    executar(servidor, :listar, :listar)
  end

  @doc "Atualiza título e/ou opções. Opções não podem mudar após a primeira votação."
  def atualizar(servidor \\ __MODULE__, id, attrs) do
    executar(servidor, :atualizar, {:atualizar, id, attrs})
  end

  @doc "Remove uma enquete e seus votos."
  def remover(servidor \\ __MODULE__, id) do
    executar(servidor, :remover, {:remover, id})
  end

  @doc "Registra o voto de um eleitor (um voto por eleitor por enquete)."
  def votar(servidor \\ __MODULE__, id, eleitor, opcao) do
    executar(servidor, :votar, {:votar, id, eleitor, opcao})
  end

  @doc "Retorna o estado atual das operações: limite, em execução e aguardando."
  def operacoes(servidor \\ __MODULE__) do
    GenServer.call(servidor, :operacoes)
  end

  # ---------------------------------------------------------------
  # Controle de concorrência (lado do cliente)
  # ---------------------------------------------------------------

  # Adquire uma vaga (bloqueia se o limite estiver atingido), executa o
  # comando no processo servidor e libera a vaga, mesmo em caso de erro.
  defp executar(servidor, tipo, comando) do
    {:ok, op_id} = GenServer.call(servidor, {:adquirir, tipo}, :infinity)

    try do
      GenServer.call(servidor, {:executar, op_id, comando}, :infinity)
    after
      GenServer.cast(servidor, {:liberar, op_id})
    end
  end

  # ---------------------------------------------------------------
  # Callbacks do GenServer
  # ---------------------------------------------------------------

  @impl true
  def init(opts) do
    {:ok,
     %{
       enquetes: %{},
       proximo_id: 1,
       max: Keyword.get(opts, :max_concorrencia, 10),
       ops: %{},
       proximo_op: 1,
       fila: :queue.new(),
       ativas: 0
     }}
  end

  @impl true
  def handle_call({:adquirir, tipo}, {pid, _} = de, state) do
    op_id = state.proximo_op
    monitor = Process.monitor(pid)

    op = %{
      tipo: tipo,
      status: :aguardando,
      pid: pid,
      monitor: monitor,
      de: de,
      criado_em: System.system_time(:millisecond)
    }

    state = %{
      state
      | proximo_op: op_id + 1,
        ops: Map.put(state.ops, op_id, op),
        fila: :queue.in(op_id, state.fila)
    }

    {:noreply, despachar(state)}
  end

  def handle_call({:executar, op_id, comando}, _from, state) do
    case Map.fetch(state.ops, op_id) do
      {:ok, %{status: :executando}} ->
        {resposta, novo_estado} = aplicar(comando, state)
        {:reply, resposta, novo_estado}

      _ ->
        {:reply, {:error, :operacao_invalida}, state}
    end
  end

  def handle_call(:operacoes, _from, state) do
    lista =
      state.ops
      |> Enum.map(fn {id, op} ->
        %{id: id, tipo: op.tipo, status: op.status, criado_em: op.criado_em}
      end)
      |> Enum.sort_by(& &1.id)

    resposta = %{
      limite: state.max,
      em_execucao: Enum.filter(lista, &(&1.status == :executando)),
      aguardando: Enum.filter(lista, &(&1.status == :aguardando))
    }

    {:reply, resposta, state}
  end

  @impl true
  def handle_cast({:liberar, op_id}, state) do
    {:noreply, liberar(op_id, state)}
  end

  @impl true
  def handle_info({:DOWN, ref, :process, _pid, _motivo}, state) do
    case Enum.find(state.ops, fn {_id, op} -> op.monitor == ref end) do
      {op_id, _op} -> {:noreply, liberar(op_id, state)}
      nil -> {:noreply, state}
    end
  end

  def handle_info(_msg, state), do: {:noreply, state}

  # ---------------------------------------------------------------
  # Fila e vagas
  # ---------------------------------------------------------------

  # Concede vagas a operações da fila enquanto houver capacidade.
  defp despachar(%{ativas: ativas, max: max} = state) when ativas >= max, do: state

  defp despachar(state) do
    case :queue.out(state.fila) do
      {{:value, op_id}, fila} ->
        op = Map.fetch!(state.ops, op_id)
        GenServer.reply(op.de, {:ok, op_id})

        ops = Map.put(state.ops, op_id, %{op | status: :executando, de: nil})
        despachar(%{state | fila: fila, ops: ops, ativas: state.ativas + 1})

      {:empty, _} ->
        state
    end
  end

  # Remove a operação (executando ou aguardando), libera a vaga e
  # despacha a próxima da fila.
  defp liberar(op_id, state) do
    case Map.pop(state.ops, op_id) do
      {nil, _ops} ->
        state

      {op, ops} ->
        Process.demonitor(op.monitor, [:flush])

        state = %{
          state
          | ops: ops,
            fila: :queue.filter(&(&1 != op_id), state.fila)
        }

        state =
          if op.status == :executando,
            do: %{state | ativas: state.ativas - 1},
            else: state

        despachar(state)
    end
  end

  # ---------------------------------------------------------------
  # Comandos de domínio (executados atomicamente no servidor)
  # ---------------------------------------------------------------

  defp aplicar({:criar, titulo, opcoes}, state) do
    with :ok <- validar_titulo(titulo),
         :ok <- validar_opcoes(opcoes) do
      id = state.proximo_id
      enquete = %{titulo: titulo, opcoes: opcoes, votos: %{}}

      {{:ok, id}, %{state | enquetes: Map.put(state.enquetes, id, enquete), proximo_id: id + 1}}
    else
      erro -> {erro, state}
    end
  end

  defp aplicar({:consultar, id}, state) do
    case buscar(state, id) do
      {:ok, enquete} -> {{:ok, resumo(id, enquete)}, state}
      erro -> {erro, state}
    end
  end

  defp aplicar(:listar, state) do
    lista =
      state.enquetes
      |> Enum.map(fn {id, enquete} -> resumo(id, enquete) end)
      |> Enum.sort_by(& &1.id)

    {{:ok, lista}, state}
  end

  defp aplicar({:atualizar, id, attrs}, state) do
    with {:ok, enquete} <- buscar(state, id),
         {:ok, nova} <- atualizar_campos(enquete, attrs) do
      {{:ok, resumo(id, nova)}, put_enquete(state, id, nova)}
    else
      erro -> {erro, state}
    end
  end

  defp aplicar({:remover, id}, state) do
    case buscar(state, id) do
      {:ok, _} ->
        {:ok, %{state | enquetes: Map.delete(state.enquetes, id)}}

      erro ->
        {erro, state}
    end
  end

  defp aplicar({:votar, id, eleitor, opcao}, state) do
    with {:ok, enquete} <- buscar(state, id),
         :ok <- validar_voto(enquete, eleitor, opcao) do
      nova = %{enquete | votos: Map.put(enquete.votos, eleitor, opcao)}
      {:ok, put_enquete(state, id, nova)}
    else
      erro -> {erro, state}
    end
  end

  # ---------------------------------------------------------------
  # Auxiliares
  # ---------------------------------------------------------------

  defp buscar(state, id) do
    case Map.fetch(state.enquetes, id) do
      {:ok, enquete} -> {:ok, enquete}
      :error -> {:error, :nao_encontrada}
    end
  end

  defp put_enquete(state, id, enquete) do
    %{state | enquetes: Map.put(state.enquetes, id, enquete)}
  end

  defp atualizar_campos(enquete, attrs) do
    titulo = Map.get(attrs, :titulo, enquete.titulo)
    opcoes = Map.get(attrs, :opcoes, enquete.opcoes)

    with :ok <- validar_titulo(titulo),
         :ok <- validar_opcoes(opcoes),
         :ok <- validar_alteracao_opcoes(enquete, opcoes) do
      {:ok, %{enquete | titulo: titulo, opcoes: opcoes}}
    end
  end

  defp validar_alteracao_opcoes(%{votos: votos, opcoes: atuais}, novas)
       when map_size(votos) > 0 and atuais != novas,
       do: {:error, :enquete_com_votos}

  defp validar_alteracao_opcoes(_enquete, _novas), do: :ok

  defp validar_titulo(titulo) when is_binary(titulo) and titulo != "", do: :ok
  defp validar_titulo(_), do: {:error, :titulo_invalido}

  defp validar_opcoes(opcoes) when is_list(opcoes) do
    cond do
      length(opcoes) < 2 -> {:error, :opcoes_insuficientes}
      length(Enum.uniq(opcoes)) != length(opcoes) -> {:error, :opcoes_duplicadas}
      true -> :ok
    end
  end

  defp validar_opcoes(_), do: {:error, :opcoes_invalidas}

  defp validar_voto(enquete, eleitor, opcao) do
    cond do
      opcao not in enquete.opcoes -> {:error, :opcao_invalida}
      Map.has_key?(enquete.votos, eleitor) -> {:error, :voto_duplicado}
      true -> :ok
    end
  end

  defp resumo(id, enquete) do
    contagem = Enum.frequencies(Map.values(enquete.votos))

    %{
      id: id,
      titulo: enquete.titulo,
      opcoes: enquete.opcoes,
      votos_totais: map_size(enquete.votos),
      contagem: Map.new(enquete.opcoes, &{&1, Map.get(contagem, &1, 0)})
    }
  end
end
