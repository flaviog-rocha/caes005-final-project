# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de votação online, atendendo somente aos seguintes requisitos:
#
# 1. Garantir a consistência dos dados durante operações concorrentes.
# 2. Permitir consultar o estado atual das operações em andamento.
# 3. Permitir limitar a quantidade de operações simultâneas.
# 4. Permitir cadastrar, consultar, atualizar e remover registros.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule SistemaVotacao do
  @moduledoc """
  Sistema de votação online concorrente, implementado com um único GenServer.

  Garantias:

    * Consistência: todo o estado vive em um único processo. Cada escrita
      (cadastrar, atualizar, remover, votar) é executada atomicamente dentro
      desse processo, após a validação de todos os invariantes. Não há
      condições de corrida entre leituras e escritas.

    * Limite de concorrência: no máximo `:limite` operações podem estar em
      execução ao mesmo tempo. As excedentes aguardam em uma fila FIFO, com
      tempo máximo de espera (`:timeout_fila`), após o qual recebem
      `{:error, :tempo_esgotado}`.

    * Observabilidade: `operacoes_em_andamento/1` informa as operações em
      execução e as que estão na fila.

    * Atualização otimista: `atualizar/4` aceita a versão esperada do
      registro e rejeita a escrita caso ela tenha sido alterada por outro
      cliente.

    * Quando um cliente morre durante uma operação, o monitor libera o slot
      automaticamente, evitando vazamento de capacidade.
  """

  use GenServer

  @limite_padrao 4
  @timeout_fila_padrao 5_000
  @status_validos [:aberta, :encerrada]

  ## API pública

  @doc """
  Inicia o sistema. Opções:

    * `:nome` - nome do processo registrado (padrão: `#{__MODULE__}`)
    * `:limite` - máximo de operações simultâneas (padrão: #{@limite_padrao})
    * `:timeout_fila` - tempo máximo em ms de espera na fila, ou `:infinity`
      (padrão: #{@timeout_fila_padrao})
  """
  def start_link(opts \\ []) do
    {nome, opts} = Keyword.pop(opts, :nome, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: nome)
  end

  @doc "Cadastra uma votação com título e opções. Retorna `{:ok, id}`."
  def cadastrar(servidor, titulo, opcoes) do
    operar(servidor, {:cadastrar, titulo}, fn ->
      GenServer.call(servidor, {:cadastrar, titulo, opcoes})
    end)
  end

  @doc "Consulta um registro pelo id. Retorna `{:ok, registro}` ou `{:error, :nao_encontrado}`."
  def consultar(servidor, id) do
    operar(servidor, {:consultar, id}, fn ->
      GenServer.call(servidor, {:consultar, id})
    end)
  end

  @doc "Lista todos os registros, ordenados por id."
  def listar(servidor) do
    operar(servidor, :listar, fn ->
      GenServer.call(servidor, :listar)
    end)
  end

  @doc """
  Atualiza `:titulo` e/ou `:status` (`:aberta` | `:encerrada`) de um registro.
  Se `versao_esperada` for informada e diferir da versão atual, retorna
  `{:error, :conflito}`.
  """
  def atualizar(servidor, id, attrs, versao_esperada \\ nil) do
    operar(servidor, {:atualizar, id}, fn ->
      GenServer.call(servidor, {:atualizar, id, attrs, versao_esperada})
    end)
  end

  @doc "Remove um registro pelo id."
  def remover(servidor, id) do
    operar(servidor, {:remover, id}, fn ->
      GenServer.call(servidor, {:remover, id})
    end)
  end

  @doc """
  Registra o voto de `votante` na opção `opcao` da votação `id`.
  Cada votante só pode votar uma vez e apenas em votações abertas.
  """
  def votar(servidor, id, votante, opcao) do
    operar(servidor, {:votar, id}, fn ->
      GenServer.call(servidor, {:votar, id, votante, opcao})
    end)
  end

  @doc """
  Retorna o estado atual das operações: limite, slots disponíveis,
  operações em execução e operações aguardando na fila.
  Esta consulta não consome slot.
  """
  def operacoes_em_andamento(servidor) do
    GenServer.call(servidor, :operacoes_em_andamento)
  end

  #执行 uma operação respeitando o limite de concorrência.
  # Adquire um slot, executa a função do cliente e libera o slot ao final,
  # mesmo que a função lance exceção.
  defp operar(servidor, operacao, fun) do
    case GenServer.call(servidor, {:adquirir, operacao, self()}, :infinity) do
      {:ok, mref} ->
        try do
          fun.()
        after
          GenServer.call(servidor, {:liberar, mref}, :infinity)
        end

      {:error, motivo} ->
        {:error, motivo}
    end
  end

  ## Callbacks do GenServer

  @impl true
  def init(opts) do
    {:ok,
     %{
       registros: %{},
       proximo_id: 1,
       limite: Keyword.get(opts, :limite, @limite_padrao),
       timeout_fila: Keyword.get(opts, :timeout_fila, @timeout_fila_padrao),
       em_execucao: %{},
       fila: :queue.new()
     }}
  end

  # Controle de concorrência

  @impl true
  def handle_call({:adquirir, operacao, pid}, from, st) do
    ref = make_ref()

    entrada = %{
      ref: ref,
      from: from,
      pid: pid,
      operacao: operacao,
      enfileirado_em: agora()
    }

    # Se a espera for limitada, agenda a expiração da entrada na fila.
    # Caso a entrada já tenha sido concedida, a mensagem é ignorada.
    if st.timeout_fila != :infinity do
      Process.send_after(self(), {:expirar_fila, ref}, st.timeout_fila)
    end

    st = %{st | fila: :queue.in(entrada, st.fila)}
    {:noreply, despachar(st)}
  end

  def handle_call({:liberar, mref}, _from, st) do
    Process.demonitor(mref, [:flush])
    st = %{st | em_execucao: Map.delete(st.em_execucao, mref)}
    {:reply, :ok, despachar(st)}
  end

  def handle_call(:operacoes_em_andamento, _from, st) do
    resposta = %{
      limite: st.limite,
      disponiveis: st.limite - map_size(st.em_execucao),
      em_execucao: Map.values(st.em_execucao),
      aguardando:
        st.fila
        |> :queue.to_list()
        |> Enum.map(&Map.take(&1, [:operacao, :pid, :enfileirado_em]))
    }

    {:reply, resposta, st}
  end

  # Operações sobre os dados (executadas atomicamente no processo)

  def handle_call({:cadastrar, titulo, opcoes}, _from, st) do
    if titulo_valido?(titulo) and opcoes_validas?(opcoes) do
      id = st.proximo_id

      registro = %{
        id: id,
        titulo: titulo,
        opcoes: Map.new(opcoes, &{&1, 0}),
        votantes: MapSet.new(),
        status: :aberta,
        versao: 1,
        criado_em: agora()
      }

      st = %{
        st
        | registros: Map.put(st.registros, id, registro),
          proximo_id: id + 1
      }

      {:reply, {:ok, id}, st}
    else
      {:reply, {:error, :dados_invalidos}, st}
    end
  end

  def handle_call({:consultar, id}, _from, st) do
    {:reply, buscar(st, id), st}
  end

  def handle_call(:listar, _from, st) do
    lista =
      st.registros
      |> Map.values()
      |> Enum.sort_by(& &1.id)

    {:reply, {:ok, lista}, st}
  end

  def handle_call({:atualizar, id, attrs, versao_esperada}, _from, st) do
    with {:ok, r} <- buscar(st, id),
         :ok <- verificar_versao(r, versao_esperada),
         campos = Map.take(attrs, [:titulo, :status]),
         :ok <- validar_campos(campos) do
      r = r |> Map.merge(campos) |> Map.update!(:versao, &(&1 + 1))
      st = %{st | registros: Map.put(st.registros, id, r)}
      {:reply, {:ok, r}, st}
    else
      erro -> {:reply, erro, st}
    end
  end

  def handle_call({:remover, id}, _from, st) do
    case Map.fetch(st.registros, id) do
      {:ok, _} ->
        {:reply, :ok, %{st | registros: Map.delete(st.registros, id)}}

      :error ->
        {:reply, {:error, :nao_encontrado}, st}
    end
  end

  def handle_call({:votar, id, votante, opcao}, _from, st) do
    with {:ok, r} <- buscar(st, id),
         :ok <- verificar_aberta(r),
         :ok <- verificar_opcao(r, opcao),
         :ok <- verificar_nao_votou(r, votante) do
      r = %{
        r
        | opcoes: Map.update!(r.opcoes, opcao, &(&1 + 1)),
          votantes: MapSet.put(r.votantes, votante),
          versao: r.versao + 1
      }

      st = %{st | registros: Map.put(st.registros, id, r)}
      {:reply, :ok, st}
    else
      erro -> {:reply, erro, st}
    end
  end

  @impl true
  def handle_info({:expirar_fila, ref}, st) do
    {alvos, restantes} = Enum.split_with(:queue.to_list(st.fila), &(&1.ref == ref))

    case alvos do
      [entrada] ->
        GenServer.reply(entrada.from, {:error, :tempo_esgotado})
        {:noreply, %{st | fila: :queue.from_list(restantes)}}

      [] ->
        # Já foi concedida ou removida; nada a fazer.
        {:noreply, st}
    end
  end

  # Cliente que possui um slot morreu sem liberá-lo.
  def handle_info({:DOWN, mref, :process, _pid, _motivo}, st) do
    st = %{st | em_execucao: Map.delete(st.em_execucao, mref)}
    {:noreply, despachar(st)}
  end

  def handle_info(_msg, st), do: {:noreply, st}

  ## Funções auxiliares privadas

  # Concede slots livres aos próximos da fila, respeitando o limite.
  defp despachar(st) do
    with true <- map_size(st.em_execucao) < st.limite,
         {{:value, entrada}, resto} <- :queue.out(st.fila) do
      mref = Process.monitor(entrada.pid)
      GenServer.reply(entrada.from, {:ok, mref})

      info = %{
        operacao: entrada.operacao,
        pid: entrada.pid,
        enfileirado_em: entrada.enfileirado_em,
        iniciado_em: agora()
      }

      st = %{st | fila: resto, em_execucao: Map.put(st.em_execucao, mref, info)}
      despachar(st)
    else
      _ -> st
    end
  end

  defp buscar(st, id) do
    case Map.fetch(st.registros, id) do
      {:ok, r} -> {:ok, r}
      :error -> {:error, :nao_encontrado}
    end
  end

  defp verificar_versao(_registro, nil), do: :ok
  defp verificar_versao(%{versao: v}, v), do: :ok
  defp verificar_versao(_registro, _versao), do: {:error, :conflito}

  defp validar_campos(campos) do
    cond do
      Map.has_key?(campos, :titulo) and not titulo_valido?(campos.titulo) ->
        {:error, :dados_invalidos}

      Map.has_key?(campos, :status) and not (campos.status in @status_validos) ->
        {:error, :dados_invalidos}

      true ->
        :ok
    end
  end

  defp verificar_aberta(%{status: :aberta}), do: :ok
  defp verificar_aberta(_registro), do: {:error, :votacao_encerrada}

  defp verificar_opcao(%{opcoes: opcoes}, opcao) do
    if Map.has_key?(opcoes, opcao), do: :ok, else: {:error, :opcao_invalida}
  end

  defp verificar_nao_votou(%{votantes: votantes}, votante) do
    if MapSet.member?(votantes, votante), do: {:error, :ja_votou}, else: :ok
  end

  defp titulo_valido?(titulo), do: is_binary(titulo) and titulo != ""

  defp opcoes_validas?(opcoes) do
    is_list(opcoes) and
      opcoes != [] and
      Enum.all?(opcoes, &(is_binary(&1) and &1 != "")) and
      Enum.uniq(opcoes) == opcoes
  end

  defp agora, do: System.system_time(:millisecond)
end
