# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de agendamento de compromissos, atendendo somente aos seguintes requisitos:
#
# 1. Implementar novas tentativas após falhas temporárias.
# 2. Executar determinadas operações periodicamente.
# 3. Persistir os dados para recuperação após reinicializações.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - Task
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# Arquivo único: agendador de compromissos com retentativas, execuções periódicas
# e persistência em disco.
#
# Uso (exemplo em iex):
#   {:ok, _} = Agendamento.Agenda.start_link(arquivo: "agenda.bin", intervalo_ms: 500)
#   :ok = Agendamento.Agenda.agendar("reuniao-1", "Reunião de planejamento", DateTime.utc_now())
#   Agendamento.Agenda.listar()
#
# Para instruir o agendador a executar uma ação real, passe `acao:` com uma função
# que recebe o agendamento e retorna {:ok, _} ou {:error, {:temporario, motivo}}
# (erros temporários são repetidos) ou {:error, motivo} (erros permanentes).

defmodule Agendamento.Armazenamento do
  @moduledoc """
  Persistência em disco. Grava em um arquivo temporário e depois renomeia.
  A renomeação é atômica no mesmo sistema de arquivos, então uma queda no meio
  da escrita não corrompe o arquivo principal.
  """

  @doc "Carrega os dados persistidos. Retorna `{:ok, mapa}` ou `{:error, motivo}`."
  def carregar(caminho) do
    case File.read(caminho) do
      {:ok, binario} ->
        try do
          # :safe impede a criação de átomos novos a partir de dados externos
          {:ok, :erlang.binary_to_term(binario, [:safe])}
        rescue
          ArgumentError -> {:error, :arquivo_corrompido}
        end

      {:error, :enoent} ->
        # Primeira execução: não há estado anterior
        {:ok, %{}}

      {:error, motivo} ->
        {:error, motivo}
    end
  end

  @doc "Salva os dados de forma atômica."
  def salvar(caminho, dados) do
    temporario = caminho <> ".tmp"

    with :ok <- File.write(temporario, :erlang.term_to_binary(dados)),
         :ok <- File.rename(temporario, caminho) do
      :ok
    end
  end
end

defmodule Agendamento.Retentativa do
  @moduledoc """
  Repete operações que falharam temporariamente, com backoff exponencial.
  Apenas retornos no formato `{:error, {:temporario, motivo}}` são repetidos.
  """

  @doc """
  Executa `fun` até obter sucesso, um erro permanente ou esgotar as tentativas.

  Opções:
    * `:max_tentativas` - número máximo de tentativas (padrão: 3)
    * `:backoff_ms` - atraso base entre tentativas (padrão: 200 ms, dobra a cada falha)
  """
  @spec executar((-> term()), keyword()) :: term()
  def executar(fun, opts \\ []) do
    max = Keyword.get(opts, :max_tentativas, 3)
    base = Keyword.get(opts, :backoff_ms, 200)
    tentar(fun, 1, max, base)
  end

  defp tentar(fun, tentativa, max, base) do
    case fun.() do
      {:error, {:temporario, _motivo}} when tentativa < max ->
        Process.sleep(base * Integer.pow(2, tentativa - 1))
        tentar(fun, tentativa + 1, max, base)

      resultado ->
        resultado
    end
  end
end

defmodule Agendamento.Agenda do
  @moduledoc """
  Servidor do agendador.

  * Persiste o estado a cada mudança, permitindo recuperação após reinicializações.
  * Executa periodicamente (a cada `intervalo_ms`) a verificação de compromissos vencidos.
  * Cada compromisso vencido é processado por uma `Task` separada, com retentativas.
  * Compromissos que estavam em andamento durante uma queda voltam a `:pendente`
    na recuperação (semântica "pelo menos uma vez").
  """

  use GenServer

  alias Agendamento.{Armazenamento, Retentativa}

  # ---------------------------------------------------------------------------
  # API pública
  # ---------------------------------------------------------------------------

  @doc """
  Inicia o agendador.

  Opções:
    * `:arquivo` - (obrigatório) caminho do arquivo de persistência
    * `:nome` - nome do processo registrado (padrão: `Agendamento.Agenda`)
    * `:intervalo_ms` - período da execução periódica (padrão: 1000 ms)
    * `:acao` - função executada para cada compromisso vencido
    * `:retentativa` - opções repassadas para `Agendamento.Retentativa.executar/2`
  """
  def start_link(opts) do
    nome = Keyword.get(opts, :nome, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: nome)
  end

  @doc "Agenda um compromisso com identificador único."
  def agendar(servidor \\ __MODULE__, id, descricao, quando) do
    GenServer.call(servidor, {:agendar, id, descricao, quando})
  end

  @doc "Cancela um compromisso que ainda está pendente."
  def cancelar(servidor \\ __MODULE__, id) do
    GenServer.call(servidor, {:cancelar, id})
  end

  @doc "Retorna todos os compromissos agendados."
  def listar(servidor \\ __MODULE__) do
    GenServer.call(servidor, :listar)
  end

  # ---------------------------------------------------------------------------
  # Inicialização e recuperação
  # ---------------------------------------------------------------------------

  @impl true
  def init(opts) do
    caminho = Keyword.fetch!(opts, :arquivo)

    case Armazenamento.carregar(caminho) do
      {:ok, dados} ->
        agendamentos = Map.new(dados, fn {id, ag} -> {id, recuperar(ag)} end)

        estado = %{
          arquivo: caminho,
          agendamentos: agendamentos,
          # mapa: referência da Task => id do compromisso
          em_execucao: %{},
          intervalo: Keyword.get(opts, :intervalo_ms, 1_000),
          acao: Keyword.get(opts, :acao, &executar_padrao/1),
          retentativa: Keyword.get(opts, :retentativa, [])
        }

        agendar_tick(estado.intervalo)
        {:ok, estado}

      {:error, motivo} ->
        {:stop, motivo}
    end
  end

  # Compromissos interrompidos por queda voltam a ficar pendentes
  defp recuperar(%{status: :em_andamento} = ag), do: %{ag | status: :pendente}
  defp recuperar(ag), do: ag

  # ---------------------------------------------------------------------------
  # Chamadas síncronas
  # ---------------------------------------------------------------------------

  @impl true
  def handle_call({:agendar, id, descricao, quando}, _from, estado) do
    if Map.has_key?(estado.agendamentos, id) do
      {:reply, {:error, :ja_existe}, estado}
    else
      ag = %{descricao: descricao, quando: quando, status: :pendente}
      estado = %{estado | agendamentos: Map.put(estado.agendamentos, id, ag)} |> persistir()
      {:reply, :ok, estado}
    end
  end

  def handle_call({:cancelar, id}, _from, estado) do
    case Map.fetch(estado.agendamentos, id) do
      {:ok, %{status: :pendente}} ->
        estado = %{estado | agendamentos: Map.delete(estado.agendamentos, id)} |> persistir()
        {:reply, :ok, estado}

      {:ok, _em_outro_estado} ->
        {:reply, {:error, :nao_cancelavel}, estado}

      :error ->
        {:reply, {:error, :nao_encontrado}, estado}
    end
  end

  def handle_call(:listar, _from, estado) do
    {:reply, estado.agendamentos, estado}
  end

  # ---------------------------------------------------------------------------
  # Execução periódica
  # ---------------------------------------------------------------------------

  @impl true
  def handle_info(:tick, estado) do
    # Reagenda o próximo ciclo antes de processar, garantindo periodicidade
    agendar_tick(estado.intervalo)

    agora = DateTime.utc_now()

    vencidos =
      for {id, %{status: :pendente, quando: quando} = ag} <- estado.agendamentos,
          DateTime.compare(quando, agora) != :gt,
          do: {id, ag}

    estado = Enum.reduce(vencidos, estado, &disparar/2)
    {:noreply, estado}
  end

  # Resultado de uma Task (mensagem enviada por Task.async ao terminar)
  def handle_info({ref, resultado}, estado) when is_reference(ref) do
    case Map.pop(estado.em_execucao, ref) do
      {nil, _} ->
        {:noreply, estado}

      {id, em_execucao} ->
        Process.demonitor(ref, [:flush])

        status = if sucesso?(resultado), do: :concluido, else: :falhou

        estado =
          %{
            estado
            | em_execucao: em_execucao,
              agendamentos: atualizar_status(estado.agendamentos, id, status)
          }
          |> persistir()

        {:noreply, estado}
    end
  end

  # Monitor de uma Task finalizada (já tratado acima; ignorado por segurança)
  def handle_info({:DOWN, _ref, :process, _pid, _motivo}, estado) do
    {:noreply, estado}
  end

  def handle_info(_msg, estado), do: {:noreply, estado}

  # ---------------------------------------------------------------------------
  # Funções privadas
  # ---------------------------------------------------------------------------

  defp disparar({id, ag}, estado) do
    acao = estado.acao
    opts = estado.retentativa

    # A Task executa a ação com retentativas sem bloquear o GenServer
    tarefa =
      Task.async(fn ->
        Retentativa.executar(fn -> seguro(acao, ag) end, opts)
      end)

    %{
      estado
      | agendamentos: atualizar_status(estado.agendamentos, id, :em_andamento),
        em_execucao: Map.put(estado.em_execucao, tarefa.ref, id)
    }
    |> persistir()
  end

  # Converte exceções em erro permanente, evitando derrubar a Task/GenServer
  defp seguro(acao, ag) do
    try do
      acao.(ag)
    rescue
      e -> {:error, {:excecao, Exception.message(e)}}
    end
  end

  defp sucesso?(:ok), do: true
  defp sucesso?({:ok, _}), do: true
  defp sucesso?(_), do: false

  defp atualizar_status(agendamentos, id, status) do
    Map.update!(agendamentos, id, &Map.put(&1, :status, status))
  end

  defp persistir(estado) do
    case Armazenamento.salvar(estado.arquivo, estado.agendamentos) do
      :ok -> :ok
      {:error, motivo} -> IO.warn("falha ao persistir agenda: #{inspect(motivo)}")
    end

    estado
  end

  defp agendar_tick(intervalo) do
    Process.send_after(self(), :tick, intervalo)
  end

  defp executar_padrao(ag) do
    IO.puts("Executando compromisso: #{ag.descricao}")
    {:ok, ag.descricao}
  end
end
