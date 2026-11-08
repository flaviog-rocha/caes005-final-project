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

defmodule Agendamento do
  @moduledoc """
  Sistema de agendamento de compromissos com:

    * novas tentativas (retry com backoff exponencial) para falhas temporárias;
    * execução periódica da verificação de compromissos vencidos;
    * persistência em disco para recuperação após reinicializações.

  Uso:

      {:ok, _} = Agendamento.start_link(
        caminho: "agendamentos.bin",
        intervalo_ms: 5_000,
        notificador: fn compromisso -> MeuModulo.notificar(compromisso) end
      )

      {:ok, id} = Agendamento.agendar("Reunião", DateTime.add(DateTime.utc_now(), 60))
      Agendamento.listar()
      Agendamento.cancelar(id)

  O `notificador` deve retornar `:ok`, `{:error, :temporario}` (será reexecutado)
  ou `{:error, outro_motivo}` (falha definitiva).
  """

  use GenServer
  require Logger

  @intervalo_padrao 5_000

  # ---------------------------------------------------------------------------
  # API pública
  # ---------------------------------------------------------------------------

  @doc "Inicia o agendador. Opções: :nome, :caminho, :intervalo_ms, :notificador."
  def start_link(opts \\ []) do
    nome = Keyword.get(opts, :nome, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: nome)
  end

  @doc "Agenda um compromisso para o instante `em` (DateTime UTC)."
  def agendar(titulo, %DateTime{} = em, servidor \\ __MODULE__) do
    GenServer.call(servidor, {:agendar, titulo, em})
  end

  @doc "Cancela um compromisso pelo id."
  def cancelar(id, servidor \\ __MODULE__) do
    GenServer.call(servidor, {:cancelar, id})
  end

  @doc "Lista todos os compromissos ordenados por id."
  def listar(servidor \\ __MODULE__) do
    GenServer.call(servidor, :listar)
  end

  # ---------------------------------------------------------------------------
  # Callbacks do GenServer
  # ---------------------------------------------------------------------------

  @impl true
  def init(opts) do
    caminho = Keyword.get(opts, :caminho, "agendamentos.bin")
    intervalo = Keyword.get(opts, :intervalo_ms, @intervalo_padrao)
    notificador = Keyword.get(opts, :notificador, fn _compromisso -> :ok end)

    case Agendamento.Armazenamento.carregar(caminho) do
      {:ok, %{compromissos: compromissos, proximo_id: proximo_id}} ->
        estado = %{
          caminho: caminho,
          intervalo: intervalo,
          notificador: notificador,
          compromissos: recuperar(compromissos),
          proximo_id: proximo_id,
          # mapa %{referência_da_task => id_do_compromisso}
          tarefas: %{}
        }

        Process.send_after(self(), :verificar, intervalo)
        {:ok, estado}

      {:error, motivo} ->
        {:stop, {:falha_ao_carregar, motivo}}
    end
  end

  @impl true
  def handle_call({:agendar, titulo, em}, _from, estado) do
    id = estado.proximo_id

    compromisso = %{id: id, titulo: titulo, em: em, status: :agendado}

    estado =
      %{
        estado
        | compromissos: Map.put(estado.compromissos, id, compromisso),
          proximo_id: id + 1
      }
      |> persistir()

    {:reply, {:ok, id}, estado}
  end

  def handle_call({:cancelar, id}, _from, estado) do
    if Map.has_key?(estado.compromissos, id) do
      estado =
        %{estado | compromissos: Map.delete(estado.compromissos, id)}
        |> persistir()

      {:reply, :ok, estado}
    else
      {:reply, {:error, :nao_encontrado}, estado}
    end
  end

  def handle_call(:listar, _from, estado) do
    lista =
      estado.compromissos
      |> Map.values()
      |> Enum.sort_by(& &1.id)

    {:reply, lista, estado}
  end

  # Execução periódica: dispara os compromissos vencidos.
  @impl true
  def handle_info(:verificar, estado) do
    agora = DateTime.utc_now()

    devidos =
      estado.compromissos
      |> Map.values()
      |> Enum.filter(fn c ->
        c.status == :agendado and DateTime.compare(c.em, agora) != :gt
      end)

    estado = Enum.reduce(devidos, estado, &disparar/2)

    Process.send_after(self(), :verificar, estado.intervalo)

    estado = if devidos == [], do: estado, else: persistir(estado)
    {:noreply, estado}
  end

  # Resultado de uma Task concluída com sucesso.
  def handle_info({ref, resultado}, estado) when is_reference(ref) do
    case Map.pop(estado.tarefas, ref) do
      {nil, _} ->
        {:noreply, estado}

      {id, tarefas} ->
        Process.demonitor(ref, [:flush])

        status = if resultado == :ok, do: :notificado, else: :falhou

        estado =
          %{estado | tarefas: tarefas}
          |> atualizar_status(id, status)
          |> persistir()

        {:noreply, estado}
    end
  end

  # Task que terminou abruptamente (crash).
  def handle_info({:DOWN, ref, :process, _pid, motivo}, estado) do
    case Map.pop(estado.tarefas, ref) do
      {nil, _} ->
        {:noreply, estado}

      {id, tarefas} ->
        Logger.error("Tarefa do compromisso #{id} falhou: #{inspect(motivo)}")

        estado =
          %{estado | tarefas: tarefas}
          |> atualizar_status(id, :falhou)
          |> persistir()

        {:noreply, estado}
    end
  end

  def handle_info(_msg, estado), do: {:noreply, estado}

  # ---------------------------------------------------------------------------
  # Funções privadas
  # ---------------------------------------------------------------------------

  # Inicia uma Task (com retry) para notificar o compromisso.
  defp disparar(compromisso, estado) do
    notificador = estado.notificador

    task =
      Task.async_nolink(fn ->
        Agendamento.Retentativa.executar(fn -> notificador.(compromisso) end)
      end)

    %{
      estado
      | compromissos: Map.put(estado.compromissos, compromisso.id, %{compromisso | status: :processando}),
        tarefas: Map.put(estado.tarefas, task.ref, compromisso.id)
    }
  end

  defp atualizar_status(estado, id, status) do
    case Map.fetch(estado.compromissos, id) do
      {:ok, compromisso} ->
        %{estado | compromissos: Map.put(estado.compromissos, id, %{compromisso | status: status})}

      :error ->
        # Compromisso foi cancelado enquanto a tarefa executava.
        estado
    end
  end

  # Tarefas em andamento não sobrevivem a reinicializações; ao recuperar,
  # elas voltam a ficar aguardando notificação.
  defp recuperar(compromissos) do
    Map.new(compromissos, fn {id, c} ->
      status = if c.status == :processando, do: :agendado, else: c.status
      {id, %{c | status: status}}
    end)
  end

  defp persistir(estado) do
    dados = %{compromissos: estado.compromissos, proximo_id: estado.proximo_id}

    case Agendamento.Armazenamento.salvar(estado.caminho, dados) do
      :ok ->
        :ok

      {:error, motivo} ->
        Logger.error("Falha ao persistir agendamentos: #{inspect(motivo)}")
    end

    estado
  end
end

defmodule Agendamento.Armazenamento do
  @moduledoc false

  @vazio %{compromissos: %{}, proximo_id: 1}

  @doc "Carrega os dados do disco. Retorna dados vazios se o arquivo não existir."
  def carregar(caminho) do
    case File.read(caminho) do
      {:ok, binario} ->
        try do
          {:ok, :erlang.binary_to_term(binario, [:safe])}
        rescue
          ArgumentError -> {:error, :corrompido}
        end

      {:error, :enoent} ->
        {:ok, @vazio}

      {:error, motivo} ->
        {:error, motivo}
    end
  end

  @doc "Grava os dados de forma atômica (escreve em arquivo temporário e renomeia)."
  def salvar(caminho, dados) do
    temporario = caminho <> ".tmp"

    with :ok <- File.write(temporario, :erlang.term_to_binary(dados)),
         :ok <- File.rename(temporario, caminho) do
      :ok
    end
  end
end

defmodule Agendamento.Retentativa do
  @moduledoc false

  @tentativas_padrao 3
  @espera_inicial_ms 200

  @doc """
  Executa `fun` e reexecuta em caso de `{:error, :temporario}`, com backoff
  exponencial. Exceções são convertidas em `{:error, {:excecao, msg}}` e não
  são reexecutadas.
  """
  def executar(fun, tentativas \\ @tentativas_padrao, espera \\ @espera_inicial_ms) do
    resultado =
      try do
        fun.()
      rescue
        e -> {:error, {:excecao, Exception.message(e)}}
      end

    case resultado do
      {:error, :temporario} when tentativas > 1 ->
        Process.sleep(espera)
        executar(fun, tentativas - 1, espera * 2)

      outro ->
        outro
    end
  end
end
