# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de uma fila de tarefas, atendendo somente aos seguintes requisitos:
#
# 1. Persistir os dados para recuperação após reinicializações.
# 2. Permitir comunicação entre diferentes processos.
# 3. Recuperar processos após falhas inesperadas.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - Agent
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# Arquivo único: task_queue.exs
# Execução: elixir task_queue.exs (ou importe o módulo TaskQueue em uma aplicação)

defmodule TaskQueue do
  @moduledoc """
  Fila de tarefas baseada em Agent, com:

    * Persistência em disco (escrita atômica: arquivo temporário + rename),
      recarregada na inicialização.
    * Comunicação entre processos via API pública (qualquer processo pode
      enfileirar/reivindicar tarefas) e notificações por mensagens para
      processos inscritos.
    * Recuperação após falhas: o Agent é supervisionado. Ao reiniciar, as
      tarefas que estavam em processamento (reivindicadas e não confirmadas
      com `ack/2`) voltam para o início da fila (semântica at-least-once).

  Cada mutação é persistida antes de responder, portanto a resposta ao
  chamador só ocorre após o estado estar em disco.
  """

  @default_path "task_queue.state"

  # ---------------------------------------------------------------------------
  # Especificação de filho para supervisores
  # ---------------------------------------------------------------------------

  def child_spec(opts) do
    name = Keyword.get(opts, :name, __MODULE__)

    %{
      id: name,
      start: {__MODULE__, :start_link, [opts]},
      restart: :permanent,
      type: :worker
    }
  end

  # ---------------------------------------------------------------------------
  # Inicialização
  # ---------------------------------------------------------------------------

  @doc "Inicia o Agent carregando o estado persistido (se existir)."
  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    path = Keyword.get(opts, :path, @default_path)

    Agent.start_link(fn -> load_state(path) end, name: name)
  end

  # ---------------------------------------------------------------------------
  # API pública
  # ---------------------------------------------------------------------------

  @doc "Adiciona uma tarefa ao final da fila. Retorna {:ok, id}."
  def enqueue(payload, server \\ __MODULE__) do
    Agent.get_and_update(server, fn state ->
      task = %{id: state.next_id, payload: payload, enqueued_at: now()}

      new_state =
        %{state | next_id: state.next_id + 1, pending: :queue.in(task, state.pending)}
        |> persist!()
        |> notify({:task_queue, :enqueued, task})

      {{:ok, task.id}, new_state}
    end)
  end

  @doc """
  Reivindica a próxima tarefa pendente, movendo-a para `in_progress`.
  Retorna {:ok, task} ou :empty.
  """
  def claim(server \\ __MODULE__) do
    Agent.get_and_update(server, fn state ->
      case :queue.out(state.pending) do
        {{:value, task}, rest} ->
          new_state =
            %{
              state
              | pending: rest,
                in_progress: Map.put(state.in_progress, task.id, task)
            }
            |> persist!()

          {{:ok, task}, new_state}

        {:empty, _} ->
          {:empty, state}
      end
    end)
  end

  @doc "Confirma a conclusão de uma tarefa reivindicada. Retorna :ok ou {:error, :not_found}."
  def ack(id, server \\ __MODULE__) do
    Agent.get_and_update(server, fn state ->
      if Map.has_key?(state.in_progress, id) do
        new_state =
          %{state | in_progress: Map.delete(state.in_progress, id)}
          |> persist!()

        {:ok, new_state}
      else
        {{:error, :not_found}, state}
      end
    end)
  end

  @doc """
  Devolve uma tarefa reivindicada ao início da fila (ex.: falha no processamento).
  Retorna :ok ou {:error, :not_found}.
  """
  def release(id, server \\ __MODULE__) do
    Agent.get_and_update(server, fn state ->
      case Map.fetch(state.in_progress, id) do
        {:ok, task} ->
          new_state =
            %{
              state
              | in_progress: Map.delete(state.in_progress, id),
                pending: :queue.in_r(task, state.pending)
            }
            |> persist!()

          {:ok, new_state}

        :error ->
          {{:error, :not_found}, state}
      end
    end)
  end

  @doc """
  Inscreve o processo chamador para receber notificações como
  `{:task_queue, :enqueued, task}`. Inscrições não são persistidas.
  """
  def subscribe(server \\ __MODULE__) do
    pid = self()

    Agent.update(server, fn state ->
      %{state | subscribers: Enum.uniq([pid | state.subscribers])}
    end)
  end

  @doc "Retorna estatísticas atuais da fila."
  def stats(server \\ __MODULE__) do
    Agent.get(server, fn state ->
      %{
        pending: :queue.len(state.pending),
        in_progress: map_size(state.in_progress),
        next_id: state.next_id
      }
    end)
  end

  # ---------------------------------------------------------------------------
  # Estado e persistência (funções privadas)
  # ---------------------------------------------------------------------------

  defp now, do: System.system_time(:millisecond)

  defp initial_state(path) do
    %{
      path: path,
      pending: :queue.new(),
      in_progress: %{},
      next_id: 1,
      subscribers: []
    }
  end

  # Carrega o estado do disco e recoloca no início da fila as tarefas que
  # estavam em processamento quando o processo anterior caiu.
  defp load_state(path) do
    case File.read(path) do
      {:ok, bin} ->
        data = :erlang.binary_to_term(bin, [:safe])

        recovered = data.in_progress |> Map.values() |> Enum.sort_by(& &1.id)

        pending =
          :queue.join(
            :queue.from_list(recovered),
            :queue.from_list(data.pending)
          )

        %{initial_state(path) | pending: pending, next_id: data.next_id}
        |> persist!()

      {:error, :enoent} ->
        initial_state(path)

      {:error, reason} ->
        raise "Falha ao carregar a fila de #{path}: #{inspect(reason)}"
    end
  end

  # Escrita atômica: grava em arquivo temporário, sincroniza e renomeia.
  defp persist!(%{path: path} = state) do
    data = %{
      pending: :queue.to_list(state.pending),
      in_progress: state.in_progress,
      next_id: state.next_id
    }

    bin = :erlang.term_to_binary(data)
    tmp = path <> ".tmp"

    {:ok, fd} = File.open(tmp, [:write, :binary])
    :ok = IO.binwrite(fd, bin)
    :ok = :file.sync(fd)
    :ok = File.close(fd)
    :ok = File.rename(tmp, path)

    state
  end

  # Envia a mensagem a inscritos vivos (processos locais) e remove os mortos.
  defp notify(state, message) do
    alive = Enum.filter(state.subscribers, &Process.alive?/1)
    Enum.each(alive, &send(&1, message))
    %{state | subscribers: alive}
  end
end

defmodule TaskQueue.Demo do
  @moduledoc """
  Demonstração: persistência, comunicação entre processos e recuperação
  após falha simulada do Agent.
  """

  def run do
    path = "task_queue_demo.state"
    File.rm(path)

    # Supervisor reinicia o Agent automaticamente em caso de falha.
    {:ok, _sup} =
      Supervisor.start_link(
        [{TaskQueue, name: TaskQueue, path: path}],
        strategy: :one_for_one
      )

    # Comunicação: um processo produtor e um consumidor inscrito.
    TaskQueue.subscribe()

    producer =
      spawn(fn ->
        Enum.each(1..3, fn i -> TaskQueue.enqueue(%{job: "tarefa_#{i}"}) end)
      end)

    Process.sleep(100)
    _ = producer

    receive_notifications()

    {:ok, task} = TaskQueue.claim()
    IO.inspect(task, label: "tarefa reivindicada (ainda não confirmada)")

    # Falha inesperada: o Agent é morto antes do ack.
    IO.puts("Simulando falha do processo da fila...")
    Process.exit(Process.whereis(TaskQueue), :kill)
    Process.sleep(100)

    # Após a recuperação, a tarefa em processamento volta à fila.
    IO.inspect(TaskQueue.stats(), label: "estatísticas após recuperação")

    {:ok, recovered} = TaskQueue.claim()
    IO.inspect(recovered, label: "tarefa recuperada")
    :ok = TaskQueue.ack(recovered.id)

    IO.inspect(TaskQueue.stats(), label: "estatísticas finais")

    # Simula reinicialização completa da aplicação: a fila é recarregada do disco.
    Supervisor.stop(_sup)
    {:ok, _} = Supervisor.start_link([{TaskQueue, name: TaskQueue, path: path}], strategy: :one_for_one)
    IO.inspect(TaskQueue.stats(), label: "estatísticas após reinicialização completa")

    File.rm(path)
    :ok
  end

  defp receive_notifications do
    receive do
      {:task_queue, :enqueued, task} ->
        IO.inspect(task, label: "notificação recebida")
        receive_notifications()
    after
      0 -> :ok
    end
  end
end

TaskQueue.Demo.run()
