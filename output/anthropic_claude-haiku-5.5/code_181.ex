# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de agendamento de compromissos, atendendo somente aos seguintes requisitos:
#
# 1. Impedir que uma mesma operação seja executada mais de uma vez.
# 2. Permitir comunicação entre diferentes processos.
# 3. Validar os dados recebidos antes de realizar operações.
# 4. Permitir limitar a quantidade de operações simultâneas.
# 5. Recuperar processos após falhas inesperadas.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# agenda.exs
# Sistema de agendamento de compromissos em Elixir (apenas biblioteca padrão).
#
# Requisitos atendidos:
#   1. Idempotência: cada operação é identificada por uma chave; repetições
#      retornam o resultado original sem executar a operação de novo.
#   2. Comunicação entre processos: publicação de eventos via Registry (pub/sub)
#      e chamadas síncronas via GenServer.
#   3. Validação: todos os dados são validados antes de qualquer operação.
#   4. Limite de concorrência: semáforo com fila (Agenda.Limiter).
#   5. Recuperação: supervisão com restart automático e estado persistido em DETS.

# ---------------------------------------------------------------------------
# Validação dos dados de entrada
# ---------------------------------------------------------------------------
defmodule Agenda.Validator do
  @moduledoc "Valida chaves de operação e parâmetros de agendamento."

  @doc "Valida a chave de idempotência (string não vazia, até 128 bytes)."
  def validate_key(key) when is_binary(key) do
    if String.trim(key) != "" and byte_size(key) <= 128,
      do: :ok,
      else: {:error, [:invalid_key]}
  end

  def validate_key(_), do: {:error, [:invalid_key]}

  @doc """
  Valida os parâmetros de um agendamento.
  Retorna `{:ok, dados_normalizados}` ou `{:error, lista_de_motivos}`.
  """
  def validate_schedule(params) when is_map(params) do
    title = Map.get(params, :title)
    starts_at = Map.get(params, :starts_at)
    ends_at = Map.get(params, :ends_at)
    participants = Map.get(params, :participants)

    checks = [
      {not valid_text?(title, 200), :invalid_title},
      {not match?(%DateTime{}, starts_at), :invalid_starts_at},
      {not match?(%DateTime{}, ends_at), :invalid_ends_at},
      {not valid_interval?(starts_at, ends_at), :invalid_interval},
      {not valid_participants?(participants), :invalid_participants}
    ]

    case for({bad?, reason} <- checks, bad?, do: reason) do
      [] ->
        {:ok,
         %{
           title: String.trim(title),
           starts_at: starts_at,
           ends_at: ends_at,
           participants: Enum.uniq(participants)
         }}

      errors ->
        {:error, errors}
    end
  end

  def validate_schedule(_), do: {:error, [:invalid_params]}

  defp valid_text?(text, max) do
    is_binary(text) and String.trim(text) != "" and String.length(text) <= max
  end

  defp valid_interval?(starts_at, ends_at) do
    match?(%DateTime{}, starts_at) and match?(%DateTime{}, ends_at) and
      DateTime.compare(ends_at, starts_at) == :gt
  end

  defp valid_participants?(list) do
    is_list(list) and list != [] and Enum.all?(list, &valid_text?(&1, 100))
  end
end

# ---------------------------------------------------------------------------
# Comunicação entre processos (pub/sub sobre Registry)
# ---------------------------------------------------------------------------
defmodule Agenda.Events do
  @moduledoc "Publicação e assinatura de eventos do sistema usando Registry."

  @registry Agenda.Events.Registry
  @topic :all

  @doc "Registra o processo chamador para receber eventos."
  def subscribe, do: Registry.register(@registry, @topic, nil)

  @doc "Envia o evento para todos os processos inscritos."
  def publish(event) do
    Registry.dispatch(@registry, @topic, fn entries ->
      for {pid, _} <- entries, do: send(pid, {:agenda_event, event})
    end)
  end
end

# ---------------------------------------------------------------------------
# Limite de operações simultâneas (semáforo com fila)
# ---------------------------------------------------------------------------
defmodule Agenda.Limiter do
  @moduledoc """
  Limita o número de operações executadas simultaneamente.
  O token de cada permissão é a referência de monitoramento do processo
  solicitante: se ele morrer, a vaga é liberada automaticamente.
  """
  use GenServer

  def start_link(opts) do
    GenServer.start_link(__MODULE__, Keyword.get(opts, :max, 4), name: __MODULE__)
  end

  @doc "Executa `fun` respeitando o limite de concorrência."
  def run(fun, timeout \\ 5_000) when is_function(fun, 0) do
    case acquire(timeout) do
      {:ok, token} ->
        try do
          fun.()
        after
          GenServer.cast(__MODULE__, {:release, token})
        end

      {:error, _} = error ->
        error
    end
  end

  defp acquire(timeout) do
    {:ok, GenServer.call(__MODULE__, :acquire, timeout)}
  catch
    :exit, {:timeout, _} ->
      # Remove o pedido da fila para não deixar vaga presa
      GenServer.cast(__MODULE__, {:cancel, self()})
      {:error, :timeout}
  end

  # --- Callbacks ---

  def init(max) when is_integer(max) and max > 0 do
    {:ok, %{max: max, active: %{}, waiting: :queue.new()}}
  end

  def handle_call(:acquire, {pid, _} = from, state) do
    {:noreply, admit(%{state | waiting: :queue.in({from, pid}, state.waiting)})}
  end

  def handle_cast({:release, token}, state) do
    Process.demonitor(token, [:flush])
    {:noreply, admit(%{state | active: Map.delete(state.active, token)})}
  end

  def handle_cast({:cancel, pid}, state) do
    {mine, others} = Enum.split_with(state.active, fn {_t, p} -> p == pid end)
    Enum.each(mine, fn {t, _} -> Process.demonitor(t, [:flush]) end)
    waiting = :queue.filter(fn {_from, p} -> p != pid end, state.waiting)
    {:noreply, admit(%{state | active: Map.new(others), waiting: waiting})}
  end

  # Processo detentor de vaga morreu: libera a vaga
  def handle_info({:DOWN, token, :process, _pid, _reason}, state) do
    {:noreply, admit(%{state | active: Map.delete(state.active, token)})}
  end

  # Concede vagas enquanto houver capacidade e pedidos na fila
  defp admit(%{max: max, active: active, waiting: waiting} = state) do
    with true <- map_size(active) < max,
         {{:value, {from, pid}}, rest} <- :queue.out(waiting) do
      token = Process.monitor(pid)
      GenServer.reply(from, token)
      admit(%{state | active: Map.put(active, token, pid), waiting: rest})
    else
      _ -> state
    end
  end
end

# ---------------------------------------------------------------------------
# Armazenamento persistente com idempotência e detecção de conflitos
# ---------------------------------------------------------------------------
defmodule Agenda.Store do
  @moduledoc """
  Mantém agendamentos e resultados de operações (por chave de idempotência).
  Todo estado é gravado em DETS antes de responder, permitindo recuperação
  após falhas do processo.
  """
  use GenServer

  @table :agenda_store

  def start_link(opts) do
    GenServer.start_link(__MODULE__, Keyword.fetch!(opts, :path), name: __MODULE__)
  end

  def schedule(key, data), do: GenServer.call(__MODULE__, {:schedule, key, data})

  def list, do: GenServer.call(__MODULE__, :list)

  # --- Callbacks ---

  def init(path) do
    {:ok, table} = :dets.open_file(@table, file: String.to_charlist(path), type: :set)

    # Recarrega o estado persistido (recuperação após falha)
    {appointments, idem} =
      :dets.foldl(
        fn
          {{:appointment, id}, appt}, {a, i} -> {Map.put(a, id, appt), i}
          {{:idem, key}, entry}, {a, i} -> {a, Map.put(i, key, entry)}
        end,
        {%{}, %{}},
        table
      )

    {:ok, %{table: table, appointments: appointments, idem: idem}}
  end

  def handle_call({:schedule, key, data}, _from, state) do
    case Map.fetch(state.idem, key) do
      # Mesma operação com mesmos dados: devolve o resultado original
      {:ok, {^data, result}} ->
        {:reply, result, state}

      # Mesma chave com dados diferentes: rejeita
      {:ok, _other} ->
        {:reply, {:error, :idempotency_key_reused}, state}

      :error ->
        create(key, data, state)
    end
  end

  def handle_call(:list, _from, state) do
    list = Enum.sort_by(Map.values(state.appointments), & &1.starts_at, DateTime)
    {:reply, list, state}
  end

  # --- Regras internas ---

  defp create(key, data, state) do
    case find_conflict(data, state.appointments) do
      nil ->
        appt = Map.put(data, :id, key)
        result = {:ok, appt}

        :ok =
          :dets.insert(@table, [
            {{:appointment, key}, appt},
            {{:idem, key}, {data, result}}
          ])

        :ok = :dets.sync(@table)

        Agenda.Events.publish({:appointment_scheduled, appt})

        new_state = %{
          state
          | appointments: Map.put(state.appointments, key, appt),
            idem: Map.put(state.idem, key, {data, result})
        }

        {:reply, result, new_state}

      conflict ->
        {:reply, {:error, {:conflict, conflict.id}}, state}
    end
  end

  # Conflito: algum participante em comum e intervalos sobrepostos
  defp find_conflict(data, appointments) do
    new_people = MapSet.new(data.participants)

    Enum.find(Map.values(appointments), fn a ->
      (not MapSet.disjoint?(MapSet.new(a.participants), new_people)) and
        DateTime.compare(a.starts_at, data.ends_at) == :lt and
        DateTime.compare(data.starts_at, a.ends_at) == :lt
    end)
  end
end

# ---------------------------------------------------------------------------
# Árvore de supervisão
# ---------------------------------------------------------------------------
defmodule Agenda.Root do
  @moduledoc "Supervisor principal: reinicia processos que falham."
  use Supervisor

  def start_link(opts) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def init(opts) do
    children = [
      {Registry, keys: :duplicate, name: Agenda.Events.Registry},
      {Agenda.Limiter, max: Keyword.get(opts, :max_concurrent, 4)},
      {Agenda.Store, path: Keyword.fetch!(opts, :path)}
    ]

    Supervisor.init(children, strategy: :one_for_one, max_restarts: 10, max_seconds: 10)
  end
end

# ---------------------------------------------------------------------------
# API pública
# ---------------------------------------------------------------------------
defmodule Agenda do
  @moduledoc "API pública do sistema de agendamento."

  alias Agenda.{Validator, Store, Limiter}

  @doc """
  Inicia o sistema.
  Opções: `:path` (arquivo DETS, obrigatório) e `:max_concurrent` (padrão 4).
  """
  def start(opts), do: Agenda.Root.start_link(opts)

  @doc """
  Agenda um compromisso de forma idempotente.
  `key` identifica a operação; repetições com a mesma chave e os mesmos dados
  retornam o resultado original.
  """
  def schedule(key, params) do
    with :ok <- Validator.validate_key(key),
         {:ok, data} <- Validator.validate_schedule(params) do
      Limiter.run(fn -> Store.schedule(key, data) end)
    end
  end

  @doc "Lista os compromissos ordenados pelo início."
  def list, do: Store.list()
end

# ---------------------------------------------------------------------------
# Demonstração
# ---------------------------------------------------------------------------
defmodule Agenda.Demo do
  @moduledoc "Demonstra todos os requisitos do sistema."
  alias Agenda.{Events, Store}

  def run do
    path = Path.join(System.tmp_dir!(), "agenda_demo.dets")
    File.rm(path)

    {:ok, _sup} = Agenda.start(path: path, max_concurrent: 2)
    Events.subscribe()

    base = %{
      title: "Reunião",
      starts_at: ~U[2030-01-10 10:00:00Z],
      ends_at: ~U[2030-01-10 11:00:00Z],
      participants: ["ana", "bruno"]
    }

    # 1 e 2: idempotência
    IO.inspect(Agenda.schedule("op-1", base), label: "1. agendamento")
    IO.inspect(Agenda.schedule("op-1", base), label: "2. repetição da mesma operação")

    # 3: chave reutilizada com dados diferentes
    IO.inspect(Agenda.schedule("op-1", %{base | title: "Outra"}),
      label: "3. chave reutilizada"
    )

    # 4: conflito de horário
    IO.inspect(Agenda.schedule("op-2", %{base | participants: ["ana"]}),
      label: "4. conflito de horário"
    )

    # 5: validação
    IO.inspect(Agenda.schedule("op-3", %{base | title: "  ", participants: []}),
      label: "5. dados inválidos"
    )

    # 6: concorrência com limite de 2 operações simultâneas
    1..6
    |> Enum.map(fn i ->
      params = %{
        base
        | starts_at: DateTime.add(base.starts_at, i, :day),
          ends_at: DateTime.add(base.ends_at, i, :day),
          participants: ["user#{i}"]
      }

      Task.async(fn -> Agenda.schedule("par-#{i}", params) end)
    end)
    |> Enum.map(&Task.await(&1, 10_000))
    |> IO.inspect(label: "6. concorrência (limite 2)")

    # 7: comunicação entre processos
    IO.inspect(length(flush_events()), label: "7. eventos recebidos")

    # 8: recuperação após falha
    Process.whereis(Store) |> Process.exit(:kill)
    Process.sleep(100)
    IO.inspect(Enum.map(Agenda.list(), & &1.id), label: "8. após falha do Store")
  end

  defp flush_events(acc \\ []) do
    receive do
      {:agenda_event, event} -> flush_events([event | acc])
    after
      0 -> Enum.reverse(acc)
    end
  end
end

# Executa a demonstração ao rodar o arquivo com `elixir agenda.exs`
Agenda.Demo.run()
