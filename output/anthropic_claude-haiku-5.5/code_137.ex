# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de votação online, atendendo somente aos seguintes requisitos:
#
# 1. Implementar expiração automática de registros.
# 2. Impedir atualizações parcialmente concluídas.
# 3. Recuperar processos após falhas inesperadas.
# 4. Permitir cadastrar, consultar, atualizar e remover registros.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule VotingSystem do
  @moduledoc """
  Sistema de votação online.

  Requisitos atendidos:

  1. Expiração automática: cada enquete possui TTL. Registros expirados são
     ignorados em qualquer consulta e removidos por varredura periódica.
  2. Atualizações não parciais: cada operação de escrita é uma transação
     serializada (validação, cálculo e persistência). Se qualquer etapa falhar,
     nada é alterado, nem em memória nem em disco.
  3. Recuperação de falhas: o Store roda sob supervisão com reinício automático.
     O estado é persistido em disco com escrita atômica (arquivo temporário +
     rename), então um reinício recarrega sempre um snapshot consistente.
  4. CRUD: criar, consultar, atualizar e remover enquetes, além de registrar votos.

  Uso:

      {:ok, _} = VotingSystem.start_link(path: "votes.snapshot")
      {:ok, poll} = VotingSystem.create_poll(%{title: "Linguagem", options: ["Elixir", "Erlang"], ttl_ms: 60_000})
      {:ok, _} = VotingSystem.vote(poll.id, "Elixir", "user-1")
      {:ok, _} = VotingSystem.update_poll(poll.id, %{title: "Nova pergunta"})
      {:ok, _} = VotingSystem.get_poll(poll.id)
      {:ok, _} = VotingSystem.delete_poll(poll.id)
  """

  # Inicia a árvore de supervisão (Store + reinício automático).
  defdelegate start_link(opts \\ []), to: VotingSystem.Root

  defdelegate create_poll(attrs), to: VotingSystem.Store
  defdelegate get_poll(id), to: VotingSystem.Store
  defdelegate update_poll(id, changes), to: VotingSystem.Store
  defdelegate delete_poll(id), to: VotingSystem.Store
  defdelegate vote(id, option, voter), to: VotingSystem.Store
end

defmodule VotingSystem.Root do
  @moduledoc false
  # Supervisor responsável por reiniciar o Store após falhas inesperadas.
  use Supervisor

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    children = [{VotingSystem.Store, opts}]

    # one_for_one: somente o processo que falhou é reiniciado.
    # max_restarts/max_seconds: limite contra loops de reinício.
    Supervisor.init(children, strategy: :one_for_one, max_restarts: 10, max_seconds: 5)
  end
end

defmodule VotingSystem.Store do
  @moduledoc false
  # Processo dono do estado. Todas as escritas passam por aqui, de forma serializada.

  use GenServer

  @sweep_interval_ms 1_000
  @default_ttl_ms 86_400_000
  @default_path "voting_system.snapshot"
  @max_attempts 5
  @retry_delay_ms 100

  # ===================== API pública =====================

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc "Cria uma enquete. Atributos: :title, :options (lista de strings), :ttl_ms (opcional)."
  def create_poll(attrs) do
    transaction(fn data, now ->
      with {:ok, title, options, ttl} <- validate_new(attrs) do
        id = data.next_id

        poll = %{
          id: id,
          title: title,
          options: Map.new(options, &{&1, 0}),
          voters: MapSet.new(),
          expires_at: now + ttl
        }

        {:ok, view(poll), %{data | next_id: id + 1, polls: Map.put(data.polls, id, poll)}}
      end
    end)
  end

  @doc "Consulta uma enquete ativa (não expirada)."
  def get_poll(id) do
    call({:get, id})
  end

  @doc """
  Atualiza campos de uma enquete de forma atômica (tudo ou nada).
  Campos aceitos: :title, :options (apenas sem votos), :ttl_ms (renova a expiração).
  """
  def update_poll(id, changes) when is_map(changes) do
    transaction(fn data, now ->
      with {:ok, poll} <- fetch_live(data, id, now),
           {:ok, updated} <- apply_changes(poll, changes, now) do
        {:ok, view(updated), put_poll(data, updated)}
      end
    end)
  end

  @doc "Remove uma enquete."
  def delete_poll(id) do
    transaction(fn data, now ->
      with {:ok, _poll} <- fetch_live(data, id, now) do
        {:ok, :deleted, %{data | polls: Map.delete(data.polls, id)}}
      end
    end)
  end

  @doc "Registra um voto. Um mesmo eleitor não pode votar duas vezes na mesma enquete."
  def vote(id, option, voter) do
    transaction(fn data, now ->
      with {:ok, poll} <- fetch_live(data, id, now),
           :ok <- ensure_option(poll, option),
           :ok <- ensure_not_voted(poll, voter) do
        updated = %{
          poll
          | options: Map.update!(poll.options, option, &(&1 + 1)),
            voters: MapSet.put(poll.voters, voter)
        }

        {:ok, view(updated), put_poll(data, updated)}
      end
    end)
  end

  # ===================== Transações e chamadas =====================

  # A função recebe o estado atual e o instante atual e retorna:
  #   {:ok, resposta, novo_estado} | {:error, motivo}
  # O novo estado só é adotado se a persistência em disco for bem-sucedida.
  defp transaction(fun), do: call({:transaction, fun})

  # Se o processo ainda estiver sendo reiniciado pelo supervisor, aguarda e tenta
  # novamente. Só repete quando a mensagem não chegou a ser processada (:noproc).
  defp call(msg, attempts \\ @max_attempts) do
    GenServer.call(__MODULE__, msg)
  catch
    :exit, {:noproc, _} when attempts > 1 ->
      Process.sleep(@retry_delay_ms)
      call(msg, attempts - 1)

    :exit, _reason ->
      {:error, :unavailable}
  end

  # ===================== Callbacks do GenServer =====================

  @impl true
  def init(opts) do
    path = Keyword.get(opts, :path, @default_path)
    data = load(path)
    schedule_sweep()
    {:ok, %{path: path, data: data}}
  end

  @impl true
  def handle_call({:get, id}, _from, state) do
    now = now_ms()

    reply =
      with {:ok, poll} <- fetch_live(state.data, id, now) do
        {:ok, view(poll)}
      end

    {:reply, reply, state}
  end

  def handle_call({:transaction, fun}, _from, state) do
    now = now_ms()

    case run(fun, state.data, now) do
      {:ok, reply, new_data} ->
        case persist(state.path, new_data) do
          :ok ->
            {:reply, {:ok, reply}, %{state | data: new_data}}

          {:error, reason} ->
            # Falha de disco: o estado anterior é mantido intacto.
            {:reply, {:error, {:persistence_failed, reason}}, state}
        end

      {:error, _reason} = error ->
        {:reply, error, state}
    end
  end

  @impl true
  def handle_info(:sweep, state) do
    state = expire(state, now_ms())
    schedule_sweep()
    {:noreply, state}
  end

  # ===================== Expiração =====================

  defp schedule_sweep do
    Process.send_after(self(), :sweep, @sweep_interval_ms)
  end

  defp expire(state, now) do
    {live, expired} =
      Enum.split_with(state.data.polls, fn {_id, poll} -> poll.expires_at > now end)

    if expired == [] do
      state
    else
      new_data = %{state.data | polls: Map.new(live)}

      case persist(state.path, new_data) do
        :ok -> %{state | data: new_data}
        # Se a persistência falhar, tenta novamente na próxima varredura.
        {:error, _reason} -> state
      end
    end
  end

  defp fetch_live(data, id, now) do
    case Map.fetch(data.polls, id) do
      {:ok, %{expires_at: expires_at} = poll} when expires_at > now -> {:ok, poll}
      _ -> {:error, :not_found}
    end
  end

  # ===================== Execução segura de transações =====================

  defp run(fun, data, now) do
    case fun.(data, now) do
      {:ok, _reply, _new_data} = ok -> ok
      {:error, _reason} = error -> error
      other -> {:error, {:invalid_transaction_result, other}}
    end
  rescue
    e -> {:error, {:exception, Exception.message(e)}}
  end

  # ===================== Atualização em múltiplos campos =====================

  # Aplica todas as mudanças sobre uma cópia pura da enquete.
  # Se qualquer campo for inválido, o resultado é erro e nada é aplicado.
  defp apply_changes(poll, changes, now) do
    Enum.reduce_while(changes, {:ok, poll}, fn {key, value}, {:ok, acc} ->
      case apply_change(acc, key, value, now) do
        {:ok, next} -> {:cont, {:ok, next}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp apply_change(poll, :title, title, _now) do
    with {:ok, title} <- validate_title(title) do
      {:ok, %{poll | title: title}}
    end
  end

  defp apply_change(poll, :options, options, _now) do
    if MapSet.size(poll.voters) > 0 do
      {:error, :poll_has_votes}
    else
      with {:ok, options} <- validate_options(options) do
        {:ok, %{poll | options: Map.new(options, &{&1, 0})}}
      end
    end
  end

  defp apply_change(poll, :ttl_ms, ttl, now) do
    with {:ok, ttl} <- validate_ttl(ttl) do
      {:ok, %{poll | expires_at: now + ttl}}
    end
  end

  defp apply_change(_poll, key, _value, _now), do: {:error, {:unknown_field, key}}

  # ===================== Validações =====================

  defp validate_new(attrs) when is_map(attrs) do
    with {:ok, title} <- validate_title(Map.get(attrs, :title)),
         {:ok, options} <- validate_options(Map.get(attrs, :options)),
         {:ok, ttl} <- validate_ttl(Map.get(attrs, :ttl_ms, @default_ttl_ms)) do
      {:ok, title, options, ttl}
    end
  end

  defp validate_new(_attrs), do: {:error, :invalid_attributes}

  defp validate_title(title) when is_binary(title) and title != "", do: {:ok, title}
  defp validate_title(_), do: {:error, {:invalid_field, :title}}

  defp validate_options(options) when is_list(options) do
    cond do
      length(options) < 2 ->
        {:error, :too_few_options}

      not Enum.all?(options, &(is_binary(&1) and &1 != "")) ->
        {:error, {:invalid_field, :options}}

      length(Enum.uniq(options)) != length(options) ->
        {:error, :duplicated_options}

      true ->
        {:ok, options}
    end
  end

  defp validate_options(_), do: {:error, {:invalid_field, :options}}

  defp validate_ttl(ttl) when is_integer(ttl) and ttl > 0, do: {:ok, ttl}
  defp validate_ttl(_), do: {:error, {:invalid_field, :ttl_ms}}

  defp ensure_option(poll, option) do
    if Map.has_key?(poll.options, option), do: :ok, else: {:error, :invalid_option}
  end

  defp ensure_not_voted(poll, voter) do
    if MapSet.member?(poll.voters, voter), do: {:error, :already_voted}, else: :ok
  end

  # ===================== Persistência atômica =====================

  # Escreve em arquivo temporário, força gravação em disco e só então
  # substitui o arquivo principal via rename (operação atômica no SO).
  # Uma falha no meio da escrita deixa o snapshot anterior intacto.
  defp persist(path, data) do
    tmp = path <> ".tmp"
    bin = :erlang.term_to_binary(data)

    case write_tmp(tmp, bin) do
      :ok ->
        :file.rename(tmp, path)

      {:error, _reason} = error ->
        _ = File.rm(tmp)
        error
    end
  end

  defp write_tmp(tmp, bin) do
    case :file.open(tmp, [:write, :binary, :raw]) do
      {:ok, io} ->
        result =
          with :ok <- :file.write(io, bin) do
            :file.sync(io)
          end

        _ = :file.close(io)
        result

      {:error, _reason} = error ->
        error
    end
  end

  defp load(path) do
    # Um .tmp residual indica escrita interrompida; o snapshot principal continua válido.
    _ = File.rm(path <> ".tmp")

    case File.read(path) do
      {:ok, bin} -> decode(path, bin)
      {:error, :enoent} -> empty_data()
      {:error, reason} -> raise "não foi possível ler #{path}: #{inspect(reason)}"
    end
  end

  defp decode(path, bin) do
    :erlang.binary_to_term(bin, [:safe])
  rescue
    ArgumentError ->
      # Snapshot corrompido: preserva o arquivo para análise e inicia limpo.
      _ = File.rename(path, path <> ".corrupt")
      empty_data()
  end

  defp empty_data, do: %{next_id: 1, polls: %{}}

  # ===================== Utilitários =====================

  defp put_poll(data, poll), do: %{data | polls: Map.put(data.polls, poll.id, poll)}

  defp view(poll) do
    %{
      id: poll.id,
      title: poll.title,
      options: poll.options,
      total_voters: MapSet.size(poll.voters),
      expires_at: poll.expires_at
    }
  end

  defp now_ms, do: System.system_time(:millisecond)
end
