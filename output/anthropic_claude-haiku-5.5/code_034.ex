# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de troca de mensagens entre usuários, atendendo somente aos seguintes requisitos:
#
# 1. Fazer uso de macros.
# 2. Permitir comunicação entre diferentes processos.
# 3. Garantir a consistência dos dados durante operações concorrentes.
# 4. Persistir os dados para recuperação após reinicializações.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - Task
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# Sistema de troca de mensagens entre usuários em Elixir.
# Requisitos atendidos:
#   1. Macros: `deftransition` (DSL que gera comandos e transições de estado).
#   2. Comunicação entre processos: chamadas ao GenServer e notificações enviadas
#      a processos assinantes (`send/2`), com monitoramento de assinantes.
#   3. Consistência: todas as mutações são serializadas pelo GenServer; cada
#      transição é validada sobre o estado atual antes de ser persistida e aplicada.
#   4. Persistência: log de comandos (event sourcing) em arquivo binário com
#      escrita sincronizada (`:file.datasync/1`); o estado é reconstruído por replay.
# Uso: `elixir arquivo.exs`

defmodule Messaging.Macros do
  @moduledoc """
  DSL para declarar transições de estado do sistema.

  `deftransition nome(args) do ... end` gera:
    * `apply_nome/N` (função pura): recebe o estado e os argumentos, valida
      e retorna `{:ok, novo_estado, resposta}` ou `{:error, motivo}`;
    * `nome/N` (cliente): envia o comando ao GenServer, que o executa de forma
      serializada, garantindo consistência em operações concorrentes.
  """

  defmacro deftransition({name, _meta, args}, do: body) do
    fun = :"apply_#{name}"
    args = args || []

    quote do
      # Transição pura: `state` é variável visível no corpo do bloco.
      @doc false
      def unquote(fun)(var!(state), unquote_splicing(args)) do
        unquote(body)
      end

      # Função cliente: empacota o comando e o envia ao processo serializador.
      def unquote(name)(unquote_splicing(args)) do
        GenServer.call(__MODULE__, {:command, unquote(name), [unquote_splicing(args)]})
      end
    end
  end
end

defmodule Messaging.Store do
  @moduledoc """
  Processo único responsável pelo estado do sistema. Todas as mutações chegam
  por mensagens e são processadas uma de cada vez, o que garante consistência.
  """
  use GenServer
  import Messaging.Macros

  @default_path "messaging.log"

  # ---------------------------------------------------------------------------
  # API pública
  # ---------------------------------------------------------------------------

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  # Assina notificações de novas mensagens para `user`. O `pid` recebe
  # `{:messaging, :new_message, msg}` sempre que uma mensagem chega ao usuário.
  def subscribe(user, pid \\ self()) do
    GenServer.call(__MODULE__, {:subscribe, user, pid})
  end

  # Leitura: retorna as mensagens do usuário em ordem cronológica.
  def inbox(user) do
    GenServer.call(__MODULE__, {:inbox, user})
  end

  # Transições (comandos) declaradas com a macro.

  deftransition register_user(user) do
    cond do
      not is_binary(user) ->
        {:error, :invalid_user}

      MapSet.member?(state.users, user) ->
        {:error, :already_registered}

      true ->
        new_state = %{
          state
          | users: MapSet.put(state.users, user),
            inboxes: Map.put(state.inboxes, user, [])
        }

        {:ok, new_state, :registered}
    end
  end

  deftransition send_message(from, to, text) do
    cond do
      not (MapSet.member?(state.users, from) and MapSet.member?(state.users, to)) ->
        {:error, :unknown_user}

      not is_binary(text) ->
        {:error, :invalid_text}

      true ->
        # O id é atribuído dentro da transição, logo é único e sequencial.
        id = state.next_id
        msg = %{id: id, from: from, to: to, text: text}
        inboxes = Map.update!(state.inboxes, to, &[msg | &1])
        {:ok, %{state | next_id: id + 1, inboxes: inboxes}, msg}
    end
  end

  deftransition ack(user, id) do
    case Map.fetch(state.inboxes, user) do
      {:ok, inbox} ->
        if Enum.any?(inbox, &(&1.id == id)) do
          remaining = Enum.reject(inbox, &(&1.id == id))
          {:ok, %{state | inboxes: Map.put(state.inboxes, user, remaining)}, :acked}
        else
          {:error, :not_found}
        end

      :error ->
        {:error, :unknown_user}
    end
  end

  # ---------------------------------------------------------------------------
  # Callbacks do GenServer
  # ---------------------------------------------------------------------------

  @impl true
  def init(opts) do
    path = Keyword.get(opts, :path, @default_path)
    entries = load(path)

    # Recuperação: reaplica os comandos logados sobre o estado inicial.
    data =
      Enum.reduce(entries, initial_data(), fn {name, args}, acc ->
        {:ok, new, _reply} = apply_transition(name, acc, args)
        new
      end)

    # Compacta o log (descarta eventual registro parcial no final do arquivo).
    :ok = rewrite(path, entries)
    {:ok, io} = File.open(path, [:append, :binary])

    {:ok, %{path: path, log: io, data: data, subs: %{}}}
  end

  @impl true
  def handle_call({:command, name, args}, _from, st) do
    case apply_transition(name, st.data, args) do
      {:ok, new_data, reply} ->
        # Persiste antes de confirmar: só o estado persistido é aplicado.
        case append(st.log, {name, args}) do
          :ok ->
            notify(st.subs, name, reply)
            {:reply, {:ok, reply}, %{st | data: new_data}}

          {:error, reason} ->
            {:reply, {:error, {:persistence_failed, reason}}, st}
        end

      {:error, _reason} = error ->
        {:reply, error, st}
    end
  end

  def handle_call({:inbox, user}, _from, st) do
    case Map.fetch(st.data.inboxes, user) do
      {:ok, inbox} -> {:reply, {:ok, Enum.reverse(inbox)}, st}
      :error -> {:reply, {:error, :unknown_user}, st}
    end
  end

  def handle_call({:subscribe, user, pid}, _from, st) do
    if MapSet.member?(st.data.users, user) do
      # Monitor permite remover o assinante automaticamente quando o processo morre.
      Process.monitor(pid)

      subs =
        Map.update(st.subs, user, [pid], fn pids ->
          if pid in pids, do: pids, else: [pid | pids]
        end)

      {:reply, :ok, %{st | subs: subs}}
    else
      {:reply, {:error, :unknown_user}, st}
    end
  end

  @impl true
  def handle_info({:DOWN, _ref, :process, pid, _reason}, st) do
    subs = Map.new(st.subs, fn {user, pids} -> {user, List.delete(pids, pid)} end)
    {:noreply, %{st | subs: subs}}
  end

  def handle_info(_msg, st), do: {:noreply, st}

  # ---------------------------------------------------------------------------
  # Funções privadas
  # ---------------------------------------------------------------------------

  defp initial_data, do: %{users: MapSet.new(), inboxes: %{}, next_id: 1}

  defp apply_transition(name, data, args) do
    apply(__MODULE__, :"apply_#{name}", [data | args])
  end

  defp notify(subs, :send_message, msg) do
    for pid <- Map.get(subs, msg.to, []) do
      send(pid, {:messaging, :new_message, msg})
    end

    :ok
  end

  defp notify(_subs, _name, _reply), do: :ok

  # Formato do log: <<tamanho::32, termo_binario::binary>>, repetido.
  defp append(io, term) do
    bin = :erlang.term_to_binary(term)

    with :ok <- :file.write(io, <<byte_size(bin)::32, bin::binary>>) do
      :file.datasync(io)
    end
  end

  defp load(path) do
    case File.read(path) do
      {:ok, bin} -> decode(bin, [])
      {:error, :enoent} -> []
      {:error, reason} -> raise "falha ao ler log #{path}: #{inspect(reason)}"
    end
  end

  defp decode(<<size::32, bin::binary-size(size), rest::binary>>, acc) do
    decode(rest, [:erlang.binary_to_term(bin, [:safe]) | acc])
  end

  defp decode(_trailing, acc), do: Enum.reverse(acc)

  # Reescrita atômica: grava em arquivo temporário e renomeia.
  defp rewrite(path, entries) do
    tmp = path <> ".tmp"

    with {:ok, io} <- File.open(tmp, [:write, :binary]),
         :ok <- write_all(io, entries),
         :ok <- File.close(io),
         :ok <- File.rename(tmp, path) do
      :ok
    end
  end

  defp write_all(io, entries) do
    Enum.reduce_while(entries, :ok, fn entry, :ok ->
      case append(io, entry) do
        :ok -> {:cont, :ok}
        error -> {:halt, error}
      end
    end)
  end
end

defmodule Messaging.Demo do
  @moduledoc false
  alias Messaging.Store

  def run(path \\ "messaging_demo.log") do
    _ = File.rm(path)
    {:ok, _} = Store.start_link(path: path)

    users = for i <- 1..5, do: "user#{i}"
    Enum.each(users, fn u -> {:ok, :registered} = Store.register_user(u) end)
    {:error, :already_registered} = Store.register_user("user1")

    # Task receptora: recebe notificações de user1 enviadas pelo Store.
    receiver =
      Task.async(fn ->
        for _ <- 1..40 do
          receive do
            {:messaging, :new_message, msg} -> msg
          after
            5_000 -> :timeout
          end
        end
      end)

    :ok = Store.subscribe("user1", receiver.pid)

    # Tasks remetentes executando em paralelo.
    pairs = for a <- users, b <- users, a != b, do: {a, b}

    tasks =
      for {from, to} <- pairs do
        Task.async(fn ->
          for k <- 1..10, do: Store.send_message(from, to, "mensagem #{k}")
        end)
      end

    results = tasks |> Task.await_many(10_000) |> List.flatten()
    msgs = for {:ok, m} <- results, do: m

    check!(length(results) == 200 and length(msgs) == 200, "todos os envios concluídos")

    check!(
      msgs |> Enum.map(& &1.id) |> Enum.uniq() |> length() == 200,
      "ids únicos sob concorrência"
    )

    notifications = Task.await(receiver, 10_000)

    check!(
      length(notifications) == 40 and Enum.all?(notifications, &(&1.to == "user1")),
      "notificações entregues ao assinante"
    )

    {:ok, inbox1} = Store.inbox("user1")
    check!(length(inbox1) == 40, "caixa de user1 com 40 mensagens")
    check!(total_messages(users) == 200, "total de 200 mensagens")

    [first | _] = inbox1
    {:ok, :acked} = Store.ack("user1", first.id)
    {:error, :not_found} = Store.ack("user1", first.id)

    # Simula reinicialização: encerra o processo e recria a partir do log.
    GenServer.stop(Store)
    {:ok, _} = Store.start_link(path: path)

    {:ok, inbox1_after} = Store.inbox("user1")
    check!(length(inbox1_after) == 39, "estado recuperado após reinicialização")
    check!(total_messages(users) == 199, "total recuperado após reinicialização")

    {:ok, new_msg} = Store.send_message("user2", "user1", "após restart")
    check!(new_msg.id > 200, "sequência de ids mantida após recuperação")

    IO.puts("Demo concluída com sucesso.")
  end

  defp total_messages(users) do
    Enum.reduce(users, 0, fn u, acc ->
      {:ok, box} = Store.inbox(u)
      acc + length(box)
    end)
  end

  defp check!(true, _label), do: :ok
  defp check!(false, label), do: raise("falha: #{label}")
end

Messaging.Demo.run()
