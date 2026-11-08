# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de votação online, atendendo somente aos seguintes requisitos:
#
# 1. Garantir a consistência dos dados durante operações concorrentes.
# 2. Fazer uso de macros.
# 3. Permitir que diferentes entidades sejam processadas independentemente.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - Supervisor
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# Sistema de votação online com Elixir (biblioteca padrão).
# Execute com: elixir voting.exs

# ---------------------------------------------------------------------------
# Macros: centralizam o padrão de comandos e validações
# ---------------------------------------------------------------------------
defmodule Voting.Macros do
  @moduledoc false

  # Gera uma cláusula privada `handle_command/2` para um comando.
  # O comando é uma lista `[nome | args]`, por exemplo `[:vote, voter, option]`.
  # O corpo deve retornar `{resposta, novo_estado}`.
  defmacro defcommand(name, args, do: body) do
    quote do
      defp handle_command([unquote(name) | unquote(args)], var!(state)) do
        unquote(body)
      end
    end
  end

  # Converte uma condição em `:ok` ou `{:error, motivo}`, para uso em `with`.
  defmacro ensure(condition, reason) do
    quote do
      if unquote(condition), do: :ok, else: {:error, unquote(reason)}
    end
  end
end

# ---------------------------------------------------------------------------
# Entidade independente: cada enquete é um processo GenServer próprio.
# Operações sobre a mesma enquete são serializadas pelo processo (consistência),
# enquanto enquetes diferentes são processadas em paralelo (independência).
# ---------------------------------------------------------------------------
defmodule Voting.Poll do
  @moduledoc false
  use GenServer, restart: :transient

  import Voting.Macros

  # -- API pública -----------------------------------------------------------

  def start_link(opts) do
    id = Keyword.fetch!(opts, :id)
    GenServer.start_link(__MODULE__, opts, name: via(id))
  end

  def via(id), do: {:via, Registry, {Voting.Registry, id}}

  def vote(id, voter, option), do: call(id, [:vote, voter, option])
  def close(id), do: call(id, [:close])
  def tally(id), do: call(id, [:tally])

  defp call(id, cmd) do
    GenServer.call(via(id), {:command, cmd})
  catch
    # Processo inexistente: a enquete não foi criada ou já foi encerrada
    :exit, _ -> {:error, :poll_not_found}
  end

  # -- Callbacks -------------------------------------------------------------

  @impl true
  def init(opts) do
    options = Keyword.fetch!(opts, :options)

    state = %{
      id: Keyword.fetch!(opts, :id),
      title: Keyword.get(opts, :title, ""),
      status: :open,
      # Contagem inicial zerada para cada opção
      tally: Map.new(options, &{&1, 0}),
      # Conjunto de eleitores que já votaram (garante um voto por eleitor)
      voters: MapSet.new()
    }

    {:ok, state}
  end

  @impl true
  def handle_call({:command, cmd}, _from, state) do
    {reply, new_state} = handle_command(cmd, state)
    {:reply, reply, new_state}
  end

  # -- Comandos (geradas pela macro defcommand) ------------------------------

  defcommand :vote, [voter, option] do
    with :ok <- ensure(state.status == :open, :poll_closed),
         :ok <- ensure(Map.has_key?(state.tally, option), :invalid_option),
         :ok <- ensure(not MapSet.member?(state.voters, voter), :already_voted) do
      new_state = %{
        state
        | voters: MapSet.put(state.voters, voter),
          tally: Map.update!(state.tally, option, &(&1 + 1))
      }

      {:ok, new_state}
    else
      {:error, reason} -> {{:error, reason}, state}
    end
  end

  defcommand :close, [] do
    with :ok <- ensure(state.status == :open, :already_closed) do
      {:ok, %{state | status: :closed}}
    else
      {:error, reason} -> {{:error, reason}, state}
    end
  end

  defcommand :tally, [] do
    {{:ok, state.tally}, state}
  end

  # Comando desconhecido
  defp handle_command(_cmd, state), do: {{:error, :unknown_command}, state}
end

# ---------------------------------------------------------------------------
# Árvore de supervisão: Registry + DynamicSupervisor de enquetes.
# ---------------------------------------------------------------------------
defmodule Voting.RootSupervisor do
  @moduledoc false
  use Supervisor

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    children = [
      # Registro único de enquetes por id
      {Registry, keys: :unique, name: Voting.Registry},
      # Supervisor dinâmico: cria/remove processos de enquete sob demanda
      {DynamicSupervisor, strategy: :one_for_one, name: Voting.PollSupervisor}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end

# ---------------------------------------------------------------------------
# Fachada pública do sistema
# ---------------------------------------------------------------------------
defmodule Voting do
  @moduledoc false

  @doc "Cria uma nova enquete como processo supervisionado."
  def create_poll(id, title, options) do
    spec = {Voting.Poll, [id: id, title: title, options: options]}
    DynamicSupervisor.start_child(Voting.PollSupervisor, spec)
  end
end

# ---------------------------------------------------------------------------
# Demonstração: concorrência, consistência e independência entre enquetes
# ---------------------------------------------------------------------------
{:ok, _sup} = Voting.RootSupervisor.start_link()

{:ok, _} = Voting.create_poll("linguagem", "Melhor linguagem?", ["elixir", "erlang", "go"])
{:ok, _} = Voting.create_poll("cor", "Cor favorita?", ["azul", "verde"])

# 10_000 eleitores tentam votar 3 vezes cada, de forma concorrente.
# Esperado: exatamente 10_000 votos aceitos e 20_000 rejeitados por :already_voted.
results =
  1..10_000
  |> Task.async_stream(
    fn i ->
      voter = "eleitor-#{i}"
      option = Enum.random(["elixir", "erlang", "go"])
      for _ <- 1..3, do: Voting.Poll.vote("linguagem", voter, option)
    end,
    max_concurrency: System.schedulers_online() * 4,
    timeout: :infinity
  )
  |> Enum.flat_map(fn {:ok, replies} -> replies end)
  |> Enum.frequencies()

IO.puts("Resultados das tentativas de voto:")
IO.inspect(results)

{:ok, tally} = Voting.Poll.tally("linguagem")
IO.puts("Apuração final (linguagem):")
IO.inspect(tally)

total = tally |> Map.values() |> Enum.sum()
IO.puts("Consistência: total apurado = #{total}, votos aceitos = #{results[:ok]}")

# Enquete independente: continua funcionando normalmente
{:ok, _} = Voting.Poll.vote("cor", "eleitor-1", "azul") |> then(&{:ok, &1})
IO.inspect(Voting.Poll.tally("cor"), label: "Enquete 'cor' (independente)")

# Encerramento e validações
IO.inspect(Voting.Poll.close("linguagem"), label: "Encerrar enquete")
IO.inspect(Voting.Poll.vote("linguagem", "novo-eleitor", "elixir"), label: "Voto após encerramento")
IO.inspect(Voting.Poll.vote("cor", "novo-eleitor", "roxo"), label: "Opção inválida")
IO.inspect(Voting.Poll.vote("inexistente", "x", "y"), label: "Enquete inexistente")
