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

# sistema_votacao.exs
# Execução: elixir sistema_votacao.exs
#
# Arquitetura:
#   - Voting.Root (Supervisor :rest_for_one) supervisiona o Registry e o DynamicSupervisor.
#   - Cada votação (entidade) é um processo GenServer independente, registrado no Registry.
#   - Toda mutação de uma votação passa pela mailbox do seu processo, o que serializa as
#     operações e garante consistência (voto único por eleitor, contagem exata, status atômico),
#     sem locks globais.
#   - Votações diferentes não compartilham estado, então são processadas em paralelo.
#   - Macros (defrule e defoperation) eliminam boilerplate: regras de validação e operações
#     expostas via API e handle_call são geradas a partir de uma única definição.

defmodule Voting.Macros do
  @moduledoc false

  # Define uma regra de validação pura. A regra deve retornar :ok ou {:error, motivo}.
  defmacro defrule({name, _meta, args}, do: body) do
    args = args || []

    quote do
      def unquote(name)(unquote_splicing(args)) do
        unquote(body)
      end
    end
  end

  # Define uma operação sobre a entidade. Gera:
  #   - uma função pública `nome(id, args...)` que envia a mensagem ao processo da entidade;
  #   - uma função `__op_nome__(args..., state)` que executa o corpo dentro do processo.
  # O corpo deve retornar {resposta, novo_estado}.
  defmacro defoperation({name, _meta, args}, do: body) do
    args = args || []
    handler = :"__op_#{name}__"

    quote do
      def unquote(name)(id, unquote_splicing(args)) do
        call(id, {unquote(handler), [unquote_splicing(args)]})
      end

      @doc false
      def unquote(handler)(unquote_splicing(args), var!(state)) do
        unquote(body)
      end
    end
  end
end

defmodule Voting.Poll do
  @moduledoc """
  Processo que representa uma votação. Cada votação é uma entidade isolada:
  falhas ou carga em uma votação não afetam as demais.
  """

  # :transient -> reinicia apenas em caso de falha, não após encerramento normal.
  use GenServer, restart: :transient
  import Voting.Macros

  def start_link(opts) do
    id = Keyword.fetch!(opts, :id)
    GenServer.start_link(__MODULE__, opts, name: via(id))
  end

  # Nome de registro baseado no Registry da biblioteca padrão.
  def via(id), do: {:via, Registry, {Voting.Registry, id}}

  @impl true
  def init(opts) do
    state = %{
      id: Keyword.fetch!(opts, :id),
      question: Keyword.fetch!(opts, :question),
      options: opts |> Keyword.fetch!(:options) |> Map.new(&{&1, 0}),
      voters: MapSet.new(),
      status: :open
    }

    {:ok, state}
  end

  # Chamada síncrona à entidade. Se ela não existir, retorna erro em vez de propagar exit.
  defp call(id, msg) do
    GenServer.call(via(id), msg)
  catch
    :exit, _ -> {:error, :not_found}
  end

  # ---------------------------------------------------------------------------
  # Regras de validação (puras, executadas dentro do processo da votação)
  # ---------------------------------------------------------------------------

  defrule ensure_open(state) do
    case state.status do
      :open -> :ok
      :closed -> {:error, :poll_closed}
    end
  end

  defrule ensure_option(state, option) do
    if Map.has_key?(state.options, option), do: :ok, else: {:error, :invalid_option}
  end

  defrule ensure_new_voter(state, voter) do
    if MapSet.member?(state.voters, voter), do: {:error, :already_voted}, else: :ok
  end

  # ---------------------------------------------------------------------------
  # Operações
  # ---------------------------------------------------------------------------

  # Registro de voto: verificação e atualização ocorrem na mesma mensagem,
  # logo não há janela de corrida entre checar e gravar.
  defoperation vote(voter, option) do
    with :ok <- ensure_open(state),
         :ok <- ensure_option(state, option),
         :ok <- ensure_new_voter(state, voter) do
      new_state = %{
        state
        | voters: MapSet.put(state.voters, voter),
          options: Map.update!(state.options, option, &(&1 + 1))
      }

      {:ok, new_state}
    else
      {:error, reason} -> {{:error, reason}, state}
    end
  end

  defoperation close() do
    case state.status do
      :open -> {:ok, %{state | status: :closed}}
      :closed -> {{:error, :already_closed}, state}
    end
  end

  defoperation result() do
    snapshot = %{
      question: state.question,
      status: state.status,
      tally: state.options,
      total_votes: MapSet.size(state.voters)
    }

    {{:ok, snapshot}, state}
  end

  # Handler genérico: despacha para o __op_*__ correspondente.
  # Somente as funções geradas pelas macros enviam mensagens a este processo.
  @impl true
  def handle_call({op, args}, _from, state) do
    {reply, new_state} = apply(__MODULE__, op, args ++ [state])
    {:reply, reply, new_state}
  end
end

defmodule Voting.Root do
  @moduledoc """
  Supervisor raiz. Usa :rest_for_one: se o Registry cair, o DynamicSupervisor
  (e portanto todas as votações) é reiniciado, mantendo o registro consistente.
  """
  use Supervisor

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    children = [
      {Registry, keys: :unique, name: Voting.Registry},
      {DynamicSupervisor, strategy: :one_for_one, name: Voting.PollSupervisor}
    ]

    Supervisor.init(children, strategy: :rest_for_one)
  end
end

defmodule Voting do
  @moduledoc "API pública do sistema de votação."

  def start_link(opts \\ []), do: Voting.Root.start_link(opts)

  def create_poll(id, question, options) when is_list(options) do
    DynamicSupervisor.start_child(
      Voting.PollSupervisor,
      {Voting.Poll, id: id, question: question, options: options}
    )
  end

  defdelegate vote(id, voter, option), to: Voting.Poll
  defdelegate close(id), to: Voting.Poll
  defdelegate result(id), to: Voting.Poll
end

defmodule Voting.Demo do
  @moduledoc false

  def run do
    {:ok, _} = Voting.start_link()

    {:ok, _} = Voting.create_poll(:linguagem, "Melhor linguagem?", [:elixir, :erlang, :go])
    {:ok, _} = Voting.create_poll(:cor, "Cor favorita?", [:azul, :verde])

    # Votações independentes sendo alimentadas concorrentemente.
    # Cada eleitor vota uma vez em cada enquete; a segunda tentativa deve ser rejeitada.
    1..1_000
    |> Task.async_stream(
      fn i ->
        voter = "eleitor_#{i}"
        Voting.vote(:linguagem, voter, Enum.random([:elixir, :erlang, :go]))
        Voting.vote(:linguagem, voter, :elixir)
        Voting.vote(:cor, voter, Enum.random([:azul, :verde]))
      end,
      max_concurrency: System.schedulers_online() * 4,
      ordered: false
    )
    |> Stream.run()

    # Condição de corrida: 500 processos tentam votar com o mesmo eleitor.
    # Exatamente uma tentativa deve ser aceita.
    race =
      1..500
      |> Task.async_stream(fn _ -> Voting.vote(:cor, "unico", :azul) end, max_concurrency: 100)
      |> Enum.map(fn {:ok, r} -> r end)

    IO.puts("Aceitos na corrida: #{Enum.count(race, &(&1 == :ok))} (esperado: 1)")

    {:ok, linguagem} = Voting.result(:linguagem)
    IO.inspect(linguagem, label: "Resultado linguagem")

    {:ok, cor} = Voting.result(:cor)
    IO.inspect(cor, label: "Resultado cor")

    :ok = Voting.close(:cor)
    IO.inspect(Voting.vote(:cor, "tardio", :verde), label: "Voto após fechamento")
    IO.inspect(Voting.vote(:linguagem, "x", :rust), label: "Opção inválida")
  end
end

Voting.Demo.run()
