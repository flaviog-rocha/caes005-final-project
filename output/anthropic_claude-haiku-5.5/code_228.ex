# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de gerenciamento de notificações, atendendo somente aos seguintes requisitos:
#
# 1. Gerar notificações quando determinadas condições forem atingidas.
# 2. Quando fizer sentido para o domínio, considerar a comunicação com um banco de dados fictício e, opcionalmente, o uso de Ecto.Migration, sem tornar isso obrigatório.
# 3. Diferenciar erros de validação de falhas operacionais.
# 4. Detectar encerramentos inesperados de processos.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# Sistema de gerenciamento de notificações (arquivo autocontido, somente biblioteca padrão/OTP)
#
# Execução sugerida: `elixir notificacoes.exs` e depois `Notificacoes.Demo.executar()`.

# ---------------------------------------------------------------------------
# Validação pura de eventos. Retorna erros de VALIDAÇÃO (entrada inválida),
# que nunca são confundidos com falhas operacionais.
# ---------------------------------------------------------------------------
defmodule Notificacoes.Validador do
  @tipos [:leitura, :pedido]

  @doc "Valida a estrutura de um evento. Retorna {:ok, evento} ou {:error, {:validacao, erros}}."
  def validar(evento) when is_map(evento) do
    erros = erros_tipo(evento) ++ erros_dados(evento)

    if erros == [],
      do: {:ok, evento},
      else: {:error, {:validacao, erros}}
  end

  def validar(_evento), do: {:error, {:validacao, [:evento_deve_ser_mapa]}}

  # Tipo do evento
  defp erros_tipo(%{tipo: tipo}) when tipo in @tipos, do: []
  defp erros_tipo(%{tipo: tipo}), do: [{:tipo_desconhecido, tipo}]
  defp erros_tipo(_), do: [:tipo_ausente]

  # Campos obrigatórios por tipo de evento
  defp erros_dados(%{tipo: :leitura, dados: %{temperatura: t}}) when is_number(t), do: []
  defp erros_dados(%{tipo: :leitura, dados: _}), do: [:temperatura_invalida]
  defp erros_dados(%{tipo: :pedido, dados: %{valor: v}}) when is_number(v), do: []
  defp erros_dados(%{tipo: :pedido, dados: _}), do: [:valor_invalido]
  defp erros_dados(_), do: [:dados_ausentes]
end

# ---------------------------------------------------------------------------
# Regras de negócio: funções puras que determinam quais notificações gerar.
# ---------------------------------------------------------------------------
defmodule Notificacoes.Regras do
  @regras [
    {:temperatura_alta, "Temperatura acima de 80°C"},
    {:pedido_de_alto_valor, "Pedido acima de R$ 10.000"}
  ]

  @doc "Retorna a lista de notificações (mapas) geradas pelas condições atingidas."
  def avaliar(evento) do
    for {id, mensagem} <- @regras, casa?(id, evento) do
      %{regra: id, mensagem: mensagem, tipo_evento: evento.tipo}
    end
  end

  # Condições
  defp casa?(:temperatura_alta, %{tipo: :leitura, dados: %{temperatura: t}}), do: t > 80
  defp casa?(:pedido_de_alto_valor, %{tipo: :pedido, dados: %{valor: v}}), do: v > 10_000
  defp casa?(_regra, _evento), do: false
end

# ---------------------------------------------------------------------------
# Banco de dados fictício (Agent). Permite simular falhas operacionais.
# Não depende de Ecto; a modelagem equivalente em migração seria uma tabela
# `notificacoes` com colunas: id, regra, mensagem, tipo_evento.
# ---------------------------------------------------------------------------
defmodule Notificacoes.BancoFalso do
  use Agent

  def start_link(_opts \\ []) do
    Agent.start_link(
      fn -> %{proximo_id: 1, linhas: [], falhas_restantes: 0} end,
      name: __MODULE__
    )
  end

  @doc "Insere um registro. Pode retornar {:error, motivo} quando há falha simulada."
  def inserir(attrs) do
    Agent.get_and_update(__MODULE__, fn
      %{falhas_restantes: n} = estado when n > 0 ->
        {{:error, :conexao_perdida}, %{estado | falhas_restantes: n - 1}}

      estado ->
        registro = Map.put(attrs, :id, estado.proximo_id)

        {{:ok, registro},
         %{estado | proximo_id: estado.proximo_id + 1, linhas: [registro | estado.linhas]}}
    end)
  end

  def listar, do: Agent.get(__MODULE__, &Enum.reverse(&1.linhas))

  @doc "Configura as próximas `n` operações de inserção para falharem (uso em testes)."
  def simular_falhas(n), do: Agent.update(__MODULE__, &%{&1 | falhas_restantes: n})
end

# ---------------------------------------------------------------------------
# Gerenciador: cada evento válido é processado por um worker monitorado.
# - Erros de validação são respondidos imediatamente, sem spawn.
# - Falhas operacionais (banco indisponível, exit do Agent) são respondidas
#   como {:error, {:operacional, motivo}}.
# - Encerramentos inesperados do worker (crash ou término sem resultado)
#   são detectados via monitor e respondidos como falha operacional.
# ---------------------------------------------------------------------------
defmodule Notificacoes.Gerenciador do
  use GenServer

  alias Notificacoes.{Validador, Regras, BancoFalso}

  def start_link(_opts \\ []), do: GenServer.start_link(__MODULE__, nil, name: __MODULE__)

  @doc "Processa um evento de forma síncrona e retorna o resultado."
  def processar(evento), do: GenServer.call(__MODULE__, {:processar, evento})

  @doc "Retorna contadores operacionais do gerenciador."
  def estatisticas, do: GenServer.call(__MODULE__, :estatisticas)

  # -- Callbacks ------------------------------------------------------------

  @impl true
  def init(_arg) do
    {:ok, %{pendentes: %{}, encerramentos_inesperados: 0}}
  end

  @impl true
  def handle_call({:processar, evento}, from, estado) do
    case Validador.validar(evento) do
      # Erro de validação: falha do chamador, respondida sem criar worker.
      {:error, _} = erro ->
        {:reply, erro, estado}

      {:ok, evento_valido} ->
        gerenciador = self()

        # Worker monitorado: qualquer término anormal gera {:DOWN, ...} no gerenciador.
        {pid, ref} =
          spawn_monitor(fn ->
            send(gerenciador, {:resultado, self(), executar(evento_valido)})
          end)

        pendentes = Map.put(estado.pendentes, pid, {ref, from, evento_valido})
        {:noreply, %{estado | pendentes: pendentes}}
    end
  end

  def handle_call(:estatisticas, _from, estado) do
    {:reply,
     %{
       pendentes: map_size(estado.pendentes),
       encerramentos_inesperados: estado.encerramentos_inesperados
     }, estado}
  end

  @impl true
  def handle_info({:resultado, pid, resultado}, estado) do
    case Map.pop(estado.pendentes, pid) do
      {{ref, from, _evento}, pendentes} ->
        # Resultado recebido: remove o monitor sem gerar mensagem :DOWN.
        Process.demonitor(ref, [:flush])
        GenServer.reply(from, resultado)
        {:noreply, %{estado | pendentes: pendentes}}

      {nil, _} ->
        {:noreply, estado}
    end
  end

  def handle_info({:DOWN, ref, :process, pid, motivo}, estado) do
    case Map.pop(estado.pendentes, pid) do
      {{^ref, from, _evento}, pendentes} ->
        # O worker terminou sem reportar resultado: encerramento inesperado,
        # mesmo que o motivo seja :normal.
        IO.puts(:stderr, "[alerta] worker #{inspect(pid)} encerrou inesperadamente: #{inspect(motivo)}")

        GenServer.reply(from, {:error, {:operacional, {:worker_encerrado, motivo}}})

        {:noreply,
         %{
           estado
           | pendentes: pendentes,
             encerramentos_inesperados: estado.encerramentos_inesperados + 1
         }}

      {_, _} ->
        {:noreply, estado}
    end
  end

  def handle_info(_msg, estado), do: {:noreply, estado}

  # -- Lógica executada no worker ------------------------------------------

  defp executar(evento) do
    evento
    |> Regras.avaliar()
    |> persistir_todas()
  end

  defp persistir_todas(notificacoes) do
    resultado =
      Enum.reduce_while(notificacoes, {:ok, []}, fn notificacao, {:ok, acumulado} ->
        case persistir(notificacao) do
          {:ok, salva} -> {:cont, {:ok, [salva | acumulado]}}
          {:error, motivo} -> {:halt, {:error, {:operacional, motivo}}}
        end
      end)

    case resultado do
      {:ok, acumulado} -> {:ok, Enum.reverse(acumulado)}
      erro -> erro
    end
  end

  # Falhas de infraestrutura do banco (exit do processo) viram erro operacional.
  defp persistir(notificacao) do
    BancoFalso.inserir(notificacao)
  catch
    :exit, motivo -> {:error, {:banco_indisponivel, motivo}}
  end
end

# ---------------------------------------------------------------------------
# Demonstração opcional.
# ---------------------------------------------------------------------------
defmodule Notificacoes.Demo do
  alias Notificacoes.{BancoFalso, Gerenciador}

  def executar do
    {:ok, _} = BancoFalso.start_link()
    {:ok, _} = Gerenciador.start_link()

    IO.puts("--- Evento válido que gera notificação ---")
    IO.inspect(Gerenciador.processar(%{tipo: :leitura, dados: %{temperatura: 95}}))

    IO.puts("--- Erro de validação (não operacional) ---")
    IO.inspect(Gerenciador.processar(%{tipo: :desconhecido}))

    IO.puts("--- Falha operacional simulada no banco ---")
    BancoFalso.simular_falhas(1)
    IO.inspect(Gerenciador.processar(%{tipo: :pedido, dados: %{valor: 20_000}}))

    IO.puts("--- Estado final ---")
    IO.inspect(BancoFalso.listar())
    IO.inspect(Gerenciador.estatisticas())
  end
end
