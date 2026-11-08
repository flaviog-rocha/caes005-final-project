# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de votação online, atendendo somente aos seguintes requisitos:
#
# 1. Persistir os dados para recuperação após reinicializações.
# 2. Implementar expiração automática de registros.
# 3. Fazer uso de macros.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# Arquivo autocontido: sistema de votação online com persistência (DETS),
# expiração automática de registros e uso de macros.
# Execução: elixir votacao.exs

# ---------------------------------------------------------------------------
# Macros utilitárias
# ---------------------------------------------------------------------------
defmodule Votacao.Macros do
  @moduledoc false

  # Define uma função pública cujo corpo pode interromper a execução com
  # `throw({:erro, motivo})`. A macro converte essa interrupção em `{:error, motivo}`.
  defmacro defcomando(assinatura, do: corpo) do
    quote do
      def unquote(assinatura) do
        try do
          unquote(corpo)
        catch
          :throw, {:erro, motivo} -> {:error, motivo}
        end
      end
    end
  end

  # Interrompe o fluxo com erro quando a condição é falsa.
  defmacro validar(condicao, motivo) do
    quote do
      if unquote(condicao), do: :ok, else: throw({:erro, unquote(motivo)})
    end
  end

  # Verifica se um instante de expiração (em milissegundos) já foi atingido.
  defmacro expirado?(expira_em) do
    quote do
      System.system_time(:millisecond) >= unquote(expira_em)
    end
  end

  # Registro de log simples com timestamp.
  defmacro log(nivel, mensagem) do
    quote do
      IO.puts(
        "[#{unquote(nivel)}] #{DateTime.utc_now() |> DateTime.to_iso8601()} #{unquote(mensagem)}"
      )
    end
  end
end

# ---------------------------------------------------------------------------
# Servidor: dono da tabela DETS e responsável pelo expurgo periódico
# ---------------------------------------------------------------------------
defmodule Votacao.Servidor do
  @moduledoc false
  use GenServer
  import Votacao.Macros

  @nome __MODULE__
  # Intervalo (ms) entre varreduras de expiração.
  @intervalo_expurgo 1_000

  def start_link(opcoes \\ []) do
    GenServer.start_link(__MODULE__, opcoes, name: @nome)
  end

  @impl true
  def init(opcoes) do
    # Permite fechar a tabela DETS corretamente em desligamentos.
    Process.flag(:trap_exit, true)

    arquivo =
      opcoes
      |> Keyword.get(:arquivo, "votacao.dets")
      |> String.to_charlist()

    # DETS garante a persistência em disco entre reinicializações.
    case :dets.open_file(:votacao_tab, file: arquivo, type: :set) do
      {:ok, tab} ->
        # Inicializa o contador de IDs apenas se ainda não existir.
        :dets.insert_new(tab, {:contador, 0})
        agendar_expurgo()
        log(:info, "tabela aberta em #{arquivo}")
        {:ok, %{tab: tab}}

      {:error, motivo} ->
        {:stop, motivo}
    end
  end

  @impl true
  def handle_call({:criar, titulo, opcoes, ttl}, _from, %{tab: tab} = estado) do
    id = :dets.update_counter(tab, :contador, 1)
    agora = System.system_time(:millisecond)

    enquete = %{
      id: id,
      titulo: titulo,
      opcoes: opcoes,
      criada_em: agora,
      expira_em: agora + ttl
    }

    :ok = :dets.insert(tab, {{:enquete, id}, enquete})
    :ok = :dets.sync(tab)
    {:reply, {:ok, id}, estado}
  end

  def handle_call({:votar, id, eleitor, opcao}, _from, %{tab: tab} = estado) do
    resposta =
      with {:ok, enquete} <- buscar_enquete(tab, id),
           :ok <- validar_opcao(enquete, opcao),
           :ok <- nao_votou(tab, id, eleitor) do
        # O voto expira junto com a enquete.
        registro = %{opcao: opcao, expira_em: enquete.expira_em}
        :ok = :dets.insert(tab, {{:voto, id, eleitor}, registro})
        :ok = :dets.sync(tab)
        :ok
      end

    {:reply, resposta, estado}
  end

  def handle_call({:resultado, id}, _from, %{tab: tab} = estado) do
    resposta =
      case buscar_enquete(tab, id) do
        {:ok, enquete} ->
          inicial = Map.new(enquete.opcoes, &{&1, 0})

          contagem =
            :dets.foldl(
              fn
                {{:voto, ^id, _eleitor}, %{opcao: opcao}}, acc ->
                  Map.update(acc, opcao, 1, &(&1 + 1))

                _, acc ->
                  acc
              end,
              inicial,
              tab
            )

          {:ok, %{titulo: enquete.titulo, votos: contagem}}

        erro ->
          erro
      end

    {:reply, resposta, estado}
  end

  def handle_call(:listar, _from, %{tab: tab} = estado) do
    enquetes =
      :dets.foldl(
        fn
          {{:enquete, id}, enquete}, acc ->
            if expirado?(enquete.expira_em) do
              acc
            else
              [{id, enquete.titulo} | acc]
            end

          _, acc ->
            acc
        end,
        [],
        tab
      )

    {:reply, Enum.sort(enquetes), estado}
  end

  @impl true
  def handle_info(:expurgar, %{tab: tab} = estado) do
    expurgar(tab)
    agendar_expurgo()
    {:noreply, estado}
  end

  @impl true
  def terminate(_motivo, %{tab: tab}) do
    :dets.close(tab)
  end

  # -------------------------------------------------------------------------
  # Funções auxiliares
  # -------------------------------------------------------------------------

  # Expiração "preguiçosa": uma enquete expirada é tratada como inexistente
  # mesmo antes de ser fisicamente removida pelo expurgo.
  defp buscar_enquete(tab, id) do
    case :dets.lookup(tab, {:enquete, id}) do
      [{_, enquete}] ->
        if expirado?(enquete.expira_em) do
          {:error, :nao_encontrada}
        else
          {:ok, enquete}
        end

      [] ->
        {:error, :nao_encontrada}
    end
  end

  defp validar_opcao(%{opcoes: opcoes}, opcao) do
    if opcao in opcoes, do: :ok, else: {:error, :opcao_invalida}
  end

  defp nao_votou(tab, id, eleitor) do
    case :dets.lookup(tab, {:voto, id, eleitor}) do
      [] -> :ok
      [_] -> {:error, :ja_votou}
    end
  end

  # Remove fisicamente todos os registros expirados (enquetes e votos).
  defp expurgar(tab) do
    agora = System.system_time(:millisecond)

    expirados =
      :dets.foldl(
        fn
          {chave, %{expira_em: exp}}, acc when exp <= agora -> [chave | acc]
          _, acc -> acc
        end,
        [],
        tab
      )

    if expirados != [] do
      Enum.each(expirados, &:dets.delete(tab, &1))
      :ok = :dets.sync(tab)
      log(:info, "#{length(expirados)} registro(s) expirado(s) removido(s)")
    end

    :ok
  end

  defp agendar_expurgo do
    Process.send_after(self(), :expurgar, @intervalo_expurgo)
  end
end

# ---------------------------------------------------------------------------
# API pública do sistema de votação
# ---------------------------------------------------------------------------
defmodule Votacao do
  @moduledoc """
  API pública:

    * `criar/3`    - cria uma enquete com TTL em milissegundos
    * `votar/3`    - registra o voto de um eleitor (uma vez por enquete)
    * `resultado/1` - retorna a contagem de votos
    * `listar/0`   - lista as enquetes vigentes
  """
  import Votacao.Macros
  alias Votacao.Servidor

  defcomando criar(titulo, opcoes, ttl_ms) do
    validar(is_binary(titulo) and titulo != "", :titulo_invalido)

    validar(
      is_list(opcoes) and length(opcoes) >= 2 and
        Enum.all?(opcoes, &(is_binary(&1) and &1 != "")),
      :opcoes_invalidas
    )

    validar(length(Enum.uniq(opcoes)) == length(opcoes), :opcoes_duplicadas)
    validar(is_integer(ttl_ms) and ttl_ms > 0, :ttl_invalido)

    GenServer.call(Servidor, {:criar, titulo, opcoes, ttl_ms})
  end

  defcomando votar(id, eleitor, opcao) do
    validar(
      is_integer(id) and is_binary(eleitor) and is_binary(opcao),
      :argumentos_invalidos
    )

    GenServer.call(Servidor, {:votar, id, eleitor, opcao})
  end

  defcomando resultado(id) do
    validar(is_integer(id), :id_invalido)
    GenServer.call(Servidor, {:resultado, id})
  end

  defcomando listar() do
    GenServer.call(Servidor, :listar)
  end
end

# ---------------------------------------------------------------------------
# Demonstração de uso (executada ao rodar o arquivo como script)
# ---------------------------------------------------------------------------
{:ok, _pid} = Votacao.Servidor.start_link(arquivo: "votacao.dets")

{:ok, id} = Votacao.criar("Linguagem favorita", ["Elixir", "Erlang", "Haskell"], 5_000)
IO.puts("Enquete criada com id #{id}")

:ok = Votacao.votar(id, "ana", "Elixir")
{:error, :ja_votou} = Votacao.votar(id, "ana", "Erlang")
{:error, :opcao_invalida} = Votacao.votar(id, "bruno", "Python")
:ok = Votacao.votar(id, "bruno", "Erlang")

IO.inspect(Votacao.resultado(id), label: "Resultado antes da expiração")
IO.inspect(Votacao.listar(), label: "Enquetes vigentes")

# Aguarda a expiração (TTL de 5s).
Process.sleep(6_000)

IO.inspect(Votacao.resultado(id), label: "Resultado após expiração")
IO.inspect(Votacao.listar(), label: "Enquetes vigentes após expiração")
