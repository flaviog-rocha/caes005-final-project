# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de reservas de salas, atendendo somente aos seguintes requisitos:
#
# 1. Impedir que uma mesma operação seja executada mais de uma vez.
# 2. Fazer uso de macros.
# 3. Persistir os dados para recuperação após reinicializações.
# 4. Permitir consultar o estado atual das operações em andamento.
# 5. Permitir limitar a quantidade de operações simultâneas.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# Arquivo único: gerenciamento de reservas de salas em Elixir.
#
# Uso básico (exemplo em IEx, após compilar este arquivo):
#
#   {:ok, _} = Reservas.Gerenciador.start_link(arquivo: "reservas.dets", max_simultaneas: 3)
#   Reservas.Salas.reservar("op-001", "Sala A", ~T[09:00:00], ~T[10:00:00])
#   # => {:ok, %{id: "op-001", sala: "Sala A", ...}}
#   Reservas.Salas.reservar("op-001", "Sala A", ~T[09:00:00], ~T[10:00:00])
#   # => {:ja_executada, %{id: "op-001", ...}}   (não executa de novo)
#   Reservas.Gerenciador.em_andamento()
#   Reservas.Gerenciador.estado()

defmodule Reservas do
  @moduledoc """
  Gerenciamento de reservas de salas usando apenas a biblioteca padrão e OTP.

  Requisitos atendidos:

    1. Idempotência: cada operação é identificada por uma chave textual. Uma chave
       já concluída não é reexecutada (o resultado anterior é devolvido) e uma chave
       em andamento é rejeitada.
    2. Macros: `Reservas.Operacoes.defoperacao/2` gera funções que são automaticamente
       protegidas pelo controle de idempotência e de concorrência.
    3. Persistência: o estado das operações é gravado em DETS (`:dets`), permitindo
       recuperação após reinicializações.
    4. Consulta: `Reservas.Gerenciador.em_andamento/0` e `estado/0` expõem o estado atual.
    5. Limite de concorrência: a opção `:max_simultaneas` restringe o número de
       operações executando ao mesmo tempo.
  """
end

defmodule Reservas.Gerenciador do
  @moduledoc """
  Processo central (GenServer) responsável por:

    * registrar operações em andamento e respeitar o limite de simultaneidade;
    * impedir execução duplicada de chaves já concluídas ou em andamento;
    * persistir o estado em DETS;
    * liberar operações cujo processo chamador falhou (monitor).

  Ao reiniciar, operações que estavam `:em_andamento` são marcadas como
  `:interrompida`, pois não é possível garantir se seus efeitos foram aplicados.
  Essas chaves exigem intervenção explícita via `liberar/1` antes de nova execução.
  """

  use GenServer

  @nome __MODULE__
  @tabela :reservas_operacoes
  @max_padrao 4

  # ---------------------------------------------------------------------------
  # API pública
  # ---------------------------------------------------------------------------

  @doc """
  Inicia o gerenciador.

  Opções:
    * `:arquivo` - caminho do arquivo DETS (padrão: "reservas.dets")
    * `:max_simultaneas` - limite de operações simultâneas (padrão: #{@max_padrao})
  """
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: @nome)
  end

  @doc """
  Executa `fun` sob a chave de idempotência `chave`.

  Retornos:
    * `{:ok, resultado}` - executada agora pela primeira vez
    * `{:ja_executada, resultado}` - chave já concluída anteriormente
    * `{:erro, :em_andamento}` - chave sendo executada neste momento
    * `{:erro, :interrompida}` - chave interrompida por reinicialização anterior
    * `{:erro, :limite_atingido}` - limite de simultaneidade atingido
  """
  def executar(chave, fun) when is_binary(chave) and is_function(fun, 0) do
    case GenServer.call(@nome, {:iniciar, chave, self()}) do
      :ok ->
        try do
          resultado = fun.()
          :ok = GenServer.call(@nome, {:concluir, chave, resultado})
          {:ok, resultado}
        catch
          tipo, valor ->
            # Em caso de falha, libera a chave para uma nova tentativa.
            GenServer.call(@nome, {:liberar, chave})
            :erlang.raise(tipo, valor, __STACKTRACE__)
        end

      {:cache, resultado} ->
        {:ja_executada, resultado}

      {:erro, motivo} ->
        {:erro, motivo}
    end
  end

  @doc "Lista as operações em andamento neste momento."
  def em_andamento do
    GenServer.call(@nome, :em_andamento)
  end

  @doc "Retorna um resumo do estado de concorrência."
  def estado do
    GenServer.call(@nome, :estado)
  end

  @doc """
  Remove uma chave do registro persistido (por exemplo, uma chave `:interrompida`),
  permitindo nova execução. Use com cuidado: verifique antes os efeitos já aplicados.
  """
  def liberar(chave) when is_binary(chave) do
    GenServer.call(@nome, {:liberar, chave})
  end

  @doc "Consulta o status persistido de uma chave (`:em_andamento`, `:concluida`, `:interrompida` ou `nil`)."
  def status(chave) when is_binary(chave) do
    case :dets.lookup(@tabela, chave) do
      [{^chave, status, _}] -> status
      [] -> nil
    end
  end

  # ---------------------------------------------------------------------------
  # Callbacks do GenServer
  # ---------------------------------------------------------------------------

  @impl true
  def init(opts) do
    Process.flag(:trap_exit, true)

    arquivo =
      opts
      |> Keyword.get(:arquivo, "reservas.dets")
      |> String.to_charlist()

    max = Keyword.get(opts, :max_simultaneas, @max_padrao)

    {:ok, tab} = :dets.open_file(@tabela, file: arquivo, type: :set)

    # Recuperação: operações que estavam em andamento na queda são marcadas como interrompidas.
    interrompidas =
      :dets.foldl(
        fn
          {chave, :em_andamento, _}, acc -> [chave | acc]
          _, acc -> acc
        end,
        [],
        tab
      )

    Enum.each(interrompidas, fn chave ->
      :ok = :dets.insert(tab, {chave, :interrompida, nil})
    end)

    :ok = :dets.sync(tab)

    {:ok, %{tabela: tab, max: max, ativas: %{}}}
  end

  @impl true
  def handle_call({:iniciar, chave, pid}, _from, estado) do
    %{tabela: tab, max: max, ativas: ativas} = estado

    cond do
      Map.has_key?(ativas, chave) ->
        {:reply, {:erro, :em_andamento}, estado}

      true ->
        case :dets.lookup(tab, chave) do
          [{^chave, :concluida, resultado}] ->
            {:reply, {:cache, resultado}, estado}

          [{^chave, :interrompida, _}] ->
            {:reply, {:erro, :interrompida}, estado}

          [] ->
            if map_size(ativas) >= max do
              {:reply, {:erro, :limite_atingido}, estado}
            else
              ref = Process.monitor(pid)
              :ok = :dets.insert(tab, {chave, :em_andamento, nil})
              :ok = :dets.sync(tab)

              registro = %{pid: pid, ref: ref, inicio: DateTime.utc_now()}
              {:reply, :ok, %{estado | ativas: Map.put(ativas, chave, registro)}}
            end

          _ ->
            {:reply, {:erro, :em_andamento}, estado}
        end
    end
  end

  def handle_call({:concluir, chave, resultado}, _from, estado) do
    case Map.pop(estado.ativas, chave) do
      {nil, _} ->
        {:reply, {:erro, :nao_iniciada}, estado}

      {%{ref: ref}, ativas} ->
        Process.demonitor(ref, [:flush])
        :ok = :dets.insert(estado.tabela, {chave, :concluida, resultado})
        :ok = :dets.sync(estado.tabela)
        {:reply, :ok, %{estado | ativas: ativas}}
    end
  end

  def handle_call({:liberar, chave}, _from, estado) do
    {:reply, :ok, liberar_chave(chave, estado)}
  end

  def handle_call(:em_andamento, _from, estado) do
    lista =
      for {chave, %{pid: pid, inicio: inicio}} <- estado.ativas do
        %{chave: chave, pid: pid, inicio: inicio}
      end

    {:reply, lista, estado}
  end

  def handle_call(:estado, _from, estado) do
    em_uso = map_size(estado.ativas)

    resumo = %{
      em_andamento: em_uso,
      limite: estado.max,
      disponiveis: max(estado.max - em_uso, 0)
    }

    {:reply, resumo, estado}
  end

  @impl true
  def handle_info({:DOWN, ref, :process, _pid, _motivo}, estado) do
    case Enum.find(estado.ativas, fn {_chave, %{ref: r}} -> r == ref end) do
      {chave, _} -> {:noreply, liberar_chave(chave, estado)}
      nil -> {:noreply, estado}
    end
  end

  def handle_info(_msg, estado), do: {:noreply, estado}

  @impl true
  def terminate(_motivo, %{tabela: tab}) do
    :dets.close(tab)
    :ok
  end

  # ---------------------------------------------------------------------------
  # Auxiliares privadas
  # ---------------------------------------------------------------------------

  defp liberar_chave(chave, %{tabela: tab, ativas: ativas} = estado) do
    case Map.pop(ativas, chave) do
      {nil, _} ->
        estado

      {%{ref: ref}, ativas} ->
        Process.demonitor(ref, [:flush])
        :ok = :dets.delete(tab, chave)
        :ok = :dets.sync(tab)
        %{estado | ativas: ativas}
    end
  end
end

defmodule Reservas.Operacoes do
  @moduledoc """
  Macros para definir operações idempotentes e controladas.

  Uso:

      defmodule MinhasReservas do
        use Reservas.Operacoes

        defoperacao reservar(chave, sala, inicio, fim) do
          # regra de negócio
        end
      end

  A primeira variável da lista de argumentos é usada como chave de idempotência.
  """

  @doc false
  defmacro __using__(_opts) do
    quote do
      import Reservas.Operacoes, only: [defoperacao: 2]
    end
  end

  @doc """
  Define uma função cujo corpo é executado através de `Reservas.Gerenciador.executar/2`.
  O primeiro argumento da função é a chave de idempotência (string).
  """
  defmacro defoperacao({nome, _meta, [chave | _] = args}, do: corpo) do
    quote do
      def unquote(nome)(unquote_splicing(args)) do
        Reservas.Gerenciador.executar(unquote(chave), fn -> unquote(corpo) end)
      end
    end
  end
end

defmodule Reservas.Salas do
  @moduledoc """
  Exemplo de domínio: reservas de salas definidas com a macro `defoperacao`.
  """

  use Reservas.Operacoes

  defoperacao reservar(chave, sala, inicio, fim) do
    # Regra de negócio simulada: em um sistema real, aqui haveria
    # verificação de conflitos e gravação da reserva.
    %{
      id: chave,
      sala: sala,
      inicio: inicio,
      fim: fim,
      status: :confirmada
    }
  end
end
