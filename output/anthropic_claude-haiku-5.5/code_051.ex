# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de transações financeiras, atendendo somente aos seguintes requisitos:
#
# 1. Permitir cadastrar, consultar, atualizar e remover registros.
# 2. Permitir consultar o estado atual das operações em andamento.
# 3. Executar determinadas operações periodicamente.
# 4. Processar grandes volumes de dados sem carregá-los integralmente na memória.
# 5. Permitir a execução de operações simultâneas.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - Registry
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# Sistema de gerenciamento de transações financeiras.
# Arquivo único e autocontido. Requer apenas a biblioteca padrão do Elixir/OTP.
#
# Componentes:
#   * Financeiro.Sistema       - Supervisor raiz (Registry, Task.Supervisor, DynamicSupervisor, Store)
#   * Financeiro.Store         - Armazenamento em ETS (leituras concorrentes, escritas serializadas)
#   * Financeiro.Transacoes    - API de CRUD (cadastrar, consultar, atualizar, remover)
#   * Financeiro.Operacoes     - Registro e consulta de operações em andamento via Registry
#   * Financeiro.Processamento - Processamento em lotes e em streaming sobre o armazenamento
#   * Financeiro.Importacao    - Importação de arquivos grandes via File.stream!
#   * Financeiro.Agendador     - Operações periódicas (um processo por job)
#   * Financeiro.Job           - GenServer que executa um job periodicamente

defmodule Financeiro.Sistema do
  @moduledoc """
  Supervisor raiz do sistema. Inicia o Registry (nomes de operações e jobs),
  o Task.Supervisor (execução concorrente de operações), o DynamicSupervisor
  (jobs periódicos) e o Store (armazenamento).
  """
  use Supervisor

  def start_link(opts \\ []), do: Supervisor.start_link(__MODULE__, opts, name: __MODULE__)

  @impl true
  def init(_opts) do
    children = [
      # Registry para localizar operações ({:op, id}) e jobs ({:job, nome}) por chave
      {Registry, keys: :unique, name: Financeiro.Registry},
      # Execução concorrente de operações, isoladas umas das outras
      {Task.Supervisor, name: Financeiro.TarefasSup},
      # Supervisiona os jobs periódicos
      {DynamicSupervisor, strategy: :one_for_one, name: Financeiro.JobsSup},
      # Armazenamento ETS
      Financeiro.Store
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end

defmodule Financeiro.Store do
  @moduledoc """
  Armazenamento em ETS. A tabela é pública para permitir leituras concorrentes
  sem gargalo no processo; escritas são serializadas pelo GenServer para manter
  as validações de atualização consistentes.
  """
  use GenServer

  @tabela :financeiro_transacoes

  def start_link(_), do: GenServer.start_link(__MODULE__, nil, name: __MODULE__)

  @impl true
  def init(_) do
    :ets.new(@tabela, [:set, :named_table, :public, read_concurrency: true])
    {:ok, nil}
  end

  # ---------- Leituras (executadas no processo chamador) ----------

  @doc "Busca um registro pelo id. Retorna a lista de ocorrências (vazia ou [{id, reg}])."
  def buscar(id), do: :ets.lookup(@tabela, id)

  @doc "Quantidade de registros armazenados (operação O(1))."
  def contar, do: :ets.info(@tabela, :size)

  @doc """
  Retorna um Stream lazy de registros {id, reg}. Os registros são lidos da ETS
  em páginas de `tamanho_lote` usando continuações, evitando carregar a tabela inteira.
  """
  def stream(tamanho_lote) do
    ms = [{{:_, :_}, [], [:"$_"]}]

    Stream.resource(
      fn -> pagina(:ets.select(@tabela, ms, tamanho_lote)) end,
      fn
        :fim -> {:halt, :fim}
        {itens, cont} -> {itens, pagina(:ets.select(cont))}
      end,
      fn _ -> :ok end
    )
  end

  defp pagina(:"$end_of_table"), do: :fim
  defp pagina({itens, cont}), do: {itens, cont}

  # ---------- Escritas (serializadas pelo GenServer) ----------

  @doc "Insere uma lista de registros {id, reg} em uma única chamada."
  def inserir_lote(registros), do: GenServer.call(__MODULE__, {:inserir, registros})

  @doc "Atualiza um registro aplicando `fun`, que deve retornar {:ok, novo} ou {:error, motivo}."
  def atualizar(id, fun), do: GenServer.call(__MODULE__, {:atualizar, id, fun})

  @doc "Remove um registro pelo id."
  def remover(id), do: GenServer.call(__MODULE__, {:remover, id})

  @impl true
  def handle_call({:inserir, registros}, _from, estado) do
    :ets.insert(@tabela, registros)
    {:reply, :ok, estado}
  end

  def handle_call({:atualizar, id, fun}, _from, estado) do
    resposta =
      case :ets.lookup(@tabela, id) do
        [{^id, reg}] ->
          case fun.(reg) do
            {:ok, novo} ->
              :ets.insert(@tabela, {id, novo})
              {:ok, novo}

            erro ->
              erro
          end

        [] ->
          {:error, :nao_encontrado}
      end

    {:reply, resposta, estado}
  end

  def handle_call({:remover, id}, _from, estado) do
    resposta =
      case :ets.take(@tabela, id) do
        [] -> {:error, :nao_encontrado}
        _ -> :ok
      end

    {:reply, resposta, estado}
  end
end

defmodule Financeiro.Transacoes do
  @moduledoc """
  API de CRUD de transações financeiras.

  Uma transação possui: `:descricao` (string), `:valor` (inteiro em centavos > 0)
  e `:tipo` (`:credito` ou `:debito`).
  """
  alias Financeiro.Store

  @doc "Cadastra uma transação e retorna {:ok, id}."
  def cadastrar(attrs) do
    with {:ok, reg} <- validar(attrs) do
      id = gerar_id()
      :ok = Store.inserir_lote([{id, reg}])
      {:ok, id}
    end
  end

  @doc "Consulta uma transação pelo id."
  def consultar(id) do
    case Store.buscar(id) do
      [{^id, reg}] -> {:ok, reg}
      [] -> {:error, :nao_encontrado}
    end
  end

  @doc "Atualiza parcialmente uma transação, revalidando o resultado final."
  def atualizar(id, attrs) do
    Store.atualizar(id, fn reg -> validar(Map.merge(reg, attrs)) end)
  end

  @doc "Remove uma transação pelo id."
  def remover(id), do: Store.remover(id)

  @doc "Valida os campos de uma transação. Ignora chaves desconhecidas."
  def validar(attrs) do
    reg = Map.take(attrs, [:descricao, :valor, :tipo])

    cond do
      not is_binary(reg[:descricao]) -> {:error, :descricao_invalida}
      not (is_integer(reg[:valor]) and reg[:valor] > 0) -> {:error, :valor_invalido}
      reg[:tipo] not in [:credito, :debito] -> {:error, :tipo_invalido}
      true -> {:ok, reg}
    end
  end

  @doc "Gera um identificador único e monotônico."
  def gerar_id, do: :erlang.unique_integer([:positive, :monotonic])
end

defmodule Financeiro.Operacoes do
  @moduledoc """
  Execução concorrente de operações e consulta de seu estado.

  Cada operação roda em um Task isolado que se registra no Registry com a chave
  `{:op, id}` e um mapa de metadados. Enquanto estiver em execução, seu estado
  pode ser consultado. Ao terminar, o processo encerra e o registro é removido
  automaticamente pelo Registry.
  """
  @registry Financeiro.Registry
  @tarefas Financeiro.TarefasSup

  @doc """
  Inicia uma operação concorrente. `fun` recebe o id da operação como argumento.
  Retorna {:ok, id, pid} após o registro no Registry.
  """
  def iniciar(tipo, fun) when is_function(fun, 1) do
    id = Financeiro.Transacoes.gerar_id()
    pai = self()

    {:ok, pid} =
      Task.Supervisor.start_child(@tarefas, fn ->
        {:ok, _} =
          Registry.register(@registry, {:op, id}, %{
            id: id,
            tipo: tipo,
            status: :executando,
            processados: 0,
            total: nil,
            iniciado_em: DateTime.utc_now()
          })

        # Handshake: confirma ao chamador que o registro já existe
        send(pai, {:registrada, id})
        fun.(id)
      end)

    receive do
      {:registrada, ^id} -> {:ok, id, pid}
    after
      5_000 -> {:error, :timeout}
    end
  end

  @doc "Atualiza o progresso de uma operação. Deve ser chamado pelo próprio processo da operação."
  def progresso(id, processados, total) do
    Registry.update_value(@registry, {:op, id}, &Map.merge(&1, %{processados: processados, total: total}))
    :ok
  end

  @doc "Retorna o estado atual de uma operação em andamento."
  def estado(id) do
    case Registry.lookup(@registry, {:op, id}) do
      [{_pid, meta}] -> {:ok, meta}
      [] -> {:error, :nao_encontrada_ou_concluida}
    end
  end

  @doc "Lista todas as operações em andamento como [{id, metadados}]."
  def listar do
    Registry.select(@registry, [{{{:op, :"$1"}, :"$2", :"$3"}, [], [{{:"$1", :"$3"}}]}])
  end
end

defmodule Financeiro.Processamento do
  @moduledoc """
  Processamento de grandes volumes sem carregar a base inteira na memória.
  Os registros são lidos em lotes via Stream, e cada lote é processado
  concorrentemente com Task.async_stream, respeitando um limite de concorrência.
  """
  alias Financeiro.{Operacoes, Store}

  @doc """
  Aplica `fun` a cada registro {id, reg} em lotes concorrentes.
  Opções: `:lote` (tamanho do lote, padrão 500) e `:concorrencia` (padrão: schedulers online).
  Retorna {:ok, id_operacao, pid}.
  """
  def executar(fun, opts \\ []) when is_function(fun, 1) do
    lote = Keyword.get(opts, :lote, 500)
    concorrencia = Keyword.get(opts, :concorrencia, System.schedulers_online())

    Operacoes.iniciar(:processamento, fn op_id ->
      total = Store.contar()

      Store.stream(lote)
      |> Stream.chunk_every(lote)
      |> Task.async_stream(
        fn bloco -> Enum.map(bloco, fun) end,
        max_concurrency: concorrencia,
        timeout: :infinity,
        ordered: false
      )
      |> Enum.reduce(0, fn {:ok, resultados}, feitos ->
        feitos = feitos + length(resultados)
        Operacoes.progresso(op_id, feitos, total)
        feitos
      end)
    end)
  end
end

defmodule Financeiro.Importacao do
  @moduledoc """
  Importação de arquivos grandes em streaming. O arquivo é lido linha a linha
  com File.stream!, agrupado em lotes e inserido no Store a cada lote.

  Formato de cada linha: `descricao;valor_em_centavos;credito|debito`
  """
  alias Financeiro.{Operacoes, Store, Transacoes}

  @doc "Importa um CSV de forma assíncrona. Retorna {:ok, id_operacao, pid}."
  def csv(caminho, opts \\ []) do
    lote = Keyword.get(opts, :lote, 500)

    Operacoes.iniciar(:importacao, fn op_id ->
      File.stream!(caminho)
      |> Stream.map(&String.trim/1)
      |> Stream.reject(&(&1 == ""))
      |> Stream.flat_map(fn linha ->
        case parse_linha(linha) do
          {:ok, reg} -> [reg]
          _ -> []
        end
      end)
      |> Stream.chunk_every(lote)
      |> Enum.reduce(0, fn bloco, feitos ->
        registros = Enum.map(bloco, fn reg -> {Transacoes.gerar_id(), reg} end)
        :ok = Store.inserir_lote(registros)
        feitos = feitos + length(bloco)
        Operacoes.progresso(op_id, feitos, nil)
        feitos
      end)
    end)
  end

  defp parse_linha(linha) do
    case String.split(linha, ";", trim: true) do
      [descricao, valor, tipo] ->
        with {v, ""} <- Integer.parse(valor),
             {:ok, reg} <-
               Transacoes.validar(%{descricao: descricao, valor: v, tipo: tipo_atom(tipo)}) do
          {:ok, reg}
        else
          _ -> :erro
        end

      _ ->
        :erro
    end
  end

  # Evita String.to_atom/1 com entrada externa
  defp tipo_atom("credito"), do: :credito
  defp tipo_atom("debito"), do: :debito
  defp tipo_atom(_), do: nil
end

defmodule Financeiro.Job do
  @moduledoc """
  GenServer que executa uma função periodicamente como operação concorrente.
  Se a execução anterior ainda estiver em andamento, o disparo é ignorado
  para evitar sobreposição.
  """
  use GenServer

  def start_link({nome, intervalo, fun}) do
    GenServer.start_link(__MODULE__, {nome, intervalo, fun},
      name: {:via, Registry, {Financeiro.Registry, {:job, nome}}}
    )
  end

  @impl true
  def init({nome, intervalo, fun}) do
    estado = %{nome: nome, intervalo: intervalo, fun: fun, op: nil}
    {:ok, agendar(estado)}
  end

  @impl true
  def handle_info(:tick, %{op: nil} = estado) do
    case Financeiro.Operacoes.iniciar({:periodico, estado.nome}, estado.fun) do
      {:ok, _id, pid} ->
        ref = Process.monitor(pid)
        {:noreply, agendar(%{estado | op: ref})}

      _ ->
        {:noreply, agendar(estado)}
    end
  end

  def handle_info(:tick, estado), do: {:noreply, agendar(estado)}

  def handle_info({:DOWN, ref, :process, _pid, _motivo}, %{op: ref} = estado) do
    {:noreply, %{estado | op: nil}}
  end

  def handle_info(_msg, estado), do: {:noreply, estado}

  defp agendar(estado) do
    Process.send_after(self(), :tick, estado.intervalo)
    estado
  end
end

defmodule Financeiro.Agendador do
  @moduledoc """
  Gerencia operações periódicas. Cada job é um processo supervisionado
  pelo DynamicSupervisor e registrado no Registry como `{:job, nome}`.
  """

  @doc "Agenda `fun` (recebe o id da operação) para execução a cada `intervalo_ms`."
  def agendar(nome, intervalo_ms, fun) when is_function(fun, 1) do
    DynamicSupervisor.start_child(
      Financeiro.JobsSup,
      {Financeiro.Job, {nome, intervalo_ms, fun}}
    )
  end

  @doc "Cancela um job periódico pelo nome."
  def cancelar(nome) do
    case Registry.lookup(Financeiro.Registry, {:job, nome}) do
      [{pid, _}] -> DynamicSupervisor.terminate_child(Financeiro.JobsSup, pid)
      [] -> {:error, :nao_encontrado}
    end
  end
end

# Exemplo de uso (referência):
#
#   {:ok, _} = Financeiro.Sistema.start_link()
#
#   {:ok, id} = Financeiro.Transacoes.cadastrar(%{descricao: "Venda", valor: 1500, tipo: :credito})
#   {:ok, reg} = Financeiro.Transacoes.consultar(id)
#   :ok = Financeiro.Transacoes.atualizar(id, %{valor: 1800}) |> then(fn {:ok, _} -> :ok end)
#   :ok = Financeiro.Transacoes.remover(id)
#
#   {:ok, op, _} = Financeiro.Importacao.csv("/caminho/grande.csv")
#   {:ok, meta} = Financeiro.Operacoes.estado(op)
#   Financeiro.Operacoes.listar()
#
#   Financeiro.Processamento.executar(fn {_id, reg} -> reg.valor * 2 end, lote: 1000)
#
#   Financeiro.Agendador.agendar(:conciliacao, 60_000, fn _op_id ->
#     Financeiro.Processamento.executar(fn {_id, _reg} -> :ok end)
#   end)
#   Financeiro.Agendador.cancelar(:conciliacao)
