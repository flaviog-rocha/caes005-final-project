# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de troca de mensagens entre usuários, atendendo somente aos seguintes requisitos:
#
# 1. Recuperar processos após falhas inesperadas.
# 2. Garantir a consistência dos dados durante operações concorrentes.
# 3. Fazer uso de macros.
# 4. Permitir cadastrar, consultar, atualizar e remover registros.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# Sistema de troca de mensagens entre usuários (arquivo único autocontido).
# Execução: elixir mensagens.exs
#
# Requisitos atendidos:
# 1. Recuperação de falhas: o Store roda sob um Supervisor (:one_for_one).
#    Cada mutação é persistida de forma atômica (escrita em .tmp + rename)
#    antes de responder ao cliente; ao reiniciar, o estado é recarregado.
# 2. Consistência em concorrência: todas as escritas são serializadas pelo
#    GenServer, há controle otimista por versão (compare-and-swap) e validação
#    de integridade referencial dentro da mesma operação atômica.
# 3. Macros: `defcrud/1` gera a API CRUD de cada entidade e `com_retry/1`
#    encapsula a política de nova tentativa durante reinício do processo.
# 4. CRUD completo: cadastrar, consultar, listar, atualizar e remover.

defmodule MsgSystem.Macros do
  @moduledoc false

  # Gera as funções públicas CRUD de uma entidade, delegando ao Store.
  defmacro defcrud(entidade) do
    quote do
      def cadastrar(attrs) when is_map(attrs),
        do: MsgSystem.Store.call({:criar, unquote(entidade), attrs})

      def consultar(id),
        do: MsgSystem.Store.call({:ler, unquote(entidade), id})

      def listar,
        do: MsgSystem.Store.call({:listar, unquote(entidade)})

      def atualizar(id, attrs, versao) when is_map(attrs) and is_integer(versao),
        do: MsgSystem.Store.call({:atualizar, unquote(entidade), id, attrs, versao})

      def remover(id),
        do: MsgSystem.Store.call({:remover, unquote(entidade), id})
    end
  end

  # Executa o bloco com nova tentativa caso o processo esteja reiniciando (:noproc).
  # Apenas :noproc é repetido, pois nesse caso a operação não chegou a ser executada,
  # evitando duplicidade de escritas.
  defmacro com_retry(do: expressao) do
    quote do
      MsgSystem.Macros.__retry__(fn -> unquote(expressao) end, 5)
    end
  end

  @doc false
  def __retry__(fun, tentativas) do
    fun.()
  catch
    :exit, {:noproc, _} when tentativas > 1 ->
      Process.sleep(50)
      __retry__(fun, tentativas - 1)
  end
end

defmodule MsgSystem.Store do
  @moduledoc """
  Processo dono do estado. Serializa todas as operações (consistência),
  persiste atomicamente após cada mutação (durabilidade) e recarrega
  o último snapshot válido ao ser reiniciado pelo supervisor (recuperação).
  """
  use GenServer
  import MsgSystem.Macros, only: [com_retry: 1]

  @nome __MODULE__
  @timeout 5_000

  # ---------------------------------------------------------------------------
  # API interna
  # ---------------------------------------------------------------------------

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: @nome)
  end

  @doc false
  def call(msg) do
    com_retry do
      GenServer.call(@nome, msg, @timeout)
    end
  end

  # ---------------------------------------------------------------------------
  # Callbacks
  # ---------------------------------------------------------------------------

  @impl true
  def init(opts) do
    caminho = Keyword.get(opts, :caminho, Path.join(System.tmp_dir!(), "msg_system.bin"))
    dados = carregar(caminho)
    {:ok, Map.put(dados, :caminho, caminho)}
  end

  @impl true
  def handle_call({:ler, entidade, id}, _from, st) do
    case get_in(st.dados, [entidade, id]) do
      nil -> {:reply, {:error, :nao_encontrado}, st}
      registro -> {:reply, {:ok, registro}, st}
    end
  end

  def handle_call({:listar, entidade}, _from, st) do
    lista =
      st.dados
      |> Map.fetch!(entidade)
      |> Map.values()
      |> Enum.sort_by(& &1.id)

    {:reply, {:ok, lista}, st}
  end

  def handle_call({:criar, entidade, attrs}, _from, st) do
    resultado =
      case validar(entidade, attrs, st, nil) do
        :ok ->
          id = Map.fetch!(st.seq, entidade) + 1

          registro = %{
            id: id,
            versao: 1,
            dados: attrs,
            criado_em: DateTime.utc_now()
          }

          novo = %{
            st
            | seq: Map.put(st.seq, entidade, id),
              dados: put_in(st.dados, [entidade, id], registro)
          }

          {:ok, novo, {:ok, registro}}

        erro ->
          erro
      end

    responder(st, resultado)
  end

  def handle_call({:atualizar, entidade, id, attrs, versao}, _from, st) do
    responder(st, atualizar_registro(st, entidade, id, attrs, versao))
  end

  def handle_call({:remover, entidade, id}, _from, st) do
    responder(st, remover_registro(st, entidade, id))
  end

  # ---------------------------------------------------------------------------
  # Operações puras sobre o estado
  # ---------------------------------------------------------------------------

  defp atualizar_registro(st, entidade, id, attrs, versao) do
    case get_in(st.dados, [entidade, id]) do
      nil ->
        {:error, :nao_encontrado}

      %{versao: v} when v != versao ->
        {:error, {:conflito, v}}

      registro ->
        novos_dados = Map.merge(registro.dados, attrs)

        case validar(entidade, novos_dados, st, id) do
          :ok ->
            novo_registro = %{registro | dados: novos_dados, versao: registro.versao + 1}
            novo = %{st | dados: put_in(st.dados, [entidade, id], novo_registro)}
            {:ok, novo, {:ok, novo_registro}}

          erro ->
            erro
        end
    end
  end

  defp remover_registro(st, entidade, id) do
    cond do
      get_in(st.dados, [entidade, id]) == nil ->
        {:error, :nao_encontrado}

      entidade == :usuarios and referenciado?(st, id) ->
        {:error, :usuario_com_mensagens}

      true ->
        dados = Map.update!(st.dados, entidade, &Map.delete(&1, id))
        {:ok, %{st | dados: dados}, :ok}
    end
  end

  # ---------------------------------------------------------------------------
  # Validações de integridade
  # ---------------------------------------------------------------------------

  defp validar(:usuarios, attrs, st, ignorar) do
    with :ok <- texto(attrs, :nome),
         :ok <- texto(attrs, :email) do
      if email_em_uso?(st, attrs.email, ignorar),
        do: {:error, :email_duplicado},
        else: :ok
    end
  end

  defp validar(:mensagens, attrs, st, _ignorar) do
    with :ok <- texto(attrs, :corpo),
         :ok <- existe_usuario(st, Map.get(attrs, :de)),
         :ok <- existe_usuario(st, Map.get(attrs, :para)) do
      :ok
    end
  end

  defp texto(attrs, campo) do
    case Map.get(attrs, campo) do
      valor when is_binary(valor) and byte_size(valor) > 0 -> :ok
      _ -> {:error, {:campo_invalido, campo}}
    end
  end

  defp existe_usuario(st, id) do
    if Map.has_key?(st.dados.usuarios, id),
      do: :ok,
      else: {:error, :usuario_inexistente}
  end

  defp email_em_uso?(st, email, ignorar) do
    Enum.any?(st.dados.usuarios, fn {id, registro} ->
      id != ignorar and registro.dados.email == email
    end)
  end

  defp referenciado?(st, id) do
    Enum.any?(st.dados.mensagens, fn {_, m} ->
      m.dados.de == id or m.dados.para == id
    end)
  end

  # ---------------------------------------------------------------------------
  # Persistência atômica e recuperação
  # ---------------------------------------------------------------------------

  # Só responde ao cliente após persistir; em caso de falha, mantém o estado anterior.
  defp responder(st, {:ok, novo, resposta}) do
    case persistir(novo) do
      :ok -> {:reply, resposta, novo}
      {:error, motivo} -> {:reply, {:error, {:persistencia, motivo}}, st}
    end
  end

  defp responder(st, erro), do: {:reply, erro, st}

  defp persistir(%{caminho: caminho} = st) do
    tmp = caminho <> ".tmp"
    bin = :erlang.term_to_binary(%{dados: st.dados, seq: st.seq})

    with :ok <- File.write(tmp, bin),
         :ok <- File.rename(tmp, caminho) do
      :ok
    end
  end

  defp carregar(caminho) do
    vazio = %{
      dados: %{usuarios: %{}, mensagens: %{}},
      seq: %{usuarios: 0, mensagens: 0}
    }

    # Tenta o snapshot principal e, se estiver corrompido, o temporário.
    Enum.find_value([caminho, caminho <> ".tmp"], vazio, fn arquivo ->
      with {:ok, bin} <- File.read(arquivo),
           {:ok, snapshot} <- decodificar(bin) do
        snapshot
      else
        _ -> nil
      end
    end)
  end

  defp decodificar(bin) do
    {:ok, :erlang.binary_to_term(bin, [:safe])}
  rescue
    ArgumentError -> :error
  end
end

defmodule MsgSystem.Usuarios do
  @moduledoc "CRUD de usuários gerado pela macro `defcrud`."
  import MsgSystem.Macros

  defcrud(:usuarios)
end

defmodule MsgSystem.Mensagens do
  @moduledoc "CRUD de mensagens e operações de domínio de troca de mensagens."
  import MsgSystem.Macros

  defcrud(:mensagens)

  def enviar(de, para, corpo) do
    cadastrar(%{de: de, para: para, corpo: corpo, lida: false})
  end

  def caixa_de_entrada(usuario_id) do
    with {:ok, mensagens} <- listar() do
      {:ok, Enum.filter(mensagens, &(&1.dados.para == usuario_id))}
    end
  end

  def marcar_lida(id) do
    with {:ok, mensagem} <- consultar(id) do
      atualizar(id, %{lida: true}, mensagem.versao)
    end
  end
end

defmodule MsgSystem do
  @moduledoc "Ponto de entrada: inicia o supervisor do sistema."

  def start(opts \\ []) do
    Supervisor.start_link(
      [{MsgSystem.Store, opts}],
      strategy: :one_for_one,
      name: MsgSystem.Supervisor
    )
  end
end

defmodule MsgSystem.Demo do
  @moduledoc "Demonstração de CRUD, concorrência e recuperação após falha."
  alias MsgSystem.{Usuarios, Mensagens}

  def run do
    caminho = Path.join(System.tmp_dir!(), "msg_system_demo.bin")
    File.rm(caminho)
    {:ok, _sup} = MsgSystem.start(caminho: caminho)

    # CRUD e integridade
    {:ok, ana} = Usuarios.cadastrar(%{nome: "Ana", email: "ana@x.com"})
    {:ok, bruno} = Usuarios.cadastrar(%{nome: "Bruno", email: "bruno@x.com"})
    IO.inspect(Usuarios.cadastrar(%{nome: "Outra", email: "ana@x.com"}), label: "email duplicado")

    {:ok, _} = Mensagens.enviar(ana.id, bruno.id, "Oi, Bruno!")
    {:ok, _} = Mensagens.enviar(bruno.id, ana.id, "Oi, Ana!")

    {:ok, caixa} = Mensagens.caixa_de_entrada(bruno.id)
    IO.inspect(Enum.map(caixa, & &1.dados.corpo), label: "caixa do Bruno")

    # Concorrência: apenas uma atualização com a versão 1 deve ser aceita
    resultados =
      1..20
      |> Enum.map(fn i ->
        Task.async(fn -> Usuarios.atualizar(ana.id, %{nome: "Ana #{i}"}, 1) end)
      end)
      |> Enum.map(&Task.await/1)

    IO.inspect(Enum.frequencies_by(resultados, &elem(&1, 0)), label: "atualizações concorrentes")

    # Falha inesperada: mata o Store e aguarda sua finalização
    pid = Process.whereis(MsgSystem.Store)
    ref = Process.monitor(pid)
    Process.exit(pid, :kill)

    receive do
      {:DOWN, ^ref, _, _, _} -> :ok
    end

    # O supervisor reinicia o Store e recarrega o snapshot
    {:ok, ana_atual} = Usuarios.consultar(ana.id)
    IO.inspect(ana_atual.dados.nome, label: "após recuperação")

    IO.inspect(Usuarios.remover(bruno.id), label: "remover bruno com mensagens")
    IO.inspect(Usuarios.remover(999), label: "remover inexistente")
    :ok
  end
end

MsgSystem.Demo.run()
