# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de empréstimos de livros, atendendo somente aos seguintes requisitos:
#
# 1. Garantir a consistência dos dados durante operações concorrentes.
# 2. Permitir comunicação entre diferentes processos.
# 3. Permitir que diferentes entidades sejam processadas independentemente.
# 4. Validar os dados recebidos antes de realizar operações.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# Arquivo único: gerenciamento de empréstimos de livros em Elixir (somente biblioteca padrão)
#
# Estratégia de concorrência:
#   * Cada livro é um processo GenServer independente, registrado via Registry.
#   * Todas as mudanças de estado de um livro passam pela sua mailbox, então
#     dois empréstimos concorrentes do mesmo livro nunca são aceitos ao mesmo tempo.
#   * Livros diferentes são processados em paralelo, sem bloqueio mútuo.
#   * Eventos são publicados via Registry (modo :duplicate) para quem assinar.
#   * Toda entrada é validada antes de atingir os processos.

defmodule Biblioteca.Validador do
  @moduledoc """
  Valida os dados de entrada antes de qualquer operação.
  """

  @doc "Valida um identificador (string não vazia)."
  def id(valor) when is_binary(valor) do
    if String.trim(valor) != "", do: {:ok, valor}, else: {:error, {:id_invalido, valor}}
  end

  def id(valor), do: {:error, {:id_invalido, valor}}

  @doc "Valida os atributos para cadastro de um livro."
  def livro(attrs) when is_map(attrs) do
    with {:ok, id} <- id(Map.get(attrs, :id)),
         {:ok, titulo} <- titulo(Map.get(attrs, :titulo)) do
      {:ok, %{id: id, titulo: titulo}}
    end
  end

  def livro(attrs), do: {:error, {:atributos_invalidos, attrs}}

  defp titulo(valor) when is_binary(valor) do
    if String.trim(valor) != "", do: {:ok, String.trim(valor)}, else: {:error, {:titulo_invalido, valor}}
  end

  defp titulo(valor), do: {:error, {:titulo_invalido, valor}}
end

defmodule Biblioteca.Livro do
  @moduledoc """
  Processo GenServer que representa um livro.
  Serializa todas as operações sobre o seu próprio estado,
  garantindo consistência sem locks explícitos.
  """
  use GenServer, restart: :transient

  alias Biblioteca.Eventos

  # ---------------------------------------------------------------------------
  # API cliente
  # ---------------------------------------------------------------------------

  def start_link(%{id: id} = dados) do
    GenServer.start_link(__MODULE__, dados, name: via(id))
  end

  @doc "Nome de registro do processo deste livro."
  def via(id), do: {:via, Registry, {Biblioteca.Registro, id}}

  def emprestar(id, membro_id), do: GenServer.call(via(id), {:emprestar, membro_id})
  def devolver(id, membro_id), do: GenServer.call(via(id), {:devolver, membro_id})
  def consultar(id), do: GenServer.call(via(id), :consultar)

  # ---------------------------------------------------------------------------
  # Callbacks do GenServer
  # ---------------------------------------------------------------------------

  @impl true
  def init(%{id: id, titulo: titulo}) do
    {:ok, %{id: id, titulo: titulo, emprestado_para: nil, emprestado_em: nil}}
  end

  @impl true
  def handle_call({:emprestar, membro_id}, _from, %{emprestado_para: nil} = estado) do
    agora = DateTime.utc_now()
    novo = %{estado | emprestado_para: membro_id, emprestado_em: agora}

    Eventos.publicar({:emprestado, estado.id, membro_id, agora})
    {:reply, :ok, novo}
  end

  def handle_call({:emprestar, _membro_id}, _from, estado) do
    {:reply, {:error, :indisponivel}, estado}
  end

  def handle_call({:devolver, membro_id}, _from, %{emprestado_para: membro_id} = estado) do
    agora = DateTime.utc_now()
    novo = %{estado | emprestado_para: nil, emprestado_em: nil}

    Eventos.publicar({:devolvido, estado.id, membro_id, agora})
    {:reply, :ok, novo}
  end

  def handle_call({:devolver, _membro_id}, _from, estado) do
    {:reply, {:error, :nao_emprestado_a_este_membro}, estado}
  end

  def handle_call(:consultar, _from, estado) do
    {:reply, {:ok, Map.take(estado, [:id, :titulo, :emprestado_para, :emprestado_em])}, estado}
  end
end

defmodule Biblioteca.Eventos do
  @moduledoc """
  Comunicação entre processos: publica eventos para todos os assinantes.
  Os assinantes recebem mensagens no formato `{:biblioteca, evento}`.
  """

  @registro Biblioteca.Eventos
  @chave :todos

  @doc "Inscreve o processo chamador para receber eventos."
  def assinar, do: Registry.register(@registro, @chave, nil)

  @doc "Cancela a inscrição do processo chamador."
  def cancelar, do: Registry.unregister(@registro, @chave)

  @doc "Envia um evento para todos os assinantes."
  def publicar(evento) do
    Registry.dispatch(@registro, @chave, fn entradas ->
      for {pid, _} <- entradas, do: send(pid, {:biblioteca, evento})
    end)

    :ok
  end
end

defmodule Biblioteca.Sistema do
  @moduledoc """
  Árvore de supervisão do sistema: registros, eventos e supervisor dinâmico de livros.
  """
  use Supervisor

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    children = [
      {Registry, keys: :unique, name: Biblioteca.Registro},
      {Registry, keys: :duplicate, name: Biblioteca.Eventos},
      {DynamicSupervisor, strategy: :one_for_one, name: Biblioteca.LivroSupervisor}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end

defmodule Biblioteca do
  @moduledoc """
  API pública do sistema de empréstimos de livros.

  Exemplo de uso:

      {:ok, _} = Biblioteca.iniciar()
      Biblioteca.assinar()
      {:ok, "L1"} = Biblioteca.cadastrar_livro(%{id: "L1", titulo: "Dom Casmurro"})
      :ok = Biblioteca.emprestar("L1", "M1")
      :ok = Biblioteca.devolver("L1", "M1")
  """

  alias Biblioteca.{Livro, Validador, Eventos}

  @doc "Inicia o sistema (supervisores, registros e processos base)."
  def iniciar, do: Biblioteca.Sistema.start_link()

  @doc "Assina eventos do sistema para o processo chamador."
  def assinar, do: Eventos.assinar()

  @doc "Cancela a assinatura de eventos do processo chamador."
  def cancelar_assinatura, do: Eventos.cancelar()

  @doc "Cadastra um novo livro, criando um processo independente para ele."
  def cadastrar_livro(attrs) do
    with {:ok, dados} <- Validador.livro(attrs),
         {:ok, _pid} <-
           DynamicSupervisor.start_child(Biblioteca.LivroSupervisor, {Livro, dados}) do
      {:ok, dados.id}
    else
      {:error, {:already_started, _pid}} -> {:error, :ja_cadastrado}
      {:error, motivo} -> {:error, motivo}
    end
  end

  @doc "Realiza o empréstimo de um livro para um membro."
  def emprestar(livro_id, membro_id) do
    with {:ok, livro_id} <- Validador.id(livro_id),
         {:ok, membro_id} <- Validador.id(membro_id),
         :ok <- garantir_existencia(livro_id) do
      executar(fn -> Livro.emprestar(livro_id, membro_id) end)
    end
  end

  @doc "Realiza a devolução de um livro por um membro."
  def devolver(livro_id, membro_id) do
    with {:ok, livro_id} <- Validador.id(livro_id),
         {:ok, membro_id} <- Validador.id(membro_id),
         :ok <- garantir_existencia(livro_id) do
      executar(fn -> Livro.devolver(livro_id, membro_id) end)
    end
  end

  @doc "Consulta o estado atual de um livro."
  def consultar(livro_id) do
    with {:ok, livro_id} <- Validador.id(livro_id),
         :ok <- garantir_existencia(livro_id) do
      executar(fn -> Livro.consultar(livro_id) end)
    end
  end

  # ---------------------------------------------------------------------------
  # Auxiliares privados
  # ---------------------------------------------------------------------------

  defp garantir_existencia(livro_id) do
    case Registry.lookup(Biblioteca.Registro, livro_id) do
      [_ | _] -> :ok
      [] -> {:error, :nao_encontrado}
    end
  end

  # Protege o chamador contra o processo do livro encerrar entre a checagem e a chamada.
  defp executar(fun) do
    fun.()
  catch
    :exit, _motivo -> {:error, :nao_encontrado}
  end
end
