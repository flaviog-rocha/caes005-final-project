# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de empréstimos de livros, atendendo somente aos seguintes requisitos:
#
# 1. Garantir a consistência dos dados durante operações concorrentes.
# 2. Permitir comunicação entre diferentes processos.
# 3. Permitir que diferentes entidades sejam processadas independentemente.
# 4. Validar os dados recebidos antes de realizar operações.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# Arquivo único: sistema de empréstimo de livros com processos OTP da biblioteca padrão.
# Executar: Biblioteca.start_link() e depois usar a API da módulo Biblioteca.

defmodule Biblioteca do
  @moduledoc """
  API pública do sistema de empréstimos.

  Cada livro e cada membro é um processo independente (GenServer) registrado
  no Registry e supervisionado por um DynamicSupervisor. Como cada entidade
  serializa suas próprias operações em uma única mensagem por vez, não há
  condições de corrida sobre o estado de um mesmo livro ou membro.
  """

  alias Biblioteca.{Validador, Entidade, Livro, Membro}

  # Inicia a árvore de supervisão do sistema.
  def start_link(_opts \\ []) do
    children = [
      {Registry, keys: :unique, name: Biblioteca.Registry},
      {Registry, keys: :duplicate, name: Biblioteca.PubSub},
      {DynamicSupervisor, strategy: :one_for_one, name: Biblioteca.EntidadesSup}
    ]

    Supervisor.start_link(children, strategy: :one_for_one, name: Biblioteca.Sup)
  end

  def child_spec(opts) do
    %{id: __MODULE__, start: {__MODULE__, :start_link, [opts]}, type: :supervisor}
  end

  # Cadastra um livro validando os atributos antes de criar o processo.
  def cadastrar_livro(attrs) do
    with {:ok, livro} <- Validador.livro(attrs) do
      Entidade.iniciar(Livro, livro)
    end
  end

  # Cadastra um membro validando os atributos antes de criar o processo.
  def cadastrar_membro(attrs) do
    with {:ok, membro} <- Validador.membro(attrs) do
      Entidade.iniciar(Membro, membro)
    end
  end

  def consultar_livro(livro_id) do
    with {:ok, livro_id} <- Validador.id(livro_id) do
      Livro.consultar(livro_id)
    end
  end

  # Realiza o empréstimo em duas etapas coordenadas:
  # 1. Reserva uma vaga no processo do membro (limite de empréstimos).
  # 2. Tenta emprestar no processo do livro.
  # Se a segunda etapa falhar, a reserva é desfeita (compensação).
  def emprestar(livro_id, membro_id) do
    with {:ok, livro_id} <- Validador.id(livro_id),
         {:ok, membro_id} <- Validador.id(membro_id),
         :ok <- Membro.reservar(membro_id) do
      case Livro.emprestar(livro_id, membro_id) do
        {:ok, emprestimo} ->
          {:ok, emprestimo}

        erro ->
          Membro.liberar(membro_id)
          erro
      end
    end
  end

  # Devolve o livro e libera a vaga do membro.
  def devolver(livro_id, membro_id) do
    with {:ok, livro_id} <- Validador.id(livro_id),
         {:ok, membro_id} <- Validador.id(membro_id),
         :ok <- Livro.devolver(livro_id, membro_id) do
      Membro.liberar(membro_id)
    end
  end

  # Inscreve o processo chamador para receber eventos do sistema.
  # Eventos chegam como {:biblioteca, evento}.
  def inscrever_eventos, do: Biblioteca.Eventos.inscrever()
end

defmodule Biblioteca.Validador do
  @moduledoc """
  Validações puras, executadas antes de qualquer alteração de estado.
  Atributos devem ser informados com chaves atômicas.
  """

  @isbn_regex ~r/^\d{13}$/
  @max_id 64

  def livro(%{} = attrs) do
    with {:ok, id} <- id(Map.get(attrs, :id)),
         {:ok, titulo} <- texto(Map.get(attrs, :titulo), 1, 200),
         {:ok, isbn} <- isbn(Map.get(attrs, :isbn)) do
      {:ok, %{id: id, titulo: titulo, isbn: isbn}}
    end
  end

  def livro(_), do: {:error, :atributos_invalidos}

  def membro(%{} = attrs) do
    with {:ok, id} <- id(Map.get(attrs, :id)),
         {:ok, nome} <- texto(Map.get(attrs, :nome), 1, 200) do
      {:ok, %{id: id, nome: nome}}
    end
  end

  def membro(_), do: {:error, :atributos_invalidos}

  def id(id) when is_binary(id) and byte_size(id) in 1..@max_id, do: {:ok, id}
  def id(_), do: {:error, :id_invalido}

  def texto(valor, min, max) when is_binary(valor) do
    limpo = String.trim(valor)
    tamanho = String.length(limpo)

    if tamanho >= min and tamanho <= max do
      {:ok, limpo}
    else
      {:error, :texto_invalido}
    end
  end

  def texto(_, _, _), do: {:error, :texto_invalido}

  def isbn(valor) when is_binary(valor) do
    if Regex.match?(@isbn_regex, valor), do: {:ok, valor}, else: {:error, :isbn_invalido}
  end

  def isbn(_), do: {:error, :isbn_invalido}
end

defmodule Biblioteca.Entidade do
  @moduledoc """
  Utilidades compartilhadas para criar, localizar e chamar processos de entidades.
  """

  @registry Biblioteca.Registry
  @supervisor Biblioteca.EntidadesSup

  def via(tipo, id), do: {:via, Registry, {@registry, {tipo, id}}}

  # Inicia um processo de entidade sob o supervisor dinâmico.
  def iniciar(modulo, dados) do
    case DynamicSupervisor.start_child(@supervisor, {modulo, dados}) do
      {:ok, _pid} -> {:ok, dados.id}
      {:error, {:already_started, _pid}} -> {:error, :ja_cadastrado}
      erro -> erro
    end
  end

  # Envia uma chamada síncrona ao processo da entidade, se existir.
  def chamar(tipo, id, mensagem) do
    case Registry.lookup(@registry, {tipo, id}) do
      [{pid, _}] ->
        try do
          GenServer.call(pid, mensagem)
        catch
          :exit, _ -> {:error, :indisponivel}
        end

      [] ->
        {:error, :nao_encontrado}
    end
  end
end

defmodule Biblioteca.Eventos do
  @moduledoc """
  Comunicação entre processos via Registry em modo duplicado (pub/sub local).
  """

  @pubsub Biblioteca.PubSub
  @topico :eventos

  def inscrever do
    Registry.register(@pubsub, @topico, nil)
  end

  def publicar(evento) do
    Registry.dispatch(@pubsub, @topico, fn entradas ->
      for {pid, _} <- entradas, do: send(pid, {:biblioteca, evento})
    end)

    :ok
  end
end

defmodule Biblioteca.Livro do
  @moduledoc """
  Processo de um livro. Garante que um livro só seja emprestado a um membro
  por vez, pois todas as mensagens são processadas sequencialmente.
  """
  use GenServer, restart: :transient

  alias Biblioteca.{Entidade, Eventos}

  def start_link(%{id: id} = dados) do
    GenServer.start_link(__MODULE__, dados, name: Entidade.via(:livro, id))
  end

  def emprestar(livro_id, membro_id),
    do: Entidade.chamar(:livro, livro_id, {:emprestar, membro_id})

  def devolver(livro_id, membro_id),
    do: Entidade.chamar(:livro, livro_id, {:devolver, membro_id})

  def consultar(livro_id),
    do: Entidade.chamar(:livro, livro_id, :consultar)

  @impl true
  def init(dados) do
    {:ok, %{dados: dados, status: :disponivel}}
  end

  @impl true
  def handle_call({:emprestar, membro_id}, _from, %{status: :disponivel} = estado) do
    emprestimo = %{livro_id: estado.dados.id, membro_id: membro_id, em: DateTime.utc_now()}
    Eventos.publicar({:emprestado, emprestimo})

    {:reply, {:ok, emprestimo}, %{estado | status: {:emprestado, membro_id, emprestimo.em}}}
  end

  def handle_call({:emprestar, _membro_id}, _from, estado) do
    {:reply, {:error, :indisponivel}, estado}
  end

  def handle_call({:devolver, membro_id}, _from, %{status: {:emprestado, membro_id, _}} = estado) do
    Eventos.publicar({:devolvido, estado.dados.id, membro_id})
    {:reply, :ok, %{estado | status: :disponivel}}
  end

  def handle_call({:devolver, _membro_id}, _from, estado) do
    {:reply, {:error, :nao_emprestado_ao_membro}, estado}
  end

  def handle_call(:consultar, _from, estado) do
    {:reply, {:ok, %{dados: estado.dados, status: estado.status}}, estado}
  end
end

defmodule Biblioteca.Membro do
  @moduledoc """
  Processo de um membro. Controla o limite de empréstimos simultâneos
  de forma atômica, pois cada reserva ocorre dentro do próprio processo.
  """
  use GenServer, restart: :transient

  alias Biblioteca.Entidade

  @limite 3

  def start_link(%{id: id} = dados) do
    GenServer.start_link(__MODULE__, dados, name: Entidade.via(:membro, id))
  end

  def reservar(membro_id), do: Entidade.chamar(:membro, membro_id, :reservar)
  def liberar(membro_id), do: Entidade.chamar(:membro, membro_id, :liberar)

  @impl true
  def init(dados) do
    {:ok, %{dados: dados, emprestimos: 0}}
  end

  @impl true
  def handle_call(:reservar, _from, %{emprestimos: n} = estado) when n < @limite do
    {:reply, :ok, %{estado | emprestimos: n + 1}}
  end

  def handle_call(:reservar, _from, estado) do
    {:reply, {:error, :limite_atingido}, estado}
  end

  def handle_call(:liberar, _from, %{emprestimos: n} = estado) do
    {:reply, :ok, %{estado | emprestimos: max(n - 1, 0)}}
  end
end
