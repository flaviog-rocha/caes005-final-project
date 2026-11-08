# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de estoque de uma loja, atendendo somente aos seguintes requisitos:
#
# 1. Implementar expiração automática de registros.
# 2. Validar os dados recebidos antes de realizar operações.
# 3. Detectar encerramentos inesperados de processos.
# 4. Quando fizer sentido para o domínio, considerar a comunicação com um banco de dados fictício e, opcionalmente, o uso de Ecto.Migration, sem tornar isso obrigatório.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule Estoque do
  @moduledoc """
  Gerenciamento de estoque de uma loja, com:

    * expiração automática de registros (TTL por produto), via limpeza periódica
      e verificação preguiçosa na leitura;
    * validação de todos os dados de entrada antes de qualquer operação;
    * reservas vinculadas a processos, monitoradas com `Process.monitor/1`, para
      detectar encerramentos inesperados e liberar o estoque reservado;
    * persistência através do behaviour `Estoque.Repositorio`, com um banco fictício
      em memória (`Estoque.Repositorio.BancoFake`).

  Não há dependência de Ecto. Para usar um banco real, implemente `Estoque.Repositorio`
  com Ecto.Repo e crie uma migration equivalente a:

      create table(:produtos, primary_key: false) do
        add :sku, :string, primary_key: true
        add :nome, :string, null: false
        add :preco_centavos, :integer, null: false
        add :quantidade, :integer, null: false
        add :expira_em, :bigint, null: false
      end
  """
end

defmodule Estoque.Produto do
  @moduledoc "Estrutura de produto e validações puras."

  @enforce_keys [:sku, :nome, :preco_centavos, :quantidade, :expira_em]
  defstruct [:sku, :nome, :preco_centavos, :quantidade, :expira_em]

  @type t :: %__MODULE__{
          sku: String.t(),
          nome: String.t(),
          preco_centavos: pos_integer(),
          quantidade: non_neg_integer(),
          expira_em: integer()
        }

  @regex_sku ~r/\A[A-Z0-9-]{3,20}\z/
  # Limite de 1 ano para o TTL, em milissegundos.
  @ttl_maximo_ms 365 * 24 * 60 * 60 * 1_000

  @doc "Valida os atributos de um produto (sem TTL)."
  def validar_atributos(attrs) when is_map(attrs) do
    with {:ok, sku} <- validar_sku(Map.get(attrs, :sku)),
         {:ok, nome} <- validar_nome(Map.get(attrs, :nome)),
         {:ok, preco} <- validar_inteiro(Map.get(attrs, :preco_centavos), :preco_centavos, 1),
         {:ok, qtd} <- validar_inteiro(Map.get(attrs, :quantidade), :quantidade, 0) do
      {:ok, %{sku: sku, nome: nome, preco_centavos: preco, quantidade: qtd}}
    end
  end

  def validar_atributos(_), do: {:error, {:atributos, :tipo_invalido}}

  @doc "Valida um SKU (3 a 20 caracteres: letras maiúsculas, dígitos e hífen)."
  def validar_sku(sku) when is_binary(sku) do
    if Regex.match?(@regex_sku, sku),
      do: {:ok, sku},
      else: {:error, {:sku, :formato_invalido}}
  end

  def validar_sku(_), do: {:error, {:sku, :tipo_invalido}}

  @doc "Valida o TTL em milissegundos."
  def validar_ttl(ttl) do
    with {:ok, ttl} <- validar_inteiro(ttl, :ttl_ms, 1) do
      if ttl <= @ttl_maximo_ms,
        do: {:ok, ttl},
        else: {:error, {:ttl_ms, :acima_do_maximo}}
    end
  end

  @doc "Valida um inteiro com valor mínimo informado."
  def validar_inteiro(valor, campo, minimo) when is_integer(valor) do
    if valor >= minimo,
      do: {:ok, valor},
      else: {:error, {campo, :abaixo_do_minimo}}
  end

  def validar_inteiro(_valor, campo, _minimo), do: {:error, {campo, :tipo_invalido}}

  defp validar_nome(nome) when is_binary(nome) do
    nome = String.trim(nome)

    cond do
      nome == "" -> {:error, {:nome, :vazio}}
      String.length(nome) > 120 -> {:error, {:nome, :muito_longo}}
      true -> {:ok, nome}
    end
  end

  defp validar_nome(_), do: {:error, {:nome, :tipo_invalido}}
end

defmodule Estoque.Repositorio do
  @moduledoc """
  Contrato de persistência. Qualquer adaptador (memória, Ecto, arquivo etc.)
  pode implementar estas funções. Todas recebem o estado do repositório como
  primeiro argumento e o devolvem atualizado, mantendo o núcleo funcional.
  """

  @type estado :: term()
  @type chave :: String.t()

  @callback inicializar() :: estado()
  @callback salvar(estado(), chave(), term()) :: estado()
  @callback buscar(estado(), chave()) :: {:ok, term()} | {:error, :nao_encontrado}
  @callback remover(estado(), chave()) :: estado()
  @callback listar(estado()) :: [{chave(), term()}]
end

defmodule Estoque.Repositorio.BancoFake do
  @moduledoc "Banco fictício em memória, usando um mapa puro como armazenamento."
  @behaviour Estoque.Repositorio

  @impl true
  def inicializar, do: %{}

  @impl true
  def salvar(estado, chave, valor), do: Map.put(estado, chave, valor)

  @impl true
  def buscar(estado, chave) do
    case Map.fetch(estado, chave) do
      {:ok, valor} -> {:ok, valor}
      :error -> {:error, :nao_encontrado}
    end
  end

  @impl true
  def remover(estado, chave), do: Map.delete(estado, chave)

  @impl true
  def listar(estado), do: Map.to_list(estado)
end

defmodule Estoque.Gerenciador do
  @moduledoc """
  Processo (GenServer) responsável pelo estoque.

  Responsabilidades:
    * cadastrar produtos com TTL e removê-los automaticamente ao expirar;
    * baixar estoque diretamente ou por meio de reservas;
    * reservas pertencem ao processo que as criou. Se esse processo encerrar
      (normal ou inesperadamente) sem confirmar a reserva, o estoque é liberado.
  """

  use GenServer
  require Logger

  alias Estoque.Produto

  @intervalo_limpeza_padrao 1_000

  # ---------------------------------------------------------------------------
  # API pública
  # ---------------------------------------------------------------------------

  @doc "Inicia o gerenciador. Opções: `:nome`, `:repo`, `:intervalo_limpeza_ms`."
  def start_link(opts \\ []) do
    {nome, opts} = Keyword.pop(opts, :nome, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: nome)
  end

  @doc "Cadastra (ou substitui) um produto com TTL em milissegundos."
  def cadastrar(srv \\ __MODULE__, attrs, ttl_ms),
    do: GenServer.call(srv, {:cadastrar, attrs, ttl_ms})

  @doc "Consulta um produto ativo (não expirado)."
  def consultar(srv \\ __MODULE__, sku), do: GenServer.call(srv, {:consultar, sku})

  @doc "Baixa a quantidade de um produto, respeitando as reservas existentes."
  def baixar(srv \\ __MODULE__, sku, qtd), do: GenServer.call(srv, {:baixar, sku, qtd})

  @doc "Reserva quantidade para o processo chamador."
  def reservar(srv \\ __MODULE__, sku, qtd), do: GenServer.call(srv, {:reservar, sku, qtd})

  @doc "Confirma a reserva do processo chamador, efetivando a baixa."
  def confirmar_reserva(srv \\ __MODULE__, sku), do: GenServer.call(srv, {:confirmar, sku})

  @doc "Cancela a reserva do processo chamador, devolvendo o estoque."
  def cancelar_reserva(srv \\ __MODULE__, sku), do: GenServer.call(srv, {:cancelar, sku})

  # ---------------------------------------------------------------------------
  # Callbacks do GenServer
  # ---------------------------------------------------------------------------

  @impl true
  def init(opts) do
    intervalo = Keyword.get(opts, :intervalo_limpeza_ms, @intervalo_limpeza_padrao)
    repo = Keyword.get(opts, :repo, Estoque.Repositorio.BancoFake)

    if is_integer(intervalo) and intervalo > 0 do
      estado = %{
        repo: repo,
        dados: repo.inicializar(),
        # %{pid => {monitor_ref, %{sku => quantidade_reservada}}}
        reservas: %{},
        intervalo: intervalo
      }

      agendar_limpeza(intervalo)
      {:ok, estado}
    else
      {:stop, {:configuracao_invalida, :intervalo_limpeza_ms}}
    end
  end

  @impl true
  def handle_call({:cadastrar, attrs, ttl}, _from, st) do
    resultado =
      with {:ok, ttl} <- Produto.validar_ttl(ttl),
           {:ok, campos} <- Produto.validar_atributos(attrs) do
        produto = struct!(Produto, Map.put(campos, :expira_em, agora() + ttl))
        dados = st.repo.salvar(st.dados, produto.sku, produto)
        {:ok, produto, %{st | dados: dados}}
      end

    responder(resultado, st)
  end

  def handle_call({:consultar, sku}, _from, st) do
    responder(obter_ativo(st, sku) |> envelopar_sem_estado(st), st)
  end

  def handle_call({:baixar, sku, qtd}, _from, st) do
    resultado =
      with {:ok, qtd} <- Produto.validar_inteiro(qtd, :quantidade, 1),
           {:ok, p} <- obter_ativo(st, sku),
           :ok <- verificar_disponibilidade(st, p, qtd),
           {:ok, p2, st2} <- atualizar_quantidade(st, p, p.quantidade - qtd) do
        {:ok, p2, st2}
      end

    responder(resultado, st)
  end

  def handle_call({:reservar, sku, qtd}, {pid, _}, st) do
    resultado =
      with {:ok, qtd} <- Produto.validar_inteiro(qtd, :quantidade, 1),
           {:ok, p} <- obter_ativo(st, sku),
           :ok <- verificar_reserva_inexistente(st, pid, p.sku),
           :ok <- verificar_disponibilidade(st, p, qtd) do
        {:ok, qtd, registrar_reserva(st, pid, p.sku, qtd)}
      end

    responder(resultado, st)
  end

  def handle_call({:confirmar, sku}, {pid, _}, st) do
    resultado =
      with {:ok, p} <- obter_ativo(st, sku),
           {:ok, qtd, st1} <- liberar_reserva(st, pid, p.sku),
           {:ok, p2, st2} <- atualizar_quantidade(st1, p, p.quantidade - qtd) do
        {:ok, p2.quantidade, st2}
      end

    responder(resultado, st)
  end

  def handle_call({:cancelar, sku}, {pid, _}, st) do
    resultado =
      with {:ok, sku} <- Produto.validar_sku(sku),
           {:ok, _qtd, st1} <- liberar_reserva(st, pid, sku) do
        {:ok, :cancelada, st1}
      end

    responder(resultado, st)
  end

  @impl true
  def handle_info(:limpar_expirados, st) do
    agora = agora()

    expirados =
      st.repo.listar(st.dados)
      |> Enum.filter(fn {_sku, produto} -> expirado?(produto, agora) end)

    dados =
      Enum.reduce(expirados, st.dados, fn {sku, _}, acc -> st.repo.remover(acc, sku) end)

    if expirados != [],
      do: Logger.info("#{length(expirados)} registro(s) expirado(s) removido(s)")

    agendar_limpeza(st.intervalo)
    {:noreply, %{st | dados: dados}}
  end

  # Encerramento de um processo que possui reservas.
  def handle_info({:DOWN, _ref, :process, pid, motivo}, st) do
    case Map.pop(st.reservas, pid) do
      {nil, _} ->
        {:noreply, st}

      {{_ref, itens}, reservas} ->
        if motivo == :normal do
          Logger.info(
            "Processo #{inspect(pid)} encerrou sem confirmar reservas; " <>
              "liberando #{inspect(Map.keys(itens))}"
          )
        else
          Logger.warning(
            "Processo #{inspect(pid)} encerrou inesperadamente (#{inspect(motivo)}); " <>
              "liberando reservas #{inspect(itens)}"
          )
        end

        {:noreply, %{st | reservas: reservas}}
    end
  end

  def handle_info(_msg, st), do: {:noreply, st}

  # ---------------------------------------------------------------------------
  # Funções auxiliares (puras ou que apenas manipulam o estado)
  # ---------------------------------------------------------------------------

  defp responder({:ok, resposta, st2}, _st), do: {:reply, {:ok, resposta}, st2}
  defp responder({:error, _} = erro, st), do: {:reply, erro, st}

  # Adapta o retorno de obter_ativo/2 ao formato de responder/2.
  defp envelopar_sem_estado({:ok, p}, st), do: {:ok, p, st}
  defp envelopar_sem_estado({:error, _} = erro, _st), do: erro

  defp obter_ativo(st, sku) do
    with {:ok, sku} <- Produto.validar_sku(sku),
         {:ok, produto} <- st.repo.buscar(st.dados, sku),
         false <- expirado?(produto, agora()) do
      {:ok, produto}
    else
      true -> {:error, {:registro, :expirado}}
      {:error, _} = erro -> erro
    end
  end

  defp expirado?(%Produto{expira_em: expira_em}, agora), do: agora >= expira_em

  defp reservado(st, sku) do
    st.reservas
    |> Map.values()
    |> Enum.reduce(0, fn {_ref, itens}, acc -> acc + Map.get(itens, sku, 0) end)
  end

  defp verificar_disponibilidade(st, produto, qtd) do
    disponivel = produto.quantidade - reservado(st, produto.sku)

    if qtd <= disponivel,
      do: :ok,
      else: {:error, {:estoque, :insuficiente}}
  end

  defp verificar_reserva_inexistente(st, pid, sku) do
    case Map.fetch(st.reservas, pid) do
      {:ok, {_ref, itens}} when is_map_key(itens, sku) -> {:error, {:reserva, :ja_existente}}
      _ -> :ok
    end
  end

  defp registrar_reserva(st, pid, sku, qtd) do
    {ref, itens} =
      case Map.fetch(st.reservas, pid) do
        {:ok, {ref, itens}} -> {ref, itens}
        :error -> {Process.monitor(pid), %{}}
      end

    %{st | reservas: Map.put(st.reservas, pid, {ref, Map.put(itens, sku, qtd)})}
  end

  defp liberar_reserva(st, pid, sku) do
    case Map.fetch(st.reservas, pid) do
      {:ok, {ref, itens}} when is_map_key(itens, sku) ->
        {qtd, itens_restantes} = Map.pop(itens, sku)

        reservas =
          if map_size(itens_restantes) == 0 do
            Process.demonitor(ref, [:flush])
            Map.delete(st.reservas, pid)
          else
            Map.put(st.reservas, pid, {ref, itens_restantes})
          end

        {:ok, qtd, %{st | reservas: reservas}}

      _ ->
        {:error, {:reserva, :inexistente}}
    end
  end

  defp atualizar_quantidade(_st, _produto, nova) when nova < 0,
    do: {:error, {:quantidade, :insuficiente}}

  defp atualizar_quantidade(st, produto, nova) do
    produto2 = %{produto | quantidade: nova}
    dados = st.repo.salvar(st.dados, produto2.sku, produto2)
    {:ok, produto2, %{st | dados: dados}}
  end

  defp agendar_limpeza(intervalo), do: Process.send_after(self(), :limpar_expirados, intervalo)

  defp agora, do: System.os_time(:millisecond)
end
