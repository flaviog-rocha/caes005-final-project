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
  Gerenciamento de estoque de uma loja com:

    * expiração automática de registros (varredura periódica e TTL por item);
    * validação de todos os dados antes de qualquer operação;
    * detecção de encerramentos inesperados de processos que possuem reservas
      (via monitores, sem links, para não derrubar o servidor);
    * persistência opcional em um adaptador de "banco fictício" (`Estoque.Storage`).

  Exemplo de uso:

      {:ok, _} = Estoque.start_link(armazenamento: {Estoque.Storage.Arquivo, "estoque.bin"})
      {:ok, "CAM-01"} = Estoque.cadastrar(%{sku: "CAM-01", nome: "Camiseta", quantidade: 10, preco: 59.9, ttl_ms: 60_000})
      {:ok, ref} = Estoque.reservar("CAM-01", 2)     # reserva vinculada ao processo chamador
      :ok = Estoque.liberar(ref)                     # devolve ao estoque

  Observação: a persistência opcional com Ecto.Migration pode ser feita com um
  adaptador próprio que implemente o behaviour `Estoque.Storage`. Um equivalente
  em migration seria algo como:

      # create table(:itens_estoque, primary_key: false) do
      #   add :sku, :string, primary_key: true
      #   add :nome, :string, null: false
      #   add :preco, :decimal, null: false
      #   add :quantidade, :integer, null: false
      #   add :reservado, :integer, null: false, default: 0
      #   add :expira_em, :bigint, null: false
      # end
      # create constraint(:itens_estoque, :quantidade_nao_negativa, check: "quantidade >= 0")
  """

  use GenServer
  require Logger

  alias Estoque.Validador

  @intervalo_varredura_padrao_ms 1_000

  ## API pública

  def start_link(opts \\ []) do
    {nome, opts} = Keyword.pop(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: nome)
  end

  def cadastrar(srv \\ __MODULE__, atributos), do: GenServer.call(srv, {:cadastrar, atributos})

  def consultar(srv \\ __MODULE__, sku), do: GenServer.call(srv, {:consultar, sku})

  def ajustar(srv \\ __MODULE__, sku, delta), do: GenServer.call(srv, {:ajustar, sku, delta})

  def reservar(srv \\ __MODULE__, sku, quantidade),
    do: GenServer.call(srv, {:reservar, sku, quantidade, self()})

  def liberar(srv \\ __MODULE__, referencia), do: GenServer.call(srv, {:liberar, referencia})

  def listar(srv \\ __MODULE__), do: GenServer.call(srv, :listar)

  ## Callbacks do GenServer

  @impl true
  def init(opts) do
    intervalo = Keyword.get(opts, :intervalo_varredura_ms, @intervalo_varredura_padrao_ms)
    armazenamento = Keyword.get(opts, :armazenamento)

    case carregar_itens(armazenamento) do
      {:ok, itens} ->
        Process.send_after(self(), :varrer, intervalo)

        estado = %{
          itens: recuperar_reservas_orfas(itens),
          reservas: %{},
          armazenamento: armazenamento,
          intervalo: intervalo
        }

        {:ok, estado}

      {:error, motivo} ->
        {:stop, {:falha_ao_carregar, motivo}}
    end
  end

  @impl true
  def handle_call({:cadastrar, atributos}, _from, estado) do
    with {:ok, dados} <- Validador.item(atributos),
         :ok <- verificar_inexistente(estado.itens, dados.sku) do
      registro = %{
        sku: dados.sku,
        nome: dados.nome,
        preco: dados.preco,
        quantidade: dados.quantidade,
        reservado: 0,
        expira_em: agora() + dados.ttl_ms
      }

      estado = atualizar(estado, registro)
      {:reply, {:ok, dados.sku}, estado}
    else
      erro -> {:reply, erro, estado}
    end
  end

  def handle_call({:consultar, sku}, _from, estado) do
    resposta =
      with {:ok, sku} <- Validador.sku(sku),
           {:ok, registro} <- buscar_ativo(estado.itens, sku) do
        {:ok, visao(registro)}
      end

    {:reply, resposta, estado}
  end

  def handle_call({:ajustar, sku, delta}, _from, estado) do
    with {:ok, sku, delta} <- Validador.ajuste(sku, delta),
         {:ok, registro} <- buscar_ativo(estado.itens, sku),
         nova = registro.quantidade + delta,
         :ok <- garantir(nova >= 0, :estoque_insuficiente) do
      estado = atualizar(estado, %{registro | quantidade: nova})
      {:reply, {:ok, nova}, estado}
    else
      erro -> {:reply, erro, estado}
    end
  end

  def handle_call({:reservar, sku, quantidade, pid}, _from, estado) do
    with {:ok, sku, quantidade} <- Validador.reserva(sku, quantidade, pid),
         {:ok, registro} <- buscar_ativo(estado.itens, sku),
         :ok <- garantir(registro.quantidade >= quantidade, :estoque_insuficiente) do
      # O monitor é a própria referência da reserva: se o processo cair,
      # recebemos {:DOWN, ref, ...} e devolvemos a quantidade ao estoque.
      referencia = Process.monitor(pid)

      atualizado = %{
        registro
        | quantidade: registro.quantidade - quantidade,
          reservado: registro.reservado + quantidade
      }

      estado = atualizar(estado, atualizado)
      estado = %{estado | reservas: Map.put(estado.reservas, referencia, {sku, quantidade})}
      {:reply, {:ok, referencia}, estado}
    else
      erro -> {:reply, erro, estado}
    end
  end

  def handle_call({:liberar, referencia}, _from, estado) do
    case Map.pop(estado.reservas, referencia) do
      {nil, _} ->
        {:reply, {:error, :reserva_inexistente}, estado}

      {{sku, quantidade}, reservas} ->
        Process.demonitor(referencia, [:flush])
        estado = devolver(%{estado | reservas: reservas}, sku, quantidade)
        {:reply, :ok, estado}
    end
  end

  def handle_call(:listar, _from, estado) do
    itens =
      estado.itens
      |> Map.values()
      |> Enum.filter(&ativo?/1)
      |> Enum.map(&visao/1)

    {:reply, {:ok, itens}, estado}
  end

  @impl true
  def handle_info({:DOWN, referencia, :process, pid, motivo}, estado) do
    case Map.pop(estado.reservas, referencia) do
      {nil, _} ->
        {:noreply, estado}

      {{sku, quantidade}, reservas} ->
        if esperado?(motivo) do
          Logger.info("Processo #{inspect(pid)} encerrou normalmente; reserva de #{quantidade}x #{sku} liberada")
        else
          Logger.warning(
            "Processo #{inspect(pid)} encerrou inesperadamente (#{inspect(motivo)}); " <>
              "reserva de #{quantidade}x #{sku} liberada"
          )
        end

        {:noreply, devolver(%{estado | reservas: reservas}, sku, quantidade)}
    end
  end

  def handle_info(:varrer, estado) do
    agora = agora()

    {expirados, mantidos} =
      Enum.split_with(estado.itens, fn {_sku, r} -> r.expira_em <= agora end)

    Enum.each(expirados, fn {sku, _} ->
      Logger.info("Registro expirado removido do estoque: #{sku}")
    end)

    Process.send_after(self(), :varrer, estado.intervalo)

    estado =
      if expirados == [] do
        estado
      else
        novo = %{estado | itens: Map.new(mantidos)}
        persistir(novo)
      end

    {:noreply, estado}
  end

  def handle_info(mensagem, estado) do
    Logger.debug("Mensagem inesperada ignorada: #{inspect(mensagem)}")
    {:noreply, estado}
  end

  ## Funções privadas

  defp carregar_itens(nil), do: {:ok, %{}}
  defp carregar_itens({modulo, args}), do: modulo.carregar(args)

  # Reservas não são persistidas; ao reiniciar, o que estava reservado volta ao estoque.
  defp recuperar_reservas_orfas(itens) do
    Map.new(itens, fn {sku, r} ->
      {sku, %{r | quantidade: r.quantidade + r.reservado, reservado: 0}}
    end)
  end

  defp verificar_inexistente(itens, sku) do
    case buscar_ativo(itens, sku) do
      {:error, _} -> :ok
      {:ok, _} -> {:error, :ja_cadastrado}
    end
  end

  defp buscar_ativo(itens, sku) do
    case Map.fetch(itens, sku) do
      {:ok, registro} ->
        if ativo?(registro), do: {:ok, registro}, else: {:error, :expirado}

      :error ->
        {:error, :nao_encontrado}
    end
  end

  defp devolver(estado, sku, quantidade) do
    case Map.fetch(estado.itens, sku) do
      {:ok, registro} ->
        atualizado = %{
          registro
          | quantidade: registro.quantidade + quantidade,
            reservado: max(registro.reservado - quantidade, 0)
        }

        atualizar(estado, atualizado)

      :error ->
        Logger.warning("Não foi possível devolver #{quantidade}x #{sku}: item não existe mais")
        estado
    end
  end

  defp atualizar(estado, registro) do
    novo = %{estado | itens: Map.put(estado.itens, registro.sku, registro)}
    persistir(novo)
  end

  defp persistir(%{armazenamento: nil} = estado), do: estado

  defp persistir(%{armazenamento: {modulo, args}, itens: itens} = estado) do
    case modulo.salvar(itens, args) do
      :ok ->
        estado

      {:error, motivo} ->
        Logger.error("Falha ao persistir estoque: #{inspect(motivo)}")
        estado
    end
  end

  defp garantir(true, _motivo), do: :ok
  defp garantir(false, motivo), do: {:error, motivo}

  defp ativo?(%{expira_em: expira_em}), do: expira_em > agora()

  defp esperado?(:normal), do: true
  defp esperado?(:shutdown), do: true
  defp esperado?({:shutdown, _}), do: true
  defp esperado?(_), do: false

  defp visao(registro) do
    Map.take(registro, [:sku, :nome, :preco, :quantidade, :reservado, :expira_em])
  end

  defp agora, do: System.system_time(:millisecond)
end

defmodule Estoque.Validador do
  @moduledoc false

  @ttl_padrao_ms 30 * 24 * 60 * 60 * 1000
  @ttl_maximo_ms 365 * 24 * 60 * 60 * 1000

  def item(atributos) when is_map(atributos) do
    with {:ok, sku} <- sku(Map.get(atributos, :sku)),
         {:ok, nome} <- texto(Map.get(atributos, :nome), :nome, 200),
         {:ok, quantidade} <- quantidade_inicial(Map.get(atributos, :quantidade)),
         {:ok, preco} <- preco(Map.get(atributos, :preco)),
         {:ok, ttl_ms} <- ttl(Map.get(atributos, :ttl_ms, @ttl_padrao_ms)) do
      {:ok, %{sku: sku, nome: nome, quantidade: quantidade, preco: preco, ttl_ms: ttl_ms}}
    end
  end

  def item(_), do: {:error, {:atributos, :esperado_mapa}}

  def sku(valor) when is_binary(valor) do
    if Regex.match?(~r/\A[A-Za-z0-9_-]{1,64}\z/, valor) do
      {:ok, valor}
    else
      {:error, {:sku, :formato_invalido}}
    end
  end

  def sku(_), do: {:error, {:sku, :tipo_invalido}}

  def ajuste(sku, delta) when is_integer(delta) and delta != 0 do
    with {:ok, sku} <- sku(sku), do: {:ok, sku, delta}
  end

  def ajuste(_sku, _delta), do: {:error, {:delta, :deve_ser_inteiro_diferente_de_zero}}

  def reserva(sku, quantidade, pid) when is_pid(pid) do
    with {:ok, sku} <- sku(sku),
         {:ok, quantidade} <- quantidade(quantidade) do
      {:ok, sku, quantidade}
    end
  end

  def reserva(_sku, _quantidade, _pid), do: {:error, {:pid, :invalido}}

  def quantidade(q) when is_integer(q) and q > 0, do: {:ok, q}
  def quantidade(_), do: {:error, {:quantidade, :deve_ser_inteiro_positivo}}

  defp quantidade_inicial(q) when is_integer(q) and q >= 0, do: {:ok, q}
  defp quantidade_inicial(_), do: {:error, {:quantidade, :deve_ser_inteiro_nao_negativo}}

  defp preco(p) when is_number(p) and p > 0, do: {:ok, p}
  defp preco(_), do: {:error, {:preco, :invalido}}

  defp ttl(t) when is_integer(t) and t > 0 and t <= @ttl_maximo_ms, do: {:ok, t}
  defp ttl(_), do: {:error, {:ttl_ms, :fora_do_intervalo_permitido}}

  defp texto(valor, campo, maximo) when is_binary(valor) do
    limpo = String.trim(valor)

    if limpo != "" and String.length(limpo) <= maximo do
      {:ok, limpo}
    else
      {:error, {campo, :tamanho_invalido}}
    end
  end

  defp texto(_valor, campo, _maximo), do: {:error, {campo, :tipo_invalido}}
end

defmodule Estoque.Storage do
  @moduledoc """
  Behaviour para o "banco fictício". Qualquer adaptador (arquivo, ETS, Ecto, etc.)
  pode implementar estes callbacks.
  """

  @callback carregar(args :: term()) :: {:ok, map()} | {:error, term()}
  @callback salvar(itens :: map(), args :: term()) :: :ok | {:error, term()}
end

defmodule Estoque.Storage.Arquivo do
  @moduledoc """
  Adaptador de persistência em arquivo binário (term_to_binary), com escrita
  atômica via arquivo temporário.
  """

  @behaviour Estoque.Storage

  @impl true
  def carregar(caminho) do
    case File.read(caminho) do
      {:ok, bin} ->
        try do
          {:ok, :erlang.binary_to_term(bin, [:safe])}
        rescue
          ArgumentError -> {:error, :dados_corrompidos}
        end

      {:error, :enoent} ->
        {:ok, %{}}

      {:error, motivo} ->
        {:error, motivo}
    end
  end

  @impl true
  def salvar(itens, caminho) do
    tmp = caminho <> ".tmp"

    with :ok <- File.write(tmp, :erlang.term_to_binary(itens)) do
      File.rename(tmp, caminho)
    end
  end
end
