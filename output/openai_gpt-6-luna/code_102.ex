# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de empréstimos de livros, atendendo somente aos seguintes requisitos:
#
# 1. Permitir comunicação entre diferentes processos.
# 2. Executar determinadas operações periodicamente.
# 3. Manter um histórico das operações realizadas.
# 4. Permitir cadastrar, consultar, atualizar e remover registros.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - DETS
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule BibliotecaEmprestimos do
  use GenServer

  @tipos_validos [:livro, :emprestimo]

  def start_link(opts \\ []) do
    nome = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: nome)
  end

  def cadastrar(tipo, registro), do: cadastrar(__MODULE__, tipo, registro)

  def cadastrar(servidor, tipo, registro) do
    GenServer.call(servidor, {:cadastrar, tipo, registro})
  end

  def consultar(tipo, id), do: consultar(__MODULE__, tipo, id)

  def consultar(servidor, tipo, id) do
    GenServer.call(servidor, {:consultar, tipo, id})
  end

  def atualizar(tipo, id, alteracoes), do: atualizar(__MODULE__, tipo, id, alteracoes)

  def atualizar(servidor, tipo, id, alteracoes) do
    GenServer.call(servidor, {:atualizar, tipo, id, alteracoes})
  end

  def remover(tipo, id), do: remover(__MODULE__, tipo, id)

  def remover(servidor, tipo, id) do
    GenServer.call(servidor, {:remover, tipo, id})
  end

  def historico, do: historico(__MODULE__)

  def historico(servidor) do
    GenServer.call(servidor, :historico)
  end

  @impl true
  def init(opts) do
    caminho = opts |> Keyword.get(:arquivo, "emprestimos.dets") |> Path.expand()
    intervalo_ms = Keyword.get(opts, :intervalo_ms, 60_000)
    File.mkdir_p!(Path.dirname(caminho))

    sufixo = System.unique_integer([:positive])
    tabela_dados = String.to_atom("biblioteca_dados_#{sufixo}")
    tabela_historico = String.to_atom("biblioteca_historico_#{sufixo}")

    with {:ok, _} <-
           :dets.open_file(tabela_dados,
             file: String.to_charlist(caminho),
             type: :set
           ),
         {:ok, _} <-
           :dets.open_file(tabela_historico,
             file: String.to_charlist(caminho <> ".historico"),
             type: :set
           ) do
      temporizador = Process.send_after(self(), :verificar_atrasos, intervalo_ms)

      {:ok,
       %{
         dados: tabela_dados,
         historico: tabela_historico,
         intervalo_ms: intervalo_ms,
         temporizador: temporizador
       }}
    else
      {:error, motivo} -> {:stop, motivo}
    end
  end

  @impl true
  def handle_call({:cadastrar, tipo, registro}, _from, estado) do
    resultado = cadastrar_registro(estado.dados, tipo, registro)
    registrar(estado.historico, :cadastrar, tipo, registro_id(registro), resultado)
    {:reply, resultado, estado}
  end

  def handle_call({:consultar, tipo, id}, _from, estado) do
    resultado =
      if tipo_valido?(tipo) do
        case :dets.lookup(estado.dados, {tipo, id}) do
          [{{^tipo, ^id}, registro}] -> {:ok, registro}
          [] -> {:error, :nao_encontrado}
        end
      else
        {:error, :tipo_invalido}
      end

    registrar(estado.historico, :consultar, tipo, id, resultado)
    {:reply, resultado, estado}
  end

  def handle_call({:atualizar, tipo, id, alteracoes}, _from, estado) do
    resultado = atualizar_registro(estado.dados, tipo, id, alteracoes)
    registrar(estado.historico, :atualizar, tipo, id, resultado)
    {:reply, resultado, estado}
  end

  def handle_call({:remover, tipo, id}, _from, estado) do
    resultado = remover_registro(estado.dados, tipo, id)
    registrar(estado.historico, :remover, tipo, id, resultado)
    {:reply, resultado, estado}
  end

  def handle_call(:historico, _from, estado) do
    eventos =
      :dets.foldl(
        fn
          {sequencia, evento}, acumulador when is_integer(sequencia) ->
            [evento | acumulador]

          _, acumulador ->
            acumulador
        end,
        [],
        estado.historico
      )
      |> Enum.reverse()

    {:reply, eventos, estado}
  end

  @impl true
  def handle_info(:verificar_atrasos, estado) do
    verificar_emprestimos_atrasados(estado)
    temporizador = Process.send_after(self(), :verificar_atrasos, estado.intervalo_ms)
    {:noreply, %{estado | temporizador: temporizador}}
  end

  @impl true
  def terminate(_motivo, estado) do
    Process.cancel_timer(estado.temporizador)
    :dets.close(estado.dados)
    :dets.close(estado.historico)
    :ok
  end

  defp cadastrar_registro(_tabela, tipo, _registro) when tipo not in @tipos_validos do
    {:error, :tipo_invalido}
  end

  defp cadastrar_registro(tabela, tipo, registro) when is_map(registro) do
    case Map.fetch(registro, :id) do
      {:ok, id} ->
        chave = {tipo, id}

        case :dets.lookup(tabela, chave) do
          [] ->
            :dets.insert(tabela, {chave, registro})
            :dets.sync(tabela)
            {:ok, registro}

          _ ->
            {:error, :ja_existe}
        end

      :error ->
        {:error, :id_obrigatorio}
    end
  end

  defp cadastrar_registro(_tabela, _tipo, _registro), do: {:error, :registro_invalido}

  defp atualizar_registro(_tabela, tipo, _id, _alteracoes) when tipo not in @tipos_validos do
    {:error, :tipo_invalido}
  end

  defp atualizar_registro(tabela, tipo, id, alteracoes) when is_map(alteracoes) do
    chave = {tipo, id}

    case :dets.lookup(tabela, chave) do
      [{^chave, registro}] ->
        atualizado = Map.merge(registro, Map.delete(alteracoes, :id)) |> Map.put(:id, id)
        :dets.insert(tabela, {chave, atualizado})
        :dets.sync(tabela)
        {:ok, atualizado}

      [] ->
        {:error, :nao_encontrado}
    end
  end

  defp atualizar_registro(_tabela, _tipo, _id, _alteracoes), do: {:error, :alteracoes_invalidas}

  defp remover_registro(_tabela, tipo, _id) when tipo not in @tipos_validos do
    {:error, :tipo_invalido}
  end

  defp remover_registro(tabela, tipo, id) do
    chave = {tipo, id}

    case :dets.lookup(tabela, chave) do
      [{^chave, registro}] ->
        :dets.delete(tabela, chave)
        :dets.sync(tabela)
        {:ok, registro}

      [] ->
        {:error, :nao_encontrado}
    end
  end

  defp verificar_emprestimos_atrasados(estado) do
    hoje = Date.utc_today()

    atrasados =
      :dets.foldl(
        fn
          {{:emprestimo, id}, registro}, acumulador ->
            if emprestimo_atrasado?(registro, hoje) do
              [{id, registro} | acumulador]
            else
              acumulador
            end

          _, acumulador ->
            acumulador
        end,
        [],
        estado.dados
      )

    Enum.each(atrasados, fn {id, registro} ->
      atualizado = Map.put(registro, :status, :atrasado)
      :dets.insert(estado.dados, {{:emprestimo, id}, atualizado})
      registrar(estado.historico, :marcar_atrasado, :emprestimo, id, {:ok, atualizado})
    end)

    if atrasados != [], do: :dets.sync(estado.dados)
  end

  defp emprestimo_atrasado?(registro, hoje) do
    status = Map.get(registro, :status)
    data_devolucao = Map.get(registro, :data_devolucao)

    status not in [:atrasado, :devolvido] and
      case converter_data(data_devolucao) do
        %Date{} = data -> Date.compare(data, hoje) == :lt
        _ -> false
      end
  end

  defp converter_data(%Date{} = data), do: data

  defp converter_data(data) when is_binary(data) do
    case Date.from_iso8601(data) do
      {:ok, data} -> data
      _ -> nil
    end
  end

  defp converter_data(_), do: nil

  defp registrar(tabela, operacao, tipo, id, resultado) do
    sequencia =
      case :dets.lookup(tabela, :sequencia) do
        [{:sequencia, atual}] -> atual + 1
        [] -> 1
      end

    evento = %{
      sequencia: sequencia,
      operacao: operacao,
      tipo: tipo,
      id: id,
      resultado: resultado,
      data_hora: DateTime.utc_now()
    }

    :dets.insert(tabela, [{:sequencia, sequencia}, {sequencia, evento}])
    :dets.sync(tabela)
    :ok
  end

  defp tipo_valido?(tipo), do: tipo in @tipos_validos

  defp registro_id(registro) when is_map(registro), do: Map.get(registro, :id)
  defp registro_id(_), do: nil
end
