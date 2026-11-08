# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o processamento de arquivos CSV, atendendo somente aos seguintes requisitos:
#
# 1. Persistir os dados para recuperação após reinicializações.
# 2. Manter um histórico das operações realizadas.
# 3. Processar grandes volumes de dados sem carregá-los integralmente na memória.
# 4. Permitir cadastrar, consultar, atualizar e remover registros.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule Armazem do
  @moduledoc """
  Armazenamento de registros CSV com persistência, histórico e processamento em streaming.

  Arquitetura:

    * Diário (journal) append-only em `journal.csv`. Cada linha é uma operação
      (INSERT, UPDATE ou DELETE) com sequência, timestamp e o registro completo.
      O diário é ao mesmo tempo a persistência e o histórico de operações.
    * Na inicialização, o diário é relido em streaming (linha a linha) para
      reconstruir um índice `id => {offset, tamanho}`. Apenas o índice fica em
      memória; os conteúdos dos registros são lidos do disco sob demanda via `pread`.
    * Escritas são serializadas por um GenServer e sincronizadas com `fsync`
      antes de responder ao cliente.
    * Importação e exportação de CSV são feitas por streaming, em lotes.

  Uso:

      {:ok, _} = Armazem.start_link(diretorio: "dados")
      {:ok, _} = Armazem.cadastrar(%{id: 1, nome: "Ana", email: "ana@x.com", idade: 30})
      {:ok, reg} = Armazem.consultar(1)
      {:ok, _} = Armazem.atualizar(1, %{idade: 31})
      :ok = Armazem.remover(1)
      {:ok, ops} = Armazem.historico(50)
      {:ok, totais} = Armazem.importar_csv("entrada.csv")
      :ok = Armazem.exportar_csv("saida.csv")
  """

  use GenServer

  @campos ~w(id nome email idade)
  @arquivo "journal.csv"
  @tamanho_lote 1_000

  # ---------------------------------------------------------------------------
  # API pública
  # ---------------------------------------------------------------------------

  @doc "Inicia o armazenamento. Opções: `diretorio:` (padrão \"dados\")."
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc "Cadastra um novo registro. Retorna erro se o id já existir."
  def cadastrar(registro), do: GenServer.call(__MODULE__, {:cadastrar, registro})

  @doc "Consulta um registro pelo id."
  def consultar(id), do: GenServer.call(__MODULE__, {:consultar, to_string(id)})

  @doc "Atualiza campos de um registro existente (o id não pode ser alterado)."
  def atualizar(id, mudancas) do
    GenServer.call(__MODULE__, {:atualizar, to_string(id), mudancas})
  end

  @doc "Remove um registro pelo id."
  def remover(id), do: GenServer.call(__MODULE__, {:remover, to_string(id)})

  @doc "Retorna as últimas `limite` operações registradas no histórico."
  def historico(limite \\ 100), do: GenServer.call(__MODULE__, {:historico, limite})

  @doc "Quantidade de registros ativos."
  def total, do: GenServer.call(__MODULE__, :total)

  @doc """
  Importa um CSV com cabeçalho contendo ao menos a coluna `id`.
  O arquivo é lido em streaming e enviado em lotes ao armazenamento.
  """
  def importar_csv(caminho) do
    case caminho |> File.stream!() |> Enum.take(1) do
      [] ->
        {:error, :arquivo_vazio}

      [linha_cabecalho] ->
        cabecalho = linha_cabecalho |> limpa_linha() |> csv_parse()

        if "id" in cabecalho do
          totais =
            caminho
            |> File.stream!()
            |> Stream.drop(1)
            |> Stream.reject(&(limpa_linha(&1) == ""))
            |> Stream.map(&linha_para_mapa(cabecalho, &1))
            |> Stream.chunk_every(@tamanho_lote)
            |> Enum.reduce(%{inseridos: 0, rejeitados: 0}, fn lote, acc ->
              {:ok, parcial} = GenServer.call(__MODULE__, {:lote, lote}, :infinity)

              %{
                inseridos: acc.inseridos + parcial.inseridos,
                rejeitados: acc.rejeitados + parcial.rejeitados
              }
            end)

          {:ok, totais}
        else
          {:error, :cabecalho_sem_id}
        end
    end
  end

  @doc "Exporta todos os registros ativos para um CSV, em streaming."
  def exportar_csv(caminho) do
    GenServer.call(__MODULE__, {:exportar, caminho}, :infinity)
  end

  # ---------------------------------------------------------------------------
  # Inicialização e recuperação
  # ---------------------------------------------------------------------------

  @impl true
  def init(opts) do
    dir = Keyword.get(opts, :diretorio, "dados")
    File.mkdir_p!(dir)
    caminho = Path.join(dir, @arquivo)
    File.touch!(caminho)

    # Recupera o estado relendo o diário em streaming.
    {indice, seq, valido} = replay(caminho)

    {:ok, fd} = :file.open(caminho, [:read, :append, :binary, :raw])

    # Remove uma eventual última linha incompleta (falha durante escrita).
    {:ok, _} = :file.position(fd, valido)
    :ok = :file.truncate(fd)

    {:ok, %{fd: fd, caminho: caminho, tamanho: valido, indice: indice, seq: seq}}
  end

  # Relê o diário linha a linha, construindo apenas o índice.
  # Retorna {indice, último_seq, offset_válido}.
  defp replay(caminho) do
    caminho
    |> File.stream!([], :line)
    |> Enum.reduce_while({%{}, 0, 0}, fn linha, {indice, seq, offset} ->
      if String.ends_with?(linha, "\n") do
        tamanho = byte_size(linha)

        case parse_journal(linha) do
          {:ok, %{seq: n, operacao: op, registro: reg}} ->
            indice = aplicar_no_indice(indice, op, reg["id"], {offset, tamanho})
            {:cont, {indice, max(seq, n), offset + tamanho}}

          :error ->
            IO.warn("Linha inválida ignorada no offset #{offset} do diário")
            {:cont, {indice, seq, offset + tamanho}}
        end
      else
        # Última linha incompleta: interrompe e marca o ponto válido.
        {:halt, {indice, seq, offset}}
      end
    end)
  end

  # ---------------------------------------------------------------------------
  # Callbacks do GenServer
  # ---------------------------------------------------------------------------

  @impl true
  def handle_call({:cadastrar, reg}, _from, st) do
    case inserir(st, reg) do
      {:ok, r, st} -> {:reply, {:ok, r}, sincronizar(st)}
      erro -> {:reply, erro, st}
    end
  end

  def handle_call({:lote, regs}, _from, st) do
    {st, contagem} =
      Enum.reduce(regs, {st, %{inseridos: 0, rejeitados: 0}}, fn reg, {s, c} ->
        case inserir(s, reg) do
          {:ok, _r, s} -> {s, %{c | inseridos: c.inseridos + 1}}
          _erro -> {s, %{c | rejeitados: c.rejeitados + 1}}
        end
      end)

    {:reply, {:ok, contagem}, sincronizar(st)}
  end

  def handle_call({:consultar, id}, _from, st) do
    {:reply, buscar(st, id), st}
  end

  def handle_call({:atualizar, id, mudancas}, _from, st) do
    with {:ok, atual} <- buscar(st, id),
         {:ok, r} <- normalizar(atual |> Map.merge(chaves_texto(mudancas)) |> Map.put("id", id)) do
      {:reply, {:ok, r}, st |> gravar("UPDATE", r) |> sincronizar()}
    else
      erro -> {:reply, erro, st}
    end
  end

  def handle_call({:remover, id}, _from, st) do
    case buscar(st, id) do
      # O DELETE grava o registro antigo, preservando o valor removido no histórico.
      {:ok, atual} -> {:reply, :ok, st |> gravar("DELETE", atual) |> sincronizar()}
      erro -> {:reply, erro, st}
    end
  end

  def handle_call({:historico, limite}, _from, st) do
    # Mantém apenas as últimas `limite` entradas em uma fila, sem carregar o diário inteiro.
    ultimas =
      st.caminho
      |> File.stream!()
      |> Stream.flat_map(&entrada_valida/1)
      |> Enum.reduce(:queue.new(), fn entrada, fila ->
        fila = :queue.in(entrada, fila)
        if :queue.len(fila) > limite, do: :queue.drop(fila), else: fila
      end)

    {:reply, {:ok, :queue.to_list(ultimas)}, st}
  end

  def handle_call(:total, _from, st) do
    {:reply, map_size(st.indice), st}
  end

  def handle_call({:exportar, caminho}, _from, st) do
    cabecalho = [csv_linha(@campos) <> "\n"]
    linhas = Stream.map(st.indice, fn {_id, loc} -> linha_csv(ler(st, loc)) end)

    :ok =
      cabecalho
      |> Stream.concat(linhas)
      |> Stream.into(File.stream!(caminho, [:write]))
      |> Stream.run()

    {:reply, :ok, st}
  end

  # ---------------------------------------------------------------------------
  # Operações internas de escrita e leitura
  # ---------------------------------------------------------------------------

  # Valida e insere um registro, sem sincronizar o disco.
  defp inserir(st, reg) do
    with {:ok, r} <- normalizar(reg),
         :ok <- nao_existe(st, r["id"]) do
      {:ok, r, gravar(st, "INSERT", r)}
    end
  end

  # Anexa uma operação ao diário e atualiza o índice em memória.
  defp gravar(st, op, registro) do
    seq = st.seq + 1
    valores = Enum.map(@campos, &Map.fetch!(registro, &1))
    ts = DateTime.utc_now() |> DateTime.to_iso8601()

    linha = csv_linha([Integer.to_string(seq), ts, op | valores]) <> "\n"
    :ok = :file.write(st.fd, linha)

    loc = {st.tamanho, byte_size(linha)}

    %{
      st
      | seq: seq,
        tamanho: st.tamanho + byte_size(linha),
        indice: aplicar_no_indice(st.indice, op, registro["id"], loc)
    }
  end

  # Garante que os dados estão em disco (durabilidade após reinicialização).
  defp sincronizar(st) do
    :ok = :file.sync(st.fd)
    st
  end

  defp aplicar_no_indice(indice, "DELETE", id, _loc), do: Map.delete(indice, id)
  defp aplicar_no_indice(indice, _op, id, loc), do: Map.put(indice, id, loc)

  defp nao_existe(st, id) do
    if Map.has_key?(st.indice, id), do: {:error, :ja_existe}, else: :ok
  end

  defp buscar(st, id) do
    case Map.fetch(st.indice, id) do
      {:ok, loc} -> {:ok, ler(st, loc)}
      :error -> {:error, :nao_encontrado}
    end
  end

  # Lê um registro do diário usando offset e tamanho do índice.
  defp ler(st, {offset, tamanho}) do
    {:ok, bin} = :file.pread(st.fd, offset, tamanho)
    {:ok, %{registro: reg}} = parse_journal(bin)
    reg
  end

  # ---------------------------------------------------------------------------
  # Validação e normalização de registros
  # ---------------------------------------------------------------------------

  # Converte chaves para string, mantém apenas os campos conhecidos
  # e preenche os ausentes com "". Rejeita id vazio e quebras de linha.
  defp normalizar(mapa) do
    texto = Map.new(mapa, fn {k, v} -> {to_string(k), to_string(v)} end)
    registro = Map.new(@campos, &{&1, Map.get(texto, &1, "")})

    cond do
      registro["id"] == "" ->
        {:error, :id_obrigatorio}

      Enum.any?(Map.values(registro), &String.contains?(&1, ["\n", "\r"])) ->
        {:error, :valor_invalido}

      true ->
        {:ok, registro}
    end
  end

  defp chaves_texto(mapa), do: Map.new(mapa, fn {k, v} -> {to_string(k), v} end)

  defp linha_para_mapa(cabecalho, linha) do
    linha
    |> limpa_linha()
    |> csv_parse()
    |> then(&(Enum.zip(cabecalho, &1) |> Map.new()))
  end

  defp linha_csv(registro) do
    csv_linha(Enum.map(@campos, &Map.fetch!(registro, &1))) <> "\n"
  end

  # ---------------------------------------------------------------------------
  # Formato do diário
  # Linha: seq,timestamp_iso8601,operacao,id,campo1,...,campoN
  # ---------------------------------------------------------------------------

  defp parse_journal(linha) do
    case csv_parse(limpa_linha(linha)) do
      [seq, ts, op | valores] when length(valores) == length(@campos) ->
        case Integer.parse(seq) do
          {n, ""} ->
            {:ok,
             %{
               seq: n,
               timestamp: ts,
               operacao: op,
               registro: Map.new(Enum.zip(@campos, valores))
             }}

          _ ->
            :error
        end

      _ ->
        :error
    end
  end

  defp entrada_valida(linha) do
    case parse_journal(linha) do
      {:ok, e} -> [%{seq: e.seq, timestamp: e.timestamp, operacao: e.operacao, id: e.registro["id"]}]
      :error -> []
    end
  end

  defp limpa_linha(linha) do
    linha
    |> String.trim_trailing("\n")
    |> String.trim_trailing("\r")
  end

  # ---------------------------------------------------------------------------
  # CSV (RFC 4180 simplificado)
  # ---------------------------------------------------------------------------

  defp csv_linha(campos), do: Enum.map_join(campos, ",", &csv_campo/1)

  defp csv_campo(valor) do
    if String.contains?(valor, [",", "\""]) do
      "\"" <> String.replace(valor, "\"", "\"\"") <> "\""
    else
      valor
    end
  end

  defp csv_parse(linha), do: csv_parse(linha, "", [], :normal)

  defp csv_parse(<<>>, campo, acc, _estado), do: Enum.reverse([campo | acc])

  # Dentro de aspas: "" representa uma aspa literal.
  defp csv_parse(<<?", ?", resto::binary>>, campo, acc, :aspas),
    do: csv_parse(resto, <<campo::binary, ?">>, acc, :aspas)

  # Aspa de fechamento.
  defp csv_parse(<<?", resto::binary>>, campo, acc, :aspas),
    do: csv_parse(resto, campo, acc, :normal)

  defp csv_parse(<<c, resto::binary>>, campo, acc, :aspas),
    do: csv_parse(resto, <<campo::binary, c>>, acc, :aspas)

  # Aspa de abertura no início do campo.
  defp csv_parse(<<?", resto::binary>>, "", acc, :normal),
    do: csv_parse(resto, "", acc, :aspas)

  # Separador de campo.
  defp csv_parse(<<?,, resto::binary>>, campo, acc, :normal),
    do: csv_parse(resto, "", [campo | acc], :normal)

  defp csv_parse(<<c, resto::binary>>, campo, acc, :normal),
    do: csv_parse(resto, <<campo::binary, c>>, acc, :normal)
end
