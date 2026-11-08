# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de empréstimos de livros, atendendo somente aos seguintes requisitos:
#
# 1. Diferenciar erros de validação de falhas operacionais.
# 2. Executar determinadas operações periodicamente.
# 3. Persistir os dados para recuperação após reinicializações.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule Biblioteca.Emprestimos do
  use GenServer
  require Logger

  @segundos_por_dia 86_400
  @intervalo_padrao_ms 60_000

  def start_link(opcoes \\ []) do
    nome = Keyword.get(opcoes, :name, __MODULE__)

    case nome do
      nil -> GenServer.start_link(__MODULE__, opcoes)
      _ -> GenServer.start_link(__MODULE__, opcoes, name: nome)
    end
  end

  def cadastrar_livro(titulo, quantidade),
    do: cadastrar_livro(__MODULE__, titulo, quantidade)

  def cadastrar_livro(servidor, titulo, quantidade),
    do: chamar(servidor, {:cadastrar_livro, titulo, quantidade})

  def emprestar(livro_id, leitor, dias),
    do: emprestar(__MODULE__, livro_id, leitor, dias)

  def emprestar(servidor, livro_id, leitor, dias),
    do: chamar(servidor, {:emprestar, livro_id, leitor, dias})

  def devolver(emprestimo_id), do: devolver(__MODULE__, emprestimo_id)
  def devolver(servidor, emprestimo_id), do: chamar(servidor, {:devolver, emprestimo_id})

  def listar, do: listar(__MODULE__)
  def listar(servidor), do: chamar(servidor, :listar)

  @impl true
  def init(opcoes) do
    arquivo = Keyword.get(opcoes, :arquivo, "biblioteca_dados.term")
    intervalo_ms = Keyword.get(opcoes, :intervalo_ms, @intervalo_padrao_ms)

    cond do
      not is_binary(arquivo) or arquivo == "" ->
        {:stop, {:validacao, :arquivo_invalido}}

      not is_integer(intervalo_ms) or intervalo_ms <= 0 ->
        {:stop, {:validacao, :intervalo_invalido}}

      true ->
        case carregar(arquivo) do
          {:ok, dados} ->
            Process.send_after(self(), :verificar_prazos, intervalo_ms)

            {:ok,
             %{
               arquivo: arquivo,
               intervalo_ms: intervalo_ms,
               dados: dados
             }}

          {:error, motivo} ->
            {:stop, {:operacional, motivo}}
        end
    end
  end

  @impl true
  def handle_call({:cadastrar_livro, titulo, quantidade}, _de, estado) do
    cond do
      not is_binary(titulo) or String.trim(titulo) == "" ->
        {:reply, erro_validacao(:titulo_invalido), estado}

      not is_integer(quantidade) or quantidade <= 0 ->
        {:reply, erro_validacao(:quantidade_invalida), estado}

      true ->
        dados = estado.dados
        id = dados.proximo_livro

        livro = %{
          id: id,
          titulo: String.trim(titulo),
          quantidade: quantidade
        }

        novos_dados = %{
          dados
          | livros: Map.put(dados.livros, id, livro),
            proximo_livro: id + 1
        }

        salvar_resposta(estado, novos_dados, {:ok, livro})
    end
  end

  def handle_call({:emprestar, livro_id, leitor, dias}, _de, estado) do
    cond do
      not is_integer(livro_id) or livro_id <= 0 ->
        {:reply, erro_validacao(:id_livro_invalido), estado}

      not is_binary(leitor) or String.trim(leitor) == "" ->
        {:reply, erro_validacao(:leitor_invalido), estado}

      not is_integer(dias) or dias <= 0 ->
        {:reply, erro_validacao(:prazo_invalido), estado}

      not Map.has_key?(estado.dados.livros, livro_id) ->
        {:reply, erro_validacao(:livro_inexistente), estado}

      disponiveis(estado.dados, livro_id) <= 0 ->
        {:reply, erro_validacao(:livro_indisponivel), estado}

      true ->
        dados = estado.dados
        id = dados.proximo_emprestimo
        agora = System.system_time(:second)

        emprestimo = %{
          id: id,
          livro_id: livro_id,
          leitor: String.trim(leitor),
          emprestado_em: agora,
          vence_em: agora + dias * @segundos_por_dia,
          devolvido_em: nil,
          status: :ativo
        }

        novos_dados = %{
          dados
          | emprestimos: Map.put(dados.emprestimos, id, emprestimo),
            proximo_emprestimo: id + 1
        }

        salvar_resposta(estado, novos_dados, {:ok, emprestimo})
    end
  end

  def handle_call({:devolver, emprestimo_id}, _de, estado) do
    emprestimo = Map.get(estado.dados.emprestimos, emprestimo_id)

    cond do
      not is_integer(emprestimo_id) or emprestimo_id <= 0 ->
        {:reply, erro_validacao(:id_emprestimo_invalido), estado}

      is_nil(emprestimo) ->
        {:reply, erro_validacao(:emprestimo_inexistente), estado}

      emprestimo.status == :devolvido ->
        {:reply, erro_validacao(:emprestimo_ja_devolvido), estado}

      true ->
        atualizado = %{
          emprestimo
          | status: :devolvido,
            devolvido_em: System.system_time(:second)
        }

        novos_dados = %{
          estado.dados
          | emprestimos: Map.put(estado.dados.emprestimos, emprestimo_id, atualizado)
        }

        salvar_resposta(estado, novos_dados, {:ok, atualizado})
    end
  end

  def handle_call(:listar, _de, estado) do
    livros =
      Map.new(estado.dados.livros, fn {id, livro} ->
        {id, Map.put(livro, :disponiveis, disponiveis(estado.dados, id))}
      end)

    {:reply,
     {:ok,
      %{
        livros: livros,
        emprestimos: estado.dados.emprestimos
      }}, estado}
  end

  @impl true
  def handle_info(:verificar_prazos, estado) do
    novos_dados = marcar_atrasados(estado.dados, System.system_time(:second))

    case persistir(estado.arquivo, novos_dados) do
      :ok ->
        Process.send_after(self(), :verificar_prazos, estado.intervalo_ms)
        {:noreply, %{estado | dados: novos_dados}}

      {:error, motivo} ->
        Logger.error("Falha operacional ao persistir dados periódicos: #{inspect(motivo)}")
        Process.send_after(self(), :verificar_prazos, estado.intervalo_ms)
        {:noreply, estado}
    end
  end

  defp chamar(servidor, mensagem) do
    try do
      GenServer.call(servidor, mensagem)
    catch
      :exit, motivo -> {:error, {:operacional, {:genserver, motivo}}}
    end
  end

  defp erro_validacao(motivo), do: {:error, {:validacao, motivo}}

  defp salvar_resposta(estado, novos_dados, resposta) do
    case persistir(estado.arquivo, novos_dados) do
      :ok ->
        {:reply, resposta, %{estado | dados: novos_dados}}

      {:error, motivo} ->
        {:reply, {:error, {:operacional, motivo}}, estado}
    end
  end

  defp disponiveis(dados, livro_id) do
    em_aberto =
      Enum.count(dados.emprestimos, fn {_id, emprestimo} ->
        emprestimo.livro_id == livro_id and emprestimo.status in [:ativo, :atrasado]
      end)

    dados.livros[livro_id].quantidade - em_aberto
  end

  defp marcar_atrasados(dados, agora) do
    emprestimos =
      Map.new(dados.emprestimos, fn {id, emprestimo} ->
        atualizado =
          if emprestimo.status == :ativo and emprestimo.vence_em <= agora do
            %{emprestimo | status: :atrasado}
          else
            emprestimo
          end

        {id, atualizado}
      end)

    %{dados | emprestimos: emprestimos}
  end

  defp carregar(arquivo) do
    case File.read(arquivo) do
      {:ok, binario} ->
        try do
          dados = :erlang.binary_to_term(binario, [:safe])

          if dados_validos?(dados) do
            {:ok, dados}
          else
            {:error, :formato_de_dados_invalido}
          end
        rescue
          _ -> {:error, :arquivo_corrompido}
        catch
          _, _ -> {:error, :arquivo_corrompido}
        end

      {:error, :enoent} ->
        {:ok,
         %{
           livros: %{},
           emprestimos: %{},
           proximo_livro: 1,
           proximo_emprestimo: 1
         }}

      {:error, motivo} ->
        {:error, {:leitura, motivo}}
    end
  end

  defp dados_validos?(dados) when is_map(dados) do
    is_map(dados[:livros]) and
      is_map(dados[:emprestimos]) and
      is_integer(dados[:proximo_livro]) and dados[:proximo_livro] > 0 and
      is_integer(dados[:proximo_emprestimo]) and dados[:proximo_emprestimo] > 0 and
      Enum.all?(dados.livros, fn {id, livro} ->
        is_integer(id) and id > 0 and is_map(livro) and
          livro[:id] == id and is_binary(livro[:titulo]) and
          is_integer(livro[:quantidade]) and livro[:quantidade] > 0
      end) and
      Enum.all?(dados.emprestimos, fn {id, emprestimo} ->
        is_integer(id) and id > 0 and is_map(emprestimo) and
          emprestimo[:id] == id and is_integer(emprestimo[:livro_id]) and
          is_binary(emprestimo[:leitor]) and is_integer(emprestimo[:emprestado_em]) and
          is_integer(emprestimo[:vence_em]) and
          (is_nil(emprestimo[:devolvido_em]) or is_integer(emprestimo[:devolvido_em])) and
          emprestimo[:status] in [:ativo, :atrasado, :devolvido]
      end)
  rescue
    _ -> false
  end

  defp dados_validos?(_), do: false

  defp persistir(arquivo, dados) do
    diretorio = Path.dirname(arquivo)
    temporario = arquivo <> ".tmp." <> Integer.to_string(System.unique_integer([:positive]))
    binario = :erlang.term_to_binary(dados)

    with :ok <- File.mkdir_p(diretorio),
         :ok <- File.write(temporario, binario, [:binary]),
         :ok <- File.rename(temporario, arquivo) do
      :ok
    else
      {:error, motivo} ->
        File.rm(temporario)
        {:error, {:gravacao, motivo}}
    end
  end
end
