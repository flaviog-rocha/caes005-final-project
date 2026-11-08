# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de empréstimos de livros, atendendo somente aos seguintes requisitos:
#
# 1. Validar os dados recebidos antes de realizar operações.
# 2. Diferenciar erros de validação de falhas operacionais.
# 3. Executar determinadas operações periodicamente.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - Agent
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# Arquivo único: gerenciamento de empréstimos de livros com Agent.
#
# Separação de erros:
#   {:error, {:validacao, motivos}}  -> dados de entrada inválidos (nada é executado)
#   {:error, {:operacional, motivo}} -> falha durante a operação (regra de negócio,
#                                       estado inconsistente ou agente indisponível)

defmodule Biblioteca.Emprestimos do
  @moduledoc """
  API de empréstimos de livros. O estado é mantido em um Agent com a forma:

      %{
        acervo: %{livro_id => %{titulo: String.t(), emprestado: boolean}},
        emprestimos: %{emprestimo_id => %{livro_id, usuario, devolucao_prevista, devolvido_em}},
        relatorios: [%{data: Date.t(), atrasos: [emprestimo_id]}]
      }
  """

  # Prazo máximo permitido para um empréstimo, em dias.
  @prazo_maximo_dias 30

  # ---------------------------------------------------------------------------
  # Inicialização
  # ---------------------------------------------------------------------------

  @doc "Inicia o Agent que guarda o estado da biblioteca. Aceita opções do Agent (ex.: `name:`)."
  def start_link(opts \\ []) do
    Agent.start_link(fn -> %{acervo: %{}, emprestimos: %{}, relatorios: []} end, opts)
  end

  # ---------------------------------------------------------------------------
  # Operações públicas
  # ---------------------------------------------------------------------------

  @doc "Cadastra um livro. Espera `%{id: String.t(), titulo: String.t()}`."
  def cadastrar_livro(agent, attrs) do
    # Primeiro valida a entrada; só então toca no estado do Agent.
    with {:ok, livro} <- validar_livro(attrs) do
      executar(agent, fn estado ->
        if Map.has_key?(estado.acervo, livro.id) do
          {{:error, {:operacional, :livro_ja_cadastrado}}, estado}
        else
          acervo = Map.put(estado.acervo, livro.id, %{titulo: livro.titulo, emprestado: false})
          {{:ok, livro.id}, %{estado | acervo: acervo}}
        end
      end)
    end
  end

  @doc "Realiza um empréstimo. Espera `%{livro_id, usuario, dias}`."
  def emprestar(agent, attrs) do
    with {:ok, dados} <- validar_emprestimo(attrs) do
      executar(agent, fn estado ->
        with {:ok, livro} <- buscar(estado.acervo, dados.livro_id, :livro_nao_encontrado),
             :ok <- verificar_disponivel(livro) do
          id = gerar_id()

          emprestimo = %{
            livro_id: dados.livro_id,
            usuario: dados.usuario,
            devolucao_prevista: Date.add(Date.utc_today(), dados.dias),
            devolvido_em: nil
          }

          novo_estado = %{
            estado
            | acervo: Map.put(estado.acervo, dados.livro_id, %{livro | emprestado: true}),
              emprestimos: Map.put(estado.emprestimos, id, emprestimo)
          }

          {{:ok, id}, novo_estado}
        else
          {:error, motivo} -> {{:error, {:operacional, motivo}}, estado}
        end
      end)
    end
  end

  @doc "Registra a devolução de um empréstimo pelo seu id."
  def devolver(agent, emprestimo_id) do
    with :ok <- validar_identificador(emprestimo_id, :emprestimo_id) do
      executar(agent, fn estado ->
        with {:ok, emp} <- buscar(estado.emprestimos, emprestimo_id, :emprestimo_nao_encontrado),
             :ok <- verificar_pendente(emp),
             {:ok, livro} <- buscar(estado.acervo, emp.livro_id, :livro_inconsistente) do
          emp_devolvido = %{emp | devolvido_em: Date.utc_today()}

          novo_estado = %{
            estado
            | emprestimos: Map.put(estado.emprestimos, emprestimo_id, emp_devolvido),
              acervo: Map.put(estado.acervo, emp.livro_id, %{livro | emprestado: false})
          }

          {{:ok, emprestimo_id}, novo_estado}
        else
          {:error, motivo} -> {{:error, {:operacional, motivo}}, estado}
        end
      end)
    end
  end

  @doc "Lista os ids de empréstimos em aberto cuja devolução prevista já passou."
  def listar_atrasados(agent, data \\ Date.utc_today()) do
    with :ok <- validar_data(data) do
      executar(agent, fn estado ->
        {{:ok, atrasados_em(estado, data)}, estado}
      end)
    end
  end

  @doc """
  Tarefa usada pelo agendador: calcula os atrasos de hoje e armazena
  um relatório no próprio Agent.
  """
  def registrar_relatorio_atrasos(agent) do
    executar(agent, fn estado ->
      hoje = Date.utc_today()
      relatorio = %{data: hoje, atrasos: atrasados_em(estado, hoje)}

      # Mantém apenas os 100 relatórios mais recentes para limitar o crescimento do estado.
      relatorios = Enum.take([relatorio | estado.relatorios], 100)

      {{:ok, relatorio}, %{estado | relatorios: relatorios}}
    end)
  end

  # ---------------------------------------------------------------------------
  # Execução segura sobre o Agent
  # ---------------------------------------------------------------------------

  # Converte falhas do processo (agente inexistente, morto ou que falhou durante
  # a função) em erro operacional, sem derrubar o chamador.
  defp executar(agent, fun) do
    Agent.get_and_update(agent, fun)
  catch
    :exit, motivo -> {:error, {:operacional, {:agente_indisponivel, motivo}}}
  end

  # ---------------------------------------------------------------------------
  # Regras de negócio (puras)
  # ---------------------------------------------------------------------------

  defp atrasados_em(estado, data) do
    for {id, emp} <- estado.emprestimos,
        is_nil(emp.devolvido_em),
        Date.compare(emp.devolucao_prevista, data) == :lt,
        do: id
  end

  defp verificar_disponivel(%{emprestado: true}), do: {:error, :livro_indisponivel}
  defp verificar_disponivel(%{emprestado: false}), do: :ok

  defp verificar_pendente(%{devolvido_em: nil}), do: :ok
  defp verificar_pendente(_), do: {:error, :emprestimo_ja_devolvido}

  defp buscar(mapa, chave, motivo) do
    case Map.fetch(mapa, chave) do
      {:ok, valor} -> {:ok, valor}
      :error -> {:error, motivo}
    end
  end

  defp gerar_id, do: Integer.to_string(System.unique_integer([:positive]))

  # ---------------------------------------------------------------------------
  # Validação de entrada (puras, sem efeitos colaterais)
  # ---------------------------------------------------------------------------

  defp validar_livro(attrs) when is_map(attrs) do
    erros =
      []
      |> campo_texto(attrs, :id)
      |> campo_texto(attrs, :titulo)

    concluir_validacao(erros, %{id: attrs[:id], titulo: attrs[:titulo]})
  end

  defp validar_livro(_), do: erro_entrada()

  defp validar_emprestimo(attrs) when is_map(attrs) do
    erros =
      []
      |> campo_texto(attrs, :livro_id)
      |> campo_texto(attrs, :usuario)
      |> campo_dias(attrs)

    concluir_validacao(erros, %{
      livro_id: attrs[:livro_id],
      usuario: attrs[:usuario],
      dias: attrs[:dias]
    })
  end

  defp validar_emprestimo(_), do: erro_entrada()

  defp validar_identificador(valor, campo) do
    if is_binary(valor) and String.trim(valor) != "" do
      :ok
    else
      {:error, {:validacao, [{campo, :invalido}]}}
    end
  end

  defp validar_data(data) do
    if is_struct(data, Date) do
      :ok
    else
      {:error, {:validacao, [{:data, :tipo_invalido}]}}
    end
  end

  defp campo_texto(erros, attrs, campo) do
    case Map.fetch(attrs, campo) do
      :error ->
        [{campo, :ausente} | erros]

      {:ok, valor} when is_binary(valor) ->
        if String.trim(valor) == "", do: [{campo, :vazio} | erros], else: erros

      {:ok, _} ->
        [{campo, :tipo_invalido} | erros]
    end
  end

  defp campo_dias(erros, attrs) do
    case Map.fetch(attrs, :dias) do
      :error ->
        [{:dias, :ausente} | erros]

      {:ok, dias} when is_integer(dias) and dias >= 1 and dias <= @prazo_maximo_dias ->
        erros

      {:ok, dias} when is_integer(dias) ->
        [{:dias, :fora_do_intervalo} | erros]

      {:ok, _} ->
        [{:dias, :tipo_invalido} | erros]
    end
  end

  defp concluir_validacao([], dados), do: {:ok, dados}
  defp concluir_validacao(erros, _dados), do: {:error, {:validacao, Enum.reverse(erros)}}

  defp erro_entrada, do: {:error, {:validacao, [{:entrada, :nao_e_mapa}]}}
end

defmodule Biblioteca.Agendador do
  @moduledoc """
  Executa periodicamente tarefas sobre o Agent da biblioteca,
  usando o módulo :timer da biblioteca padrão do Erlang/OTP.
  """

  @doc """
  Agenda a verificação de atrasos a cada `intervalo_ms` milissegundos.
  Retorna `{:ok, tref}` ou um erro de validação.
  """
  def iniciar(agent, intervalo_ms) when is_integer(intervalo_ms) and intervalo_ms > 0 do
    :timer.apply_interval(intervalo_ms, __MODULE__, :executar_tarefa, [agent])
  end

  def iniciar(_agent, _intervalo_ms) do
    {:error, {:validacao, [{:intervalo_ms, :invalido}]}}
  end

  @doc "Cancela um agendamento retornado por `iniciar/2`."
  def parar(tref) do
    case :timer.cancel(tref) do
      {:ok, :cancel} -> :ok
      {:error, motivo} -> {:error, {:operacional, motivo}}
    end
  end

  @doc false
  def executar_tarefa(agent) do
    # Erros de execução são tratados aqui para não interromper agendamentos futuros.
    case Biblioteca.Emprestimos.registrar_relatorio_atrasos(agent) do
      {:ok, %{data: data, atrasos: atrasos}} ->
        IO.puts("[#{Date.to_iso8601(data)}] #{length(atrasos)} empréstimo(s) em atraso")

      {:error, motivo} ->
        IO.puts(:stderr, "Falha operacional na verificação periódica: #{inspect(motivo)}")
    end
  end
end

# Exemplo de uso (comentado):
#
# {:ok, biblioteca} = Biblioteca.Emprestimos.start_link()
# {:ok, _} = Biblioteca.Emprestimos.cadastrar_livro(biblioteca, %{id: "L1", titulo: "Dom Casmurro"})
# {:ok, emp_id} = Biblioteca.Emprestimos.emprestar(biblioteca, %{livro_id: "L1", usuario: "Ana", dias: 7})
# {:ok, _} = Biblioteca.Emprestimos.devolver(biblioteca, emp_id)
# {:ok, tref} = Biblioteca.Agendador.iniciar(biblioteca, 60_000)
# Biblioteca.Agendador.parar(tref)
