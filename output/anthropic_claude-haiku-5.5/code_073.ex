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

# biblioteca.exs
# Execute com: elixir biblioteca.exs
#
# Gerenciamento de empréstimos de livros usando Agent (biblioteca padrão).
#
# Formato de erros:
#   {:error, {:validacao, [mensagens]}}  -> dados de entrada inválidos, detectados ANTES de qualquer operação
#   {:error, {:operacional, motivo}}     -> falhas durante a execução (regra de negócio ou infraestrutura)

defmodule Biblioteca do
  @moduledoc """
  Gerencia livros e empréstimos mantendo o estado em um `Agent`.
  Valida todas as entradas antes de tocar no estado e separa erros de
  validação de falhas operacionais. Verificações de atraso são executadas
  periodicamente via `:timer.apply_interval/4`.
  """

  @nome __MODULE__
  @timeout 5_000
  @limite_dias 90

  # ---------------------------------------------------------------------------
  # Inicialização
  # ---------------------------------------------------------------------------

  @doc "Inicia o Agent que guarda o estado da biblioteca."
  def iniciar do
    Agent.start_link(
      fn -> %{livros: %{}, emprestimos: %{}, proximo_id: 1} end,
      name: @nome
    )
  end

  # ---------------------------------------------------------------------------
  # API pública
  # ---------------------------------------------------------------------------

  @doc "Cadastra um livro. Espera um mapa com a chave `:titulo`."
  def cadastrar_livro(attrs) when is_map(attrs) do
    with {:ok, [titulo]} <- validar([validar_texto(Map.get(attrs, :titulo), "título")]) do
      atualizar(fn estado ->
        id = estado.proximo_id
        livro = %{id: id, titulo: titulo, disponivel: true}

        novo_estado = %{
          estado
          | livros: Map.put(estado.livros, id, livro),
            proximo_id: id + 1
        }

        {{:ok, livro}, novo_estado}
      end)
    end
  end

  def cadastrar_livro(_attrs), do: {:error, {:validacao, ["attrs deve ser um mapa"]}}

  @doc "Realiza o empréstimo de um livro por um número de dias."
  def emprestar(livro_id, emprestador, dias) do
    with {:ok, [id, nome, d]} <-
           validar([
             validar_inteiro_positivo(livro_id, "id do livro"),
             validar_texto(emprestador, "nome do emprestador"),
             validar_dias(dias)
           ]) do
      atualizar(fn estado ->
        case Map.fetch(estado.livros, id) do
          :error ->
            {{:error, {:operacional, :livro_nao_encontrado}}, estado}

          {:ok, %{disponivel: false}} ->
            {{:error, {:operacional, :livro_indisponivel}}, estado}

          {:ok, livro} ->
            emp_id = estado.proximo_id
            hoje = Date.utc_today()

            emprestimo = %{
              id: emp_id,
              livro_id: id,
              emprestador: nome,
              inicio: hoje,
              devolucao_prevista: Date.add(hoje, d),
              status: :ativo
            }

            novo_estado = %{
              estado
              | livros: Map.put(estado.livros, id, %{livro | disponivel: false}),
                emprestimos: Map.put(estado.emprestimos, emp_id, emprestimo),
                proximo_id: emp_id + 1
            }

            {{:ok, emprestimo}, novo_estado}
        end
      end)
    end
  end

  @doc "Registra a devolução de um empréstimo."
  def devolver(emprestimo_id) do
    with {:ok, [id]} <- validar([validar_inteiro_positivo(emprestimo_id, "id do empréstimo")]) do
      atualizar(fn estado ->
        case Map.fetch(estado.emprestimos, id) do
          :error ->
            {{:error, {:operacional, :emprestimo_nao_encontrado}}, estado}

          {:ok, %{status: :devolvido}} ->
            {{:error, {:operacional, :ja_devolvido}}, estado}

          {:ok, emprestimo} ->
            livro = Map.fetch!(estado.livros, emprestimo.livro_id)

            novo_estado = %{
              estado
              | emprestimos:
                  Map.put(estado.emprestimos, id, %{emprestimo | status: :devolvido}),
                livros:
                  Map.put(estado.livros, livro.id, %{livro | disponivel: true})
            }

            {{:ok, %{emprestimo | status: :devolvido}}, novo_estado}
        end
      end)
    end
  end

  @doc "Lista os empréstimos atualmente em atraso."
  def listar_atrasados do
    atualizar(fn estado ->
      atrasados = for {_id, %{status: :atrasado} = e} <- estado.emprestimos, do: e
      {{:ok, atrasados}, estado}
    end)
  end

  @doc """
  Marca como `:atrasado` os empréstimos ativos cuja data prevista já passou.
  Retorna a quantidade de empréstimos recém-marcados.
  É executada periodicamente pelo agendador.
  """
  def verificar_atrasos do
    atualizar(fn estado ->
      hoje = Date.utc_today()

      novos =
        Enum.count(estado.emprestimos, fn {_id, e} ->
          e.status == :ativo and Date.compare(e.devolucao_prevista, hoje) == :lt
        end)

      emprestimos =
        Map.new(estado.emprestimos, fn {id, e} ->
          if e.status == :ativo and Date.compare(e.devolucao_prevista, hoje) == :lt do
            {id, %{e | status: :atrasado}}
          else
            {id, e}
          end
        end)

      {{:ok, novos}, %{estado | emprestimos: emprestimos}}
    end)
  end

  @doc """
  Agenda `verificar_atrasos/0` para rodar a cada `intervalo_ms` milissegundos.
  Retorna `{:ok, tref}`; use `:timer.cancel(tref)` para interromper.
  """
  def agendar_verificacao_atrasos(intervalo_ms) do
    with {:ok, [ms]} <- validar([validar_inteiro_positivo(intervalo_ms, "intervalo (ms)")]) do
      case :timer.apply_interval(ms, __MODULE__, :verificar_atrasos, []) do
        {:ok, tref} -> {:ok, tref}
        {:error, motivo} -> {:error, {:operacional, motivo}}
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Camada de validação (funções puras, sem efeitos colaterais)
  # ---------------------------------------------------------------------------

  # Recebe uma lista de resultados {:ok, valor} | {:error, msg}.
  # Se houver erros, agrega todos; caso contrário, devolve a lista de valores.
  defp validar(resultados) do
    case for({:error, msg} <- resultados, do: msg) do
      [] -> {:ok, for({:ok, valor} <- resultados, do: valor)}
      erros -> {:error, {:validacao, erros}}
    end
  end

  defp validar_texto(valor, campo) when is_binary(valor) do
    case String.trim(valor) do
      "" -> {:error, "#{campo} não pode ser vazio"}
      limpo -> {:ok, limpo}
    end
  end

  defp validar_texto(_valor, campo), do: {:error, "#{campo} deve ser um texto"}

  defp validar_inteiro_positivo(valor, _campo) when is_integer(valor) and valor > 0,
    do: {:ok, valor}

  defp validar_inteiro_positivo(_valor, campo),
    do: {:error, "#{campo} deve ser um inteiro positivo"}

  defp validar_dias(dias) when is_integer(dias) and dias >= 1 and dias <= @limite_dias,
    do: {:ok, dias}

  defp validar_dias(_dias),
    do: {:error, "dias deve ser um inteiro entre 1 e #{@limite_dias}"}

  # ---------------------------------------------------------------------------
  # Camada operacional (acesso ao Agent com tratamento de falhas de infraestrutura)
  # ---------------------------------------------------------------------------

  # Executa a função de transição dentro do Agent. Qualquer falha de
  # infraestrutura (Agent inexistente, timeout, crash) é convertida em erro operacional.
  defp atualizar(fun) do
    Agent.get_and_update(@nome, fun, @timeout)
  catch
    :exit, {:timeout, _} -> {:error, {:operacional, :timeout}}
    :exit, _motivo -> {:error, {:operacional, :servico_indisponivel}}
  end
end

# ---------------------------------------------------------------------------
# Demonstração
# ---------------------------------------------------------------------------

{:ok, _pid} = Biblioteca.iniciar()

{:ok, livro} = Biblioteca.cadastrar_livro(%{titulo: "Dom Casmurro"})
IO.inspect(livro, label: "Livro cadastrado")

# Validação: dados inválidos são rejeitados antes de qualquer operação
IO.inspect(Biblioteca.emprestar(0, "", 500), label: "Validação")
IO.inspect(Biblioteca.cadastrar_livro(%{titulo: "   "}), label: "Validação")

# Operacional: regra de negócio
{:ok, emp} = Biblioteca.emprestar(livro.id, "Ana", 7)
IO.inspect(emp, label: "Empréstimo")
IO.inspect(Biblioteca.emprestar(livro.id, "Bruno", 7), label: "Operacional")

# Devolução
IO.inspect(Biblioteca.devolver(emp.id), label: "Devolução")
IO.inspect(Biblioteca.devolver(emp.id), label: "Operacional")

# Execução periódica: empréstimo com prazo de 1 dia, verificado a cada 500 ms
# (simulamos o atraso alterando a data prevista diretamente no Agent)
{:ok, emp2} = Biblioteca.emprestar(livro.id, "Carla", 1)
Agent.update(Biblioteca, fn e ->
  vencido = Date.add(Date.utc_today(), -1)
  %{e | emprestimos: Map.update!(e.emprestimos, emp2.id, &%{&1 | devolucao_prevista: vencido})}
end)

{:ok, tref} = Biblioteca.agendar_verificacao_atrasos(500)
Process.sleep(1_200)
:timer.cancel(tref)

IO.inspect(Biblioteca.listar_atrasados(), label: "Atrasados")
