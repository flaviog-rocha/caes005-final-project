# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de transações financeiras, atendendo somente aos seguintes requisitos:
#
# 1. Recuperar processos após falhas inesperadas.
# 2. Permitir comunicação entre diferentes processos.
# 3. Gerar notificações quando determinadas condições forem atingidas.
# 4. Persistir os dados para recuperação após reinicializações.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - GenServer
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# finance.exs
# Execução: elixir finance.exs
#
# Gerenciamento de transações financeiras com GenServer:
#  1. Recuperação de falhas: cada conta é um GenServer supervisionado
#     (DynamicSupervisor). Se o processo cair, é reiniciado e recarrega
#     seu estado do disco.
#  2. Comunicação entre processos: chamadas síncronas (GenServer.call),
#     transferências coordenadas entre contas e assinantes (Registry :duplicate).
#  3. Notificações: enviadas aos assinantes quando o saldo cruza um limite.
#  4. Persistência: estado gravado em disco de forma atômica (escrita em
#     arquivo temporário + fsync + rename) antes de cada resposta de sucesso.

# ---------------------------------------------------------------------------
# Persistência em disco
# ---------------------------------------------------------------------------
defmodule Finance.Store do
  # Diretório de dados configurável por variável de ambiente.
  def dir, do: System.get_env("FINANCE_DATA_DIR", "data")

  defp path(id), do: Path.join(dir(), "#{id}.term")

  # Carrega o termo salvo de uma conta.
  def load(id) do
    case File.read(path(id)) do
      {:ok, bin} ->
        try do
          {:ok, :erlang.binary_to_term(bin, [:safe])}
        rescue
          ArgumentError -> {:error, :corrupted}
        end

      {:error, :enoent} ->
        :not_found

      {:error, reason} ->
        {:error, reason}
    end
  end

  # Grava o termo de forma atômica e durável.
  def save(id, term) do
    File.mkdir_p!(dir())
    final = path(id)
    tmp = final <> ".tmp"
    bin = :erlang.term_to_binary(term)

    with {:ok, io} <- File.open(tmp, [:write, :binary]),
         :ok <- IO.binwrite(io, bin),
         :ok <- :file.sync(io),
         :ok <- File.close(io),
         :ok <- File.rename(tmp, final) do
      :ok
    else
      {:error, reason} -> {:error, reason}
    end
  end
end

# ---------------------------------------------------------------------------
# Processo de conta (um GenServer por conta)
# ---------------------------------------------------------------------------
defmodule Finance.Account do
  use GenServer

  # Limite de histórico mantido em memória e em disco.
  @max_history 100

  defstruct id: nil, balance: 0, threshold: nil, history: [], version: 0

  # -------------------------- API pública --------------------------------

  def child_spec(opts) do
    %{
      id: {__MODULE__, Keyword.fetch!(opts, :id)},
      start: {__MODULE__, :start_link, [opts]},
      restart: :permanent,
      type: :worker
    }
  end

  def start_link(opts) do
    id = Keyword.fetch!(opts, :id)
    GenServer.start_link(__MODULE__, opts, name: via(id))
  end

  # Nome de registro do processo da conta.
  def via(id), do: {:via, Registry, {Finance.Registry, {:account, id}}}

  def deposit(id, amount), do: call(id, {:deposit, amount})
  def withdraw(id, amount), do: call(id, {:withdraw, amount})
  def balance(id), do: call(id, :balance)

  # Assina notificações da conta. O processo chamador passa a receber
  # mensagens no formato {:finance, evento}.
  def subscribe(id), do: Registry.register(Finance.PubSub, {:account, id}, nil)

  # Cancela a assinatura do processo chamador.
  def unsubscribe(id), do: Registry.unregister(Finance.PubSub, {:account, id})

  defp call(id, msg) do
    GenServer.call(via(id), msg)
  catch
    # Conta não está aberta / em reinício.
    :exit, {:noproc, _} -> {:error, :not_open}
  end

  # -------------------------- Callbacks ----------------------------------

  @impl true
  def init(opts) do
    id = Keyword.fetch!(opts, :id)

    # Recuperação: se existir estado persistido, ele é carregado.
    state =
      case Finance.Store.load(id) do
        {:ok, %__MODULE__{} = saved} -> saved
        :not_found -> %__MODULE__{id: id, threshold: Keyword.get(opts, :threshold)}
        {:error, reason} -> exit({:load_failed, reason})
      end

    {:ok, state}
  end

  @impl true
  def handle_call(:balance, _from, state) do
    {:reply, {:ok, state.balance}, state}
  end

  def handle_call({:deposit, amount}, _from, state) when is_integer(amount) and amount > 0 do
    apply_change(state, state.balance + amount, {:deposit, amount})
  end

  def handle_call({:withdraw, amount}, _from, state) when is_integer(amount) and amount > 0 do
    if amount > state.balance do
      {:reply, {:error, :insufficient_funds}, state}
    else
      apply_change(state, state.balance - amount, {:withdraw, amount})
    end
  end

  def handle_call(_invalid, _from, state) do
    {:reply, {:error, :invalid_request}, state}
  end

  # -------------------------- Auxiliares ---------------------------------

  # Persiste antes de responder: só confirma o que está em disco.
  defp apply_change(old, new_balance, op) do
    entry = {op, DateTime.utc_now()}

    new = %{
      old
      | balance: new_balance,
        version: old.version + 1,
        history: Enum.take([entry | old.history], @max_history)
    }

    case Finance.Store.save(new.id, new) do
      :ok ->
        maybe_notify(old, new)
        {:reply, {:ok, new.balance}, new}

      {:error, reason} ->
        {:reply, {:error, {:persist_failed, reason}}, old}
    end
  end

  # Notifica somente quando o saldo cruza o limite para baixo.
  defp maybe_notify(old, new) do
    if is_integer(new.threshold) and old.balance >= new.threshold and new.balance < new.threshold do
      notify(new.id, {:low_balance, new.id, new.balance, new.threshold})
    end

    :ok
  end

  defp notify(id, event) do
    Registry.dispatch(Finance.PubSub, {:account, id}, fn entries ->
      for {pid, _} <- entries, do: send(pid, {:finance, event})
    end)
  end
end

# ---------------------------------------------------------------------------
# Fachada do banco: abertura de contas, restauração e transferências
# ---------------------------------------------------------------------------
defmodule Finance.Bank do
  # Abre (ou reutiliza) o processo de uma conta sob o supervisor dinâmico.
  def open(id, opts \\ []) do
    spec = {Finance.Account, Keyword.put(opts, :id, id)}

    case DynamicSupervisor.start_child(Finance.AccountSup, spec) do
      {:ok, pid} -> {:ok, pid}
      {:error, {:already_started, pid}} -> {:ok, pid}
      other -> other
    end
  end

  # Reabre todas as contas persistidas após reinicialização do sistema.
  def restore do
    case File.ls(Finance.Store.dir()) do
      {:ok, files} ->
        files
        |> Enum.filter(&String.ends_with?(&1, ".term"))
        |> Enum.map(&Path.rootname/1)
        |> Enum.map(&open/1)

      {:error, _} ->
        []
    end
  end

  # Transferência entre contas com compensação em caso de falha no destino.
  def transfer(from, to, amount) when is_integer(amount) and amount > 0 do
    with {:ok, _} <- Finance.Account.withdraw(from, amount) do
      case Finance.Account.deposit(to, amount) do
        {:ok, _} ->
          {:ok, :transferred}

        {:error, reason} ->
          # Compensação: devolve o valor à origem.
          _ = Finance.Account.deposit(from, amount)
          {:error, reason}
      end
    end
  end

  def transfer(_from, _to, _amount), do: {:error, :invalid_amount}
end

# ---------------------------------------------------------------------------
# Aplicação: árvore de supervisão
# ---------------------------------------------------------------------------
defmodule Finance.Application do
  use Application

  @impl true
  def start(_type, _args) do
    children = [
      {Registry, keys: :unique, name: Finance.Registry},
      {Registry, keys: :duplicate, name: Finance.PubSub},
      {DynamicSupervisor, strategy: :one_for_one, name: Finance.AccountSup}
    ]

    {:ok, sup} = Supervisor.start_link(children, strategy: :one_for_one, name: Finance.Supervisor)

    # Recuperação após reinicialização do sistema.
    Finance.Bank.restore()

    {:ok, sup}
  end
end

# ---------------------------------------------------------------------------
# Demonstração
# ---------------------------------------------------------------------------
defmodule Finance.Demo do
  def run do
    id = "conta-demo"
    {:ok, _} = Finance.Bank.open(id, threshold: 500)
    {:ok, _} = Finance.Bank.open("conta-b")
    {:ok, _} = Finance.Account.subscribe(id)

    {:ok, _} = Finance.Account.deposit(id, 1000)
    {:ok, balance} = Finance.Account.withdraw(id, 600)
    IO.puts("Saldo após saque: #{balance}")

    receive do
      {:finance, event} -> IO.puts("Notificação recebida: #{inspect(event)}")
    after
      500 -> IO.puts("Nenhuma notificação recebida")
    end

    IO.inspect(Finance.Bank.transfer(id, "conta-b", 100), label: "Transferência")

    # Simula falha: mata o processo da conta; o supervisor o reinicia
    # e o estado é recarregado do disco.
    [{pid, _}] = Registry.lookup(Finance.Registry, {:account, id})
    Process.exit(pid, :kill)
    Process.sleep(100)

    IO.inspect(Finance.Account.balance(id), label: "Saldo após recuperação")
  end
end

# ---------------------------------------------------------------------------
# Ponto de entrada
# ---------------------------------------------------------------------------
{:ok, _} = Finance.Application.start(:normal, [])
Finance.Demo.run()
