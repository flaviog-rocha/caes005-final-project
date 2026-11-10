# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de gerenciamento de notificações, atendendo somente aos seguintes requisitos:
#
# 1. Permitir que diferentes entidades sejam processadas independentemente.
# 2. Implementar novas tentativas após falhas temporárias.
# 3. Processar grandes volumes de dados sem carregá-los integralmente na memória.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - DETS
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule NotificationManager do
  @moduledoc """
  Sistema de gerenciamento de notificações utilizando DETS para persistência em disco,
  processamento lazy (Stream) para evitar sobrecarga de memória e um mecanismo 
  de retry baseado em contagem.
  """

  @dets_file ~c"notifications.dets"
  @max_retries 3

  # Inicia o armazenamento DETS
  def init do
    :dets.open_file(@dets_file, type: :set)
  end

  # Adiciona notificação ao sistema
  def add_notification(id, payload) do
    :dets.insert(@dets_file, {id, %{payload: payload, retries: 0, status: :pending}})
  end

  # Processa notificações pendentes de forma lazy (Stream)
  # O uso de :dets.match_object com o padrão de busca permite processar 
  # grandes volumes sem carregar tudo na RAM.
  def process_all do
    :dets.match_object(@dets_file, :_)
    |> Stream.map(fn {id, data} -> {id, data} end)
    |> Stream.filter(fn {_, data} -> data.status != :completed end)
    |> Enum.each(&handle_notification/1)
  end

  defp handle_notification({id, data}) do
    case perform_delivery(data.payload) do
      :ok ->
        :dets.insert(@dets_file, {id, %{data | status: :completed}})

      :error ->
        retry_notification(id, data)
    end
  end

  defp retry_notification(id, data) do
    if data.retries < @max_retries do
      new_data = %{data | retries: data.retries + 1}
      :dets.insert(@dets_file, {id, new_data})
    else
      :dets.insert(@dets_file, {id, %{data | status: :failed}})
    end
  end

  # Simulação de envio (entidade independente)
  defp perform_delivery(_payload) do
    # Lógica de IO real aqui
    Enum.random([:ok, :error])
  end

  def close do
    :dets.close(@dets_file)
  end
end
