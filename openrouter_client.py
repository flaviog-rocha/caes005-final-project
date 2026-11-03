import os


OPENROUTER_ENABLED = False


def call_model(prompt, model):
    """Envia um prompt ao OpenRouter quando a comunicação está habilitada."""
    if not OPENROUTER_ENABLED:
        raise RuntimeError(
            "Comunicação com o OpenRouter desabilitada. "
            "Altere OPENROUTER_ENABLED para True em openrouter_client.py "
            "para habilitá-la."
        )

    api_key = os.getenv("OPENROUTER_API_KEY")
    if not api_key:
        raise RuntimeError(
            "API Key não configurada. Defina OPENROUTER_API_KEY "
            "antes de habilitar a comunicação."
        )

    try:
        from openrouter import OpenRouter
    except ImportError as error:
        raise RuntimeError(
            "Pacote 'openrouter' não instalado. Instale a dependência "
            "antes de habilitar a comunicação."
        ) from error

    with OpenRouter(api_key=api_key) as client:
        response = client.chat.send(
            model=model,
            messages=[
                {
                    "role": "system",
                    "content": (
                        "Você é um programador especialista em Elixir. "
                        "Implemente os requisitos solicitados. "
                        "Retorne somente o código Elixir, sem repetir o prompt, "
                        "sem adicionar o cabeçalho 'Prompt gerado' e sem usar "
                        "blocos Markdown com crases."
                    ),
                },
                {"role": "user", "content": prompt},
            ],
            temperature=0,
            stream=False,
        )

    try:
        choice = response.choices[0]
        message = choice.message
        content = message.content
    except (AttributeError, IndexError, KeyError, TypeError):
        try:
            content = response["choices"][0]["message"]["content"]
        except (AttributeError, IndexError, KeyError, TypeError) as error:
            raise RuntimeError(
                "A resposta do OpenRouter não contém conteúdo de código."
            ) from error

    if not content:
        raise RuntimeError(
            "A resposta do OpenRouter não contém conteúdo de código."
        )
    return content
