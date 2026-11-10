# Code smells em código Elixir gerado por LLMs

Projeto final da disciplina CAES005 (Introdução à Programação Funcional). O
objetivo é investigar quais *code smells* aparecem em código Elixir gerado por
modelos de linguagem (LLMs) e com que frequência.

## Ideia

1. **Gerar prompts automaticamente.** Cada prompt pede um código Elixir para um
   domínio (estoque, reservas, filas, cache...) que atenda a um subconjunto
   sorteado de requisitos típicos da programação funcional e concorrente
   (supervisão, persistência, concorrência, expiração, macros...).
2. **Enviar os mesmos prompts a diferentes LLMs**, gerando um conjunto de
   códigos por modelo.
3. **Revisar os códigos** em busca dos code smells do catálogo de
   [Lucas Vegi](https://github.com/lucasvegi/Elixir-Code-Smells)
   (smells de design, de baixo nível e tradicionais).
4. **Analisar estatisticamente** os resultados: quais smells são mais
   frequentes e como os modelos se comparam.
5. **Validar a revisão manual** usando outra LLM como juiz (*LLM as a judge*).

O foco do trabalho é a revisão e a análise. A geração serve apenas para
produzir o material a ser avaliado.

## Estrutura

| Caminho | Conteúdo |
|---|---|
| `prompts.py` | Gerador de prompts e de códigos (chama a LLM via OpenRouter). |
| `openrouter_client.py` | Cliente do OpenRouter usado pelo gerador. |
| `data/` | Tarefas (`tasks.json`) e requisitos (`requirements.json`) usados nos prompts. |
| `output/prompts.json` | Os 125 prompts usados para gerar os códigos. |
| `output/<modelo>/code_NNN.ex` | Códigos gerados por cada modelo. Cada arquivo começa com o prompt que o originou, em comentário. |
| `CAES005_Code_Test_1.ex` | Código de teste usado no início do projeto para estudar os code smells. |

## Como os prompts são montados

- **15 tarefas** (domínios) e **22 requisitos** estão definidos em `prompts.py`.
- Cada prompt sorteia uma tarefa e de **3 a 5 requisitos**.
- Metade dos prompts (modo `mandatory`) exige um recurso específico do Elixir
  (ex.: `GenServer`, `ETS`, `Task.Supervisor`); a outra metade (modo `free`)
  deixa a escolha para o modelo.
- O conjunto usado (`output/prompts.json`) tem 125 prompts, gerados com
  `--count 125 --seed 42`: 62 `mandatory` e 63 `free`.
- Cada prompt é executado **2 vezes** por modelo, totalizando **250 códigos
  por modelo**. Os arquivos são numerados em ordem: `code_001.ex` e
  `code_002.ex` vêm do primeiro prompt, e assim por diante.

## Modelos avaliados

| Pasta em `output/` | Modelo | Como foi gerado |
|---|---|---|
| `openai_gpt-6-luna` | GPT-6 Luna (OpenAI) | OpenRouter, `temperature=0` |
| `anthropic_claude-haiku-5.5` | Claude Haiku 5.5 (Anthropic) | OpenRouter, `temperature=0` |
| `gemini-3.1-flash-lite` | Gemini 3.1 Flash-Lite (Google) | API do Gemini (Google AI Studio), temperatura padrão do modelo |

O Gemini foi gerado com a temperatura padrão porque, com `temperature=0`, ele
devolve exatamente o mesmo código nas duas repetições de cada prompt, o que
reduziria a amostra a 125 códigos distintos. Nos outros dois modelos as
repetições já variam com `temperature=0`. Essa diferença deve ser considerada
na comparação.

O arquivo `output/gemini-3.1-flash-lite/usage.jsonl` registra, para cada
código, os tokens de entrada e de saída e a latência da chamada. Com o preço
de novembro de 2026 (US$ 0,25 de entrada e US$ 1,50 de saída por milhão de
tokens), os 250 códigos custariam cerca de US$ 0,26 no plano pago.

## Como executar

Requisitos: Python 3, o pacote `openrouter` e a variável de ambiente
`OPENROUTER_API_KEY`.

```bash
# Gera um novo conjunto de prompts (sem chamar a LLM)
python prompts.py --count 125 --seed 42

# Gera 2 códigos por prompt reaproveitando os prompts salvos
python prompts.py --reuse-prompts --generate-code --prompt-repetitions 2 \
    --model openai/gpt-6-luna

# Retoma uma geração interrompida, gerando só as repetições que faltam
python prompts.py --resume --generate-code --prompt-repetitions 2 \
    --model openai/gpt-6-luna
```

Outras opções: `--prompt-limit` (processa só os N primeiros prompts),
`--file-prefix` (muda o prefixo dos arquivos), `--offline` (não chama a API)
e `--min-requirements`/`--max-requirements`. Veja `python prompts.py --help`.
