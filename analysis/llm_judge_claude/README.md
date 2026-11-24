# Avaliação dos code smells com LLM como juiz

Avaliação dos 750 códigos (`output/`) quanto aos 35 smells do catálogo de Lucas Vegi,
usando duas LLMs como juízes.

| Arquivo | Conteúdo |
|---|---|
| `Planilha resultados LLM - GPT-6.1 Sol.xlsx` | Avaliação feita pelo GPT-6.1 Sol (Codex), um smell por vez sobre todos os códigos. |
| `Planilha resultados LLM - Claude.xlsx` | Mesma planilha preenchida pelo Claude (Opus 5.5). A aba de estatísticas recalcula ao abrir no Excel. |
| `results/<modelo>__code_NNN.json` | Resultado do Claude por código, com justificativa para cada smell marcado. |
| `referencia_smells.md` | Referência dos 35 smells extraída do ExSmell-Gold (Dias e Vegi), na ordem das colunas da planilha. |
| `prompt_juiz.md` | Prompt usado pelo Claude. |
| `build_ref.py` | Gera a referência a partir de um clone do ExSmell-Gold. |
| `compare.py` | Compara os resultados do Claude com a planilha do GPT-6.1 Sol. |

O Claude avaliou cada código percorrendo os 35 smells em sequência, sem acesso aos
resultados do GPT-6.1 Sol, e foi instruído a marcar um smell só quando houvesse um trecho
concreto que correspondesse à definição.

Resultado geral: o Claude marcou 555 smells e o GPT-6.1 Sol 3.028. A concordância bruta foi
de 89,4%, com kappa de Cohen de 0,19.
