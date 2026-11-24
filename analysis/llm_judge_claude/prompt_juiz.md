# Tarefa: avaliar code smells em códigos Elixir gerados por LLM

Você é um avaliador de code smells em Elixir. A base de referência é o catálogo de
Lucas Vegi, representado pelos exemplos anotados do benchmark ExSmell-Gold (Dias e Vegi).

## Material
- Referência dos 35 smells: `C:\Users\cirog\AppData\Local\Temp\claude\49662b06f8376c91\73afa5e2-4800-4e0d-a582-b45fd6c2529a\scratchpad\judge\referencia_smells.md`
  (leia inteira antes de começar).
- Códigos: `C:\Users\cirog\source\Mestrado\IntroProgFunc\caes005-final-project\output\<pasta_modelo>\code_NNN.ex`.
  Cada arquivo começa com o prompt que o originou, em comentário. Esse cabeçalho de prompt
  NÃO faz parte do código avaliado (não conte como Comments, por exemplo).
- Apenas leia os arquivos. Nunca execute nem compile os códigos.
- Não consulte nenhuma outra avaliação existente (planilhas, CSVs, a pasta `analysis/`).

## Procedimento (um smell por vez)
Para cada código, percorra os 35 smells NA ORDEM da referência, um de cada vez. Para cada
smell, releia mentalmente a definição e o exemplo dele e decida se o código contém aquele
smell (COM) ou não (SEM). Seja conservador: marque COM só quando houver um trecho concreto
que corresponda à definição. Se o código tiver erro de sintaxe, avalie mesmo assim pelo que
dá para ler e marque `"sintaxe_invalida": true`.

## Saída
Para cada código, grave um JSON (UTF-8) em
`C:\Users\cirog\AppData\Local\Temp\claude\49662b06f8376c91\73afa5e2-4800-4e0d-a582-b45fd6c2529a\scratchpad\judge\results\<pasta_modelo>__code_NNN.json`
com exatamente este formato:

```json
{
  "arquivo": "<pasta_modelo> code_NNN",
  "sintaxe_invalida": false,
  "smells": {"1": "SEM", "2": "COM", "...": "...", "35": "SEM"},
  "justificativas": {"2": "função X/2 ... (1 frase, citando a função ou trecho)"}
}
```

`smells` deve ter as 35 chaves de "1" a "35". `justificativas` só para os marcados COM.
Grave os arquivos com a ferramenta Write. Ao final, responda só com uma linha por código:
`<arquivo>: <lista dos números marcados COM>`.
