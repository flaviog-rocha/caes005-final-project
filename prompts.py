import argparse
import json
import random
import re
from pathlib import Path

from openrouter_client import OPENROUTER_ENABLED, call_model


BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"
OUTPUT_DIR = BASE_DIR / "output"

# Altere esta variável para mudar o padrão dos arquivos gerados.
CODE_FILE_PREFIX = "code_"
CODE_FILE_EXTENSION = ".ex"
MIN_REQUIREMENTS = 3
MAX_REQUIREMENTS = 5
MAX_MANDATORY_RESOURCES = 1

TASKS = [
    {"id": "T01", "domain": "Estoque", "task": "o gerenciamento de estoque de uma loja"},
    {"id": "T02", "domain": "E-commerce", "task": "o gerenciamento de pedidos de um e-commerce"},
    {"id": "T03", "domain": "Reservas", "task": "o gerenciamento de reservas de salas"},
    {"id": "T04", "domain": "Sensores", "task": "o monitoramento de sensores"},
    {"id": "T05", "domain": "Arquivos", "task": "o processamento de arquivos CSV"},
    {"id": "T06", "domain": "Cache", "task": "o gerenciamento de um sistema de cache"},
    {"id": "T07", "domain": "Filas", "task": "o gerenciamento de uma fila de tarefas"},
    {"id": "T08", "domain": "Comunicação", "task": "um sistema de troca de mensagens entre usuários"},
    {"id": "T09", "domain": "Financeiro", "task": "o gerenciamento de transações financeiras"},
    {"id": "T10", "domain": "Usuários", "task": "o gerenciamento de usuários e sessões"},
    {"id": "T11", "domain": "Biblioteca", "task": "o gerenciamento de empréstimos de livros"},
    {"id": "T12", "domain": "Entrega", "task": "o gerenciamento de entregas e rastreamento"},
    {"id": "T13", "domain": "Agendamento", "task": "um sistema de agendamento de compromissos"},
    {"id": "T14", "domain": "Votação", "task": "um sistema de votação online"},
    {"id": "T15", "domain": "Notificações", "task": "um sistema de gerenciamento de notificações"},
]

REQUIREMENTS = [
    {"id": "R01", "text": "Permitir cadastrar, consultar, atualizar e remover registros."},
    {"id": "R02", "text": "Validar os dados recebidos antes de realizar operações."},
    {"id": "R03", "text": "Manter um histórico das operações realizadas."},
    {"id": "R04", "text": "Permitir a execução de operações simultâneas."},
    {"id": "R05", "text": "Garantir a consistência dos dados durante operações concorrentes."},
    {"id": "R06", "text": "Permitir que diferentes entidades sejam processadas independentemente."},
    {"id": "R07", "text": "Detectar encerramentos inesperados de processos."},
    {"id": "R08", "text": "Recuperar processos após falhas inesperadas."},
    {"id": "R09", "text": "Persistir os dados para recuperação após reinicializações."},
    {"id": "R10", "text": "Impedir atualizações parcialmente concluídas."},
    {"id": "R11", "text": "Executar determinadas operações periodicamente."},
    {"id": "R12", "text": "Implementar expiração automática de registros."},
    {"id": "R13", "text": "Diferenciar erros de validação de falhas operacionais."},
    {"id": "R14", "text": "Implementar novas tentativas após falhas temporárias."},
    {"id": "R15", "text": "Permitir comunicação entre diferentes processos."},
    {"id": "R16", "text": "Processar grandes volumes de dados sem carregá-los integralmente na memória."},
    {"id": "R17", "text": "Impedir que uma mesma operação seja executada mais de uma vez."},
    {"id": "R18", "text": "Permitir limitar a quantidade de operações simultâneas."},
    {"id": "R19", "text": "Gerar notificações quando determinadas condições forem atingidas."},
    {"id": "R20", "text": "Permitir consultar o estado atual das operações em andamento."},
]

ELIXIR_RESOURCES = [
    "GenServer", "Agent", "Task", "Task.Supervisor", "DynamicSupervisor",
    "Registry", "ETS", "DETS", "Stream", "Process", "Supervisor", "Logger",
]


def load_data():
    DATA_DIR.mkdir(exist_ok=True)
    (DATA_DIR / "tasks.json").write_text(
        json.dumps(TASKS, indent=4, ensure_ascii=False), encoding="utf-8"
    )
    (DATA_DIR / "requirements.json").write_text(
        json.dumps(REQUIREMENTS, indent=4, ensure_ascii=False), encoding="utf-8"
    )


def generate_prompt(task, requirements, mode):
    requirements_text = "\n".join(
        f"{i}. {req['text']}" for i, req in enumerate(requirements, start=1)
    )
    prompt = (
        f"Você é um engenheiro de software sênior focado em programação funcional. "
        f"Preciso que você gere um código Elixir que faça {task['task']}, "
        f"atendendo somente aos seguintes requisitos:\n\n{requirements_text}\n\n"
    )
    resources = []
    if mode == "mandatory":
        resources = random.sample(ELIXIR_RESOURCES, MAX_MANDATORY_RESOURCES)
        prompt += (
            "A implementação deve utilizar os seguintes recursos do Elixir:\n"
            + "\n".join(f"- {resource}" for resource in resources)
            + "\n\n"
        )
    prompt += (
        "Gere um único arquivo autocontido, prefira recursos da biblioteca "
        "padrão do Elixir e não adicione dependências externas. "
        "Inclua testes ExUnit que verifiquem os requisitos."
    )
    return prompt, resources


def generate_dataset(
    count=100,
    seed=42,
    mandatory_ratio=0.5,
    min_requirements=MIN_REQUIREMENTS,
    max_requirements=MAX_REQUIREMENTS,
):
    if min_requirements < 1:
        raise ValueError("min_requirements deve ser positivo")
    if max_requirements < min_requirements:
        raise ValueError("max_requirements deve ser maior ou igual a min_requirements")
    if max_requirements > len(REQUIREMENTS):
        raise ValueError("max_requirements não pode exceder os requisitos disponíveis")

    random.seed(seed)
    dataset = []
    mandatory_count = round(count * mandatory_ratio)
    for i in range(count):
        task = random.choice(TASKS)
        selected_requirements = random.sample(
            REQUIREMENTS,
            random.randint(min_requirements, max_requirements),
        )
        mode = "mandatory" if i < mandatory_count else "free"
        prompt, resources = generate_prompt(task, selected_requirements, mode)
        dataset.append({
            "task_id": f"TASK-{i + 1:03}",
            "domain": task["domain"],
            "task": task["task"],
            "mode": mode,
            "requirements": [req["id"] for req in selected_requirements],
            "resources": resources,
            "prompt": prompt,
            "seed": seed,
        })
    return dataset


def save_dataset(dataset):
    OUTPUT_DIR.mkdir(exist_ok=True)
    output_file = OUTPUT_DIR / "prompts.json"
    output_file.write_text(
        json.dumps(dataset, indent=4, ensure_ascii=False), encoding="utf-8"
    )
    print(f"\n{len(dataset)} prompts gerados!")
    print(f"Arquivo: {output_file}")


def load_saved_dataset(prompt_file):
    """Carrega prompts salvos para reutilização em uma nova execução."""
    try:
        dataset = json.loads(prompt_file.read_text(encoding="utf-8-sig"))
    except FileNotFoundError as error:
        raise ValueError(
            f"Arquivo de prompts não encontrado: {prompt_file}"
        ) from error
    except json.JSONDecodeError as error:
        raise ValueError(
            f"Arquivo de prompts inválido: {prompt_file}"
        ) from error

    if not isinstance(dataset, list) or not dataset:
        raise ValueError(
            f"Arquivo de prompts deve conter uma lista não vazia: {prompt_file}"
        )

    invalid_items = [
        index + 1
        for index, item in enumerate(dataset)
        if not isinstance(item, dict) or not isinstance(item.get("prompt"), str)
    ]
    if invalid_items:
        raise ValueError(
            "Os seguintes itens não possuem um campo 'prompt' válido: "
            + ", ".join(map(str, invalid_items))
        )

    return dataset


def model_directory(model):
    """Converte o nome do modelo em um nome de pasta seguro e legível."""
    return re.sub(r"[^A-Za-z0-9._-]+", "_", model.replace("/", "_"))


def next_code_number(directory, prefix=CODE_FILE_PREFIX):
    pattern = re.compile(rf"^{re.escape(prefix)}(\d+){re.escape(CODE_FILE_EXTENSION)}$")
    numbers = [
        int(match.group(1))
        for path in directory.iterdir()
        if (match := pattern.match(path.name))
    ]
    return max(numbers, default=0) + 1


def add_prompt_comment(code, prompt):
    prompt_comment = "\n".join(
        f"# {line}" if line else "#" for line in prompt.splitlines()
    )
    prompt_header = f"# Prompt gerado:\n{prompt_comment}\n\n"
    code = code.strip()
    if code.startswith("```"):
        code = re.sub(
            r"^```(?:elixir|ex)?\s*", "", code, count=1, flags=re.IGNORECASE
        )
        code = re.sub(r"\s*```$", "", code)
    if code.startswith(prompt_header):
        code = code[len(prompt_header):].lstrip()
    return f"{prompt_header}{code.strip()}\n"


def save_generated_code(code, prompt, model, prefix=CODE_FILE_PREFIX):
    directory = OUTPUT_DIR / model_directory(model)
    directory.mkdir(parents=True, exist_ok=True)
    number = next_code_number(directory, prefix)
    output_file = directory / f"{prefix}{number:03d}{CODE_FILE_EXTENSION}"
    output_file.write_text(add_prompt_comment(code, prompt), encoding="utf-8")
    print(f"Código salvo em: {output_file}")
    return output_file


def main():
    parser = argparse.ArgumentParser(
        description="Gerador de prompts para código Elixir"
    )
    parser.add_argument("--count", type=int, default=100)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--mandatory-ratio", type=float, default=0.5)
    parser.add_argument(
        "--min-requirements",
        type=int,
        default=MIN_REQUIREMENTS,
        help="Quantidade mínima de requisitos por prompt.",
    )
    parser.add_argument(
        "--max-requirements",
        type=int,
        default=MAX_REQUIREMENTS,
        help="Quantidade máxima de requisitos por prompt.",
    )
    parser.add_argument("--model", default="openai/gpt-4o")
    parser.add_argument("--generate-code", action="store_true")
    parser.add_argument(
        "--reuse-prompts",
        action="store_true",
        help="Reutiliza os prompts salvos em output/prompts.json.",
    )
    parser.add_argument(
        "--prompts-file",
        type=Path,
        default=OUTPUT_DIR / "prompts.json",
        help="Arquivo JSON usado com --reuse-prompts.",
    )
    parser.add_argument(
        "--offline",
        action="store_true",
        help="Não chama o OpenRouter nesta execução.",
    )
    parser.add_argument(
        "--file-prefix",
        default=CODE_FILE_PREFIX,
        help="Prefixo dos arquivos, por exemplo code_test_.",
    )
    args = parser.parse_args()

    if args.count <= 0:
        parser.error("--count deve ser positivo")
    if not 0 <= args.mandatory_ratio <= 1:
        parser.error("--mandatory-ratio deve estar entre 0 e 1")
    if args.min_requirements < 1:
        parser.error("--min-requirements deve ser positivo")
    if args.max_requirements < args.min_requirements:
        parser.error("--max-requirements deve ser maior ou igual a --min-requirements")
    if args.max_requirements > len(REQUIREMENTS):
        parser.error("--max-requirements excede os requisitos disponíveis")
    if not args.file_prefix or Path(args.file_prefix).name != args.file_prefix:
        parser.error("--file-prefix deve conter apenas o nome do prefixo")

    if args.reuse_prompts:
        try:
            dataset = load_saved_dataset(args.prompts_file)
        except ValueError as error:
            parser.error(str(error))
        print(f"\n{len(dataset)} prompts reutilizados!")
        print(f"Arquivo: {args.prompts_file}")
    else:
        load_data()
        dataset = generate_dataset(
            count=args.count,
            seed=args.seed,
            mandatory_ratio=args.mandatory_ratio,
            min_requirements=args.min_requirements,
            max_requirements=args.max_requirements,
        )
        save_dataset(dataset)

    if args.generate_code and OPENROUTER_ENABLED and not args.offline:
        for item in dataset:
            code = call_model(item["prompt"], args.model)
            save_generated_code(code, item["prompt"], args.model, args.file_prefix)
    elif args.generate_code:
        if args.offline:
            print("Modo offline: nenhum código foi solicitado ao OpenRouter.")
        else:
            print(
                "Comunicação com o OpenRouter desabilitada em "
                "openrouter_client.py; nenhum código foi solicitado à API."
            )


if __name__ == "__main__":
    main()
