import re, sys
from pathlib import Path
G = Path(sys.argv[1]); out = Path(sys.argv[2])
# Ordem das colunas da planilha do Flavio (1..35)
ORDER = [
 ("design_related_smells","agent_obsession"),("design_related_smells","genserver_envy"),
 ("design_related_smells","unsupervised_process"),("design_related_smells","large_messages"),
 ("design_related_smells","unrelated_multi_clause_function"),("design_related_smells","complex_extractions_in_clauses"),
 ("design_related_smells","using_exceptions_for_control_flow"),("design_related_smells","untested_polymorphic_behaviors"),
 ("design_related_smells","code_organization_by_process"),("design_related_smells","large_code_generation_by_macros"),
 ("design_related_smells","data_manipulation_by_migration"),("design_related_smells","using_app_configuration_for_libraries"),
 ("design_related_smells","compile_time_global_configuration"),("design_related_smells","use_instead_of_import"),
 ("low_level_concerns_smells","working_with_invalid_data"),("low_level_concerns_smells","complex_branching"),
 ("low_level_concerns_smells","complex_else_clauses_in_with"),("low_level_concerns_smells","alternative_return_types"),
 ("low_level_concerns_smells","accessing_non_existent_map_struct_fields"),("low_level_concerns_smells","speculative_assumptions"),
 ("low_level_concerns_smells","modules_with_identical_names"),("low_level_concerns_smells","unnecessary_macros"),
 ("low_level_concerns_smells","dynamic_atom_creation"),
 ("traditional_smells","comments"),("traditional_smells","long_parameter_list"),("traditional_smells","long_function"),
 ("traditional_smells","primitive_obsession"),("traditional_smells","shotgun_surgery"),("traditional_smells","duplicated_code"),
 ("traditional_smells","feature_envy"),("traditional_smells","divergent_change"),("traditional_smells","inappropriate_intimacy"),
 ("traditional_smells","large_class"),("traditional_smells","speculative_generality"),("traditional_smells","switch_statements"),
]
parts = ["# Referência de code smells (extraída do ExSmell-Gold)\n\nCada smell traz explicações curtas de exemplos anotados e um trecho de exemplo com o smell marcado.\n"]
for i,(cat,s) in enumerate(ORDER,1):
    d = G/cat/s/"annotated"
    files = sorted(d.glob("*.md"))
    name = None; expl = []
    snippets = []
    for f in files:
        t = f.read_text(encoding="utf-8")
        m = re.search(r"\*\*Smell name:\*\*\s*(.+)", t); name = name or (m and m.group(1).strip())
        m = re.search(r"\*\*Short explanation:\*\*\s*(.+)", t)
        if m and len(expl) < 5: expl.append(m.group(1).strip())
        m = re.search(r"(# VALIDATION: SMELL START.*?# VALIDATION: SMELL END)", t, re.S)
        if m: snippets.append(m.group(1))
    snippets.sort(key=len)
    snip = [x for x in snippets if 150 < len(x) < 1800][:1] or snippets[:1]
    parts.append(f"\n## {i}. {name}  (`{s}`)\n")
    parts += [f"- {e}" for e in expl]
    for x in snip:
        parts.append("\nExemplo:\n```elixir\n" + x + "\n```")
out.write_text("\n".join(parts)+"\n", encoding="utf-8")
print(out, len(out.read_text(encoding='utf-8')))
