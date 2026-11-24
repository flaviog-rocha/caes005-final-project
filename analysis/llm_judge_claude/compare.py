"""Compara as avaliações do juiz Claude com a planilha do Flavio (GPT-6.1 Sol)."""
import json
import sys
from pathlib import Path

import openpyxl

HERE = Path(__file__).parent
XLSX = Path(sys.argv[1]) if len(sys.argv) > 1 else Path.home() / "Downloads" / "Planilha resultados LLM.xlsx"

ws = openpyxl.load_workbook(XLSX, data_only=True)["Plan1"]
names = [ws.cell(4, c).value for c in range(5, 40)]
flavio = {}
for r in range(5, 755):
    key = ws.cell(r, 3).value
    flavio[key] = {str(i + 1): ws.cell(r, 5 + i).value for i in range(35)}

stats = {str(i): [0, 0, 0, 0] for i in range(1, 36)}  # ambos COM, só Claude, só Sol, ambos SEM
total_eq = total = 0
for f in sorted((HERE / "results").glob("*.json")):
    data = json.loads(f.read_text(encoding="utf-8"))
    ref = flavio[data["arquivo"]]
    only_c, only_s = [], []
    for k in map(str, range(1, 36)):
        c, s = data["smells"][k] == "COM", ref[k] == "COM"
        stats[k][(0 if c and s else 1 if c else 2 if s else 3)] += 1
        total += 1
        total_eq += c == s
        if c and not s:
            only_c.append(k)
        if s and not c:
            only_s.append(k)
    print(f"{data['arquivo']}: só Claude {only_c} | só Sol {only_s}")

print(f"\nConcordância geral: {total_eq}/{total} ({total_eq / max(total, 1):.1%})")
print("smell | ambos COM | só Claude | só Sol | ambos SEM")
for k, v in stats.items():
    if v[0] or v[1] or v[2]:
        print(f"{k:>2} | {v[0]} | {v[1]} | {v[2]} | {v[3]}")
