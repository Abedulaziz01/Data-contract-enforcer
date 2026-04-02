
"""
Violation injector — multiplies every confidence value x100.
Turns 0.0-1.0 scale into 0-100 scale (the canonical breaking change).
Run once: python create_violation.py
"""
import json
from pathlib import Path

input_path  = "outputs/week3/extractions.jsonl"
output_path = "outputs/week3/extractions_violated.jsonl"

records = []
with open(input_path) as f:
    for line in f:
        line = line.strip()
        if line:
            records.append(json.loads(line))

for r in records:
    for fact in r.get("extracted_facts", []):
        fact["confidence"] = round(fact["confidence"] * 100, 1)

with open(output_path, "w") as f:
    for r in records:
        f.write(json.dumps(r) + "\n")

print(f"INJECTION: confidence scale changed from 0.0-1.0 to 0-100")
print(f"Input    : {input_path} ({len(records)} records)")
print(f"Output   : {output_path}")
