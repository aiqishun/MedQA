import json
import argparse
from pathlib import Path


def derive_knowledge(meta: dict) -> str:
    """
    Derive a compact 'Knowledge' label from source_path.
    Example:
      data/raw/data_clean/questions/US/dev.jsonl -> US/dev
    """
    sp = (meta or {}).get("source_path", "")
    if not sp:
        return "unknown"

    # Normalize separators and split
    parts = sp.replace("\\", "/").split("/")

    # Try to find ".../questions/<REGION>/<SPLIT>.jsonl"
    if "questions" in parts:
        i = parts.index("questions")
        if i + 2 < len(parts):
            region = parts[i + 1]
            split_file = parts[i + 2]
            split = split_file.split(".")[0]  # dev.jsonl -> dev
            return f"{region}/{split}"

    # Fallback: last two path parts (best-effort)
    if len(parts) >= 2:
        return "/".join(parts[-2:]).split(".")[0]
    return parts[-1].split(".")[0] if parts else "unknown"


def main():
    parser = argparse.ArgumentParser(description="Convert MCQ JSONL to evaluation format")
    parser.add_argument("--input", default="data/derived/heart_disease_mcq.jsonl", help="Input MCQ jsonl file")
    parser.add_argument("--output", default="data/derived/cardio_eval.jsonl", help="Output eval jsonl file")
    parser.add_argument("--tag", default="Cardio-MedQA", help="Tag field")
    args = parser.parse_args()

    in_path = Path(args.input)
    out_path = Path(args.output)

    count = 0

    with in_path.open("r", encoding="utf-8") as fin, out_path.open("w", encoding="utf-8") as fout:
        for line in fin:
            line = line.strip()
            if not line:
                continue

            obj = json.loads(line)

            q = (obj.get("question") or "").strip()
            ans_idx = (obj.get("answer_idx") or "").strip()

            meta = obj.get("_extract_meta") or {}
            knowledge = derive_knowledge(meta)

            rec = {
                "Knowledge": knowledge,
                "Question": q,
                "Answer": ans_idx,
                "Prediction": "",
                "Tag": args.tag,
            }

            fout.write(json.dumps(rec, ensure_ascii=False) + "\n")
            count += 1

    print(f"Converted {count} records.")
    print(f"Saved to: {out_path}")


if __name__ == "__main__":
    main()
