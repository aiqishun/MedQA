#!/usr/bin/env python3
import argparse
import json
import os
import re
from typing import Any, Dict, Iterable, Iterator, List, Optional, Sequence, Tuple


DEFAULT_KEYWORDS_EN = [
    "heart disease",
    "cardiac",
    "cardiovascular",
    "coronary",
    "coronary artery",
    "CAD",
    "atherosclerosis",
    "angina",
    "myocardial infarction",
    "MI",
    "STEMI",
    "NSTEMI",
    "myocarditis",
    "pericarditis",
    "endocarditis",
    "heart failure",
    "CHF",
    "cardiomyopathy",
    "arrhythmia",
    "atrial fibrillation",
    "AFib",
    "ventricular tachycardia",
    "ventricular fibrillation",
    "valvular",
    "aortic stenosis",
    "mitral regurgitation",
]

DEFAULT_KEYWORDS_ZH = [
    "心脏病",
    "心脏",
    "冠心病",
    "冠状动脉",
    "心肌梗死",
    "心梗",
    "心绞痛",
    "心衰",
    "心力衰竭",
    "心律失常",
    "房颤",
    "心肌炎",
    "心包炎",
    "心内膜炎",
    "心肌病",
    "瓣膜",
    "动脉粥样硬化",
]


def iter_files(root: str, allowed_exts: Sequence[str]) -> Iterator[str]:
    if os.path.isfile(root):
        yield root
        return
    for dirpath, _, filenames in os.walk(root):
        for filename in filenames:
            ext = os.path.splitext(filename)[1].lower()
            if ext in allowed_exts:
                yield os.path.join(dirpath, filename)


def iter_jsonl_records(path: str, encoding: str) -> Iterator[Tuple[int, Any]]:
    with open(path, "r", encoding=encoding, errors="replace") as f:
        for line_no, line in enumerate(f, start=1):
            s = line.strip()
            if not s:
                continue
            try:
                yield line_no, json.loads(s)
            except json.JSONDecodeError as e:
                yield line_no, {"text": s, "_parse_error": f"invalid_json: {e}"}


def iter_txt_records(path: str, encoding: str, min_line_length: int) -> Iterator[Tuple[int, Dict[str, Any]]]:
    with open(path, "r", encoding=encoding, errors="replace") as f:
        for line_no, line in enumerate(f, start=1):
            text = line.strip()
            if len(text) < min_line_length:
                continue
            yield line_no, {"text": text}


def flatten_strings(value: Any, max_items: int) -> List[str]:
    out: List[str] = []

    def visit(v: Any) -> None:
        nonlocal out
        if len(out) >= max_items:
            return
        if v is None:
            return
        if isinstance(v, str):
            s = v.strip()
            if s:
                out.append(s)
            return
        if isinstance(v, (int, float, bool)):
            out.append(str(v))
            return
        if isinstance(v, dict):
            for k, vv in v.items():
                if len(out) >= max_items:
                    return
                if isinstance(k, str) and k.strip():
                    out.append(k.strip())
                visit(vv)
            return
        if isinstance(v, (list, tuple, set)):
            for vv in v:
                if len(out) >= max_items:
                    return
                visit(vv)
            return
        out.append(str(v))

    visit(value)
    return out[:max_items]


def extract_text_from_record(
    record: Any,
    max_items: int,
    include_fields: Optional[Sequence[str]],
    exclude_fields: Sequence[str],
) -> str:
    if not isinstance(record, dict):
        return " ".join(flatten_strings(record, max_items))
    
    filtered = _filter_record_fields(record, include_fields, exclude_fields)
    return " ".join(flatten_strings(filtered, max_items))


def _filter_record_fields(
    record: Dict[str, Any],
    include_fields: Optional[Sequence[str]],
    exclude_fields: Sequence[str],
) -> Any:
    """Helper function to filter record fields."""
    if include_fields:
        return {field: record[field] for field in include_fields if field in record}
    
    if exclude_fields:
        return {k: v for k, v in record.items() if k not in exclude_fields}
    
    return record


def filter_record_for_output(
    record: Any,
    include_fields: Optional[Sequence[str]],
    exclude_fields: Sequence[str],
) -> Any:
    if not isinstance(record, dict):
        return record
    return _filter_record_fields(record, include_fields, exclude_fields)


def build_matcher(keywords: Sequence[str]) -> Tuple[re.Pattern, List[str]]:
    cleaned: List[str] = []
    for kw in keywords:
        kw = kw.strip()
        if kw:
            cleaned.append(kw)
    
    if not cleaned:
        return re.compile(r"(?!.*)", re.IGNORECASE), []

    parts: List[str] = []
    for kw in cleaned:
        has_cjk = any("\u4e00" <= ch <= "\u9fff" for ch in kw)
        if has_cjk:
            parts.append(re.escape(kw))
            continue

        kw_stripped = kw.strip()
        simple_wordish = re.fullmatch(r"[A-Za-z0-9]+", kw_stripped) is not None
        is_short = len(kw_stripped) <= 3
        is_all_caps = kw_stripped.isupper() and any(c.isalpha() for c in kw_stripped)

        if simple_wordish and (is_short or is_all_caps):
            parts.append(r"\b" + re.escape(kw_stripped) + r"\b")
        else:
            tokens = [re.escape(t) for t in kw_stripped.split()]
            if len(tokens) > 1:
                parts.append(r"\b" + r"\s+".join(tokens) + r"\b")
            else:
                parts.append(re.escape(kw_stripped))

    pattern = re.compile(r"(" + "|".join(parts) + r")", re.IGNORECASE)
    return pattern, cleaned


def detect_matches(pattern: re.Pattern, text: str, max_hits: int) -> List[str]:
    hits: List[str] = []
    for m in pattern.finditer(text):
        hits.append(m.group(0))
        if len(hits) >= max_hits:
            break
    return hits


def ensure_parent_dir(path: str) -> None:
    parent = os.path.dirname(os.path.abspath(path))
    if parent:
        os.makedirs(parent, exist_ok=True)


def _load_json_file(path: str, encoding: str) -> List[Tuple[int, Any]]:
    """Safely load a JSON file and return as a list of records."""
    try:
        with open(path, "r", encoding=encoding, errors="replace") as f:
            obj = json.load(f)
            return [(1, obj)]
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON in file '{path}': {e}")
    except IOError as e:
        raise IOError(f"Failed to read file '{path}': {e}")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Extract heart-disease related content from MedQA dataset files into a new JSONL file."
    )
    parser.add_argument(
        "--input",
        default="data/raw",
        help="Input file or directory to scan (default: data/raw)",
    )
    parser.add_argument(
        "--output",
        default="data/derived/heart_disease.jsonl",
        help="Output JSONL path (default: data/derived/heart_disease.jsonl)",
    )
    parser.add_argument(
        "--encoding",
        default="utf-8",
        help="Text encoding for reading files (default: utf-8)",
    )
    parser.add_argument(
        "--extensions",
        default=".jsonl,.json,.txt",
        help="Comma-separated list of file extensions to scan (default: .jsonl,.json,.txt)",
    )
    parser.add_argument(
        "--language",
        choices=["en", "zh", "both"],
        default="both",
        help="Keyword language set to use (default: both)",
    )
    parser.add_argument(
        "--keywords",
        default="",
        help="Override keywords with a comma-separated list (optional)",
    )
    parser.add_argument(
        "--min-line-length",
        type=int,
        default=20,
        help="Minimum line length for .txt line records (default: 20)",
    )
    parser.add_argument(
        "--max-flatten-items",
        type=int,
        default=200,
        help="Max number of string fragments extracted from a JSON record for matching (default: 200)",
    )
    parser.add_argument(
        "--fields",
        default="",
        help="Comma-separated top-level fields to match only (e.g., question,answer,options,text)",
    )
    parser.add_argument(
        "--include-meta-info",
        action="store_true",
        help="Include meta_info in matching and output (default: excluded)",
    )
    parser.add_argument(
        "--max-hits-per-record",
        type=int,
        default=20,
        help="Max number of keyword hits stored per matched record (default: 20)",
    )
    parser.add_argument(
        "--max-records",
        type=int,
        default=0,
        help="Stop after scanning N records total (0 = no limit)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Do not write output; only print counts",
    )
    args = parser.parse_args()

    allowed_exts = tuple(ext.strip().lower() for ext in args.extensions.split(",") if ext.strip())
    if not allowed_exts:
        raise SystemExit("Error: --extensions cannot be empty")

    # Build keywords list
    keywords_list: List[str] = []
    if args.keywords.strip():
        keywords_list = [k.strip() for k in args.keywords.split(",") if k.strip()]
    else:
        if args.language in ("en", "both"):
            keywords_list.extend(DEFAULT_KEYWORDS_EN)
        if args.language in ("zh", "both"):
            keywords_list.extend(DEFAULT_KEYWORDS_ZH)

    pattern, normalized_keywords = build_matcher(keywords_list)
    if not normalized_keywords:
        raise SystemExit("Error: No keywords provided or all keywords were empty.")

    include_fields = [f.strip() for f in args.fields.split(",") if f.strip()] or None
    exclude_fields = [] if args.include_meta_info else ["meta_info"]

    # Validate max_records
    if args.max_records < 0:
        raise SystemExit("Error: --max-records must be non-negative")

    total_records = 0
    matched_records = 0
    scanned_files = 0

    out_f = None
    if not args.dry_run:
        try:
            ensure_parent_dir(args.output)
            out_f = open(args.output, "w", encoding="utf-8")
        except IOError as e:
            raise SystemExit(f"Error: Failed to open output file '{args.output}': {e}")

    try:
        for path in iter_files(args.input, allowed_exts):
            scanned_files += 1
            ext = os.path.splitext(path)[1].lower()

            try:
                if ext == ".jsonl":
                    record_iter: Iterable[Tuple[int, Any]] = iter_jsonl_records(path, args.encoding)
                elif ext == ".txt":
                    record_iter = iter_txt_records(path, args.encoding, args.min_line_length)
                elif ext == ".json":
                    record_iter = _load_json_file(path, args.encoding)
                else:
                    continue
            except (IOError, ValueError) as e:
                print(f"Warning: Skipping file '{path}': {e}", flush=True)
                continue

            for line_no, record in record_iter:
                total_records += 1
                if args.max_records and total_records > args.max_records:
                    break

                text = extract_text_from_record(
                    record,
                    args.max_flatten_items,
                    include_fields=include_fields,
                    exclude_fields=exclude_fields,
                )
                hits = detect_matches(pattern, text, args.max_hits_per_record)
                if not hits:
                    continue

                matched_records += 1
                meta = {
                    "source_path": path,
                    "source_line": line_no,
                    "matched_keywords": hits,
                }
                filtered_record = filter_record_for_output(
                    record,
                    include_fields=include_fields,
                    exclude_fields=exclude_fields,
                )
                if isinstance(filtered_record, dict):
                    out_record = dict(filtered_record)
                    out_record["_extract_meta"] = meta
                else:
                    out_record = {"raw": filtered_record, "_extract_meta": meta}

                if out_f is not None:
                    try:
                        out_f.write(json.dumps(out_record, ensure_ascii=False) + "\n")
                    except IOError as e:
                        raise SystemExit(f"Error: Failed to write to output file: {e}")

            if args.max_records and total_records >= args.max_records:
                break
    except KeyboardInterrupt:
        print("\nInterrupted by user.", flush=True)
        return 1
    except Exception as e:
        print(f"Error: Unexpected error during processing: {e}", flush=True)
        return 1
    finally:
        if out_f is not None:
            try:
                out_f.close()
            except IOError as e:
                print(f"Warning: Error closing output file: {e}", flush=True)

    print(f"Scanned files: {scanned_files}")
    print(f"Total records/lines scanned: {total_records}")
    print(f"Matched records/lines: {matched_records}")
    if args.dry_run:
        print("Dry-run: no output written")
    else:
        print(f"Output written to: {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
