"""Measure extraction accuracy on a labelled test set. No database or Kafka needed.

    python scripts/evaluate.py                    # 40 synthetic cases (generated once, then reused)
    python scripts/evaluate.py --dir my_cases/    # your own: a folder of .txt/.pdf + labels.json

labels.json maps filename -> {"invoice_number", "invoice_date", "due_date", "vendor", "total", "currency"}
(use null for a field that is genuinely absent). Run this on REAL documents before quoting an
accuracy number anywhere: synthetic data comes from our own generator and flatters the extractor.
"""
import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from app.evaluation import field_correct, score  # noqa: E402
from app.extraction import FIELDS, PermanentError, extract_document  # noqa: E402
from app.synthetic import generate_batch  # noqa: E402

DEFAULT_DIR = ROOT / "tests" / "data" / "cases"


def ensure_default_cases(directory: Path, n: int = 40, seed: int = 2026) -> None:
    if (directory / "labels.json").exists():
        return
    directory.mkdir(parents=True, exist_ok=True)
    labels = {}
    for fn, text, truth in generate_batch(n, seed=seed):
        (directory / fn).write_text(text, encoding="utf-8")
        labels[fn] = truth
    (directory / "labels.json").write_text(json.dumps(labels, indent=2, ensure_ascii=False), encoding="utf-8")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dir", type=Path, default=DEFAULT_DIR)
    ap.add_argument("--verbose", "-v", action="store_true", help="print every wrong field")
    args = ap.parse_args()
    if args.dir == DEFAULT_DIR:
        ensure_default_cases(args.dir)

    labels = json.loads((args.dir / "labels.json").read_text(encoding="utf-8"))
    pairs, rejected = [], []
    for fn, truth in labels.items():
        try:
            fields = extract_document((args.dir / fn).read_bytes(), fn)["fields"]
        except PermanentError as exc:
            fields = {f: None for f in FIELDS}
            rejected.append((fn, str(exc)))
        pairs.append((fields, truth))
        if args.verbose:
            for f in FIELDS:
                if not field_correct(f, fields.get(f), truth.get(f)):
                    print(f"  {fn}: {f}: got {fields.get(f)!r}, expected {truth.get(f)!r}")

    report = score(pairs)
    report["rejected_as_unprocessable"] = len(rejected)
    print(f"documents:            {report['documents']}")
    print(f"field accuracy:       {report['field_accuracy']:.1%}   (correct fields / all fields)")
    print(f"document exact match: {report['document_exact_match']:.1%}   (every field right)")
    for f, v in report["per_field"].items():
        print(f"  {f:<15} {v:.1%}")
    (ROOT / "eval_report.json").write_text(json.dumps(report, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
