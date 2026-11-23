"""Compare extracted fields against ground truth. Used by scripts/evaluate.py,
the tests, and GET /api/batches/{id}/accuracy."""
import re

from .extraction import FIELDS


def _norm_text(s):
    return re.sub(r"[^a-z0-9]+", " ", str(s).lower()).strip() if s is not None else None


def field_correct(name: str, predicted, truth) -> bool:
    if truth is None or predicted is None:
        return truth is None and predicted is None
    if name == "total":
        return abs(float(predicted) - float(truth)) < 0.01
    if name == "vendor":
        return _norm_text(predicted) == _norm_text(truth)
    return str(predicted).upper() == str(truth).upper()


def score(pairs) -> dict:
    """pairs: iterable of (predicted_fields, truth_fields)."""
    per_field = {f: [0, 0] for f in FIELDS}  # [correct, total]
    docs = exact = 0
    for predicted, truth in pairs:
        docs += 1
        all_ok = True
        for f in FIELDS:
            ok = field_correct(f, predicted.get(f), truth.get(f))
            per_field[f][0] += ok
            per_field[f][1] += 1
            all_ok &= ok
        exact += all_ok
    total_fields = sum(t for _, t in per_field.values())
    correct_fields = sum(c for c, _ in per_field.values())
    return {
        "documents": docs,
        "field_accuracy": round(correct_fields / total_fields, 4) if total_fields else None,
        "document_exact_match": round(exact / docs, 4) if docs else None,
        "per_field": {f: round(c / t, 4) if t else None for f, (c, t) in per_field.items()},
    }
