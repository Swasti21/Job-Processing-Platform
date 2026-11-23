"""Rule-based invoice field extraction (regex + normalisation).

Deliberately simple: no ML, no external calls. The platform is the interesting part;
swap ``extract_fields`` for an LLM / OCR call later without touching anything else.
"""
import io
import re
from datetime import datetime

FIELDS = ("invoice_number", "invoice_date", "due_date", "vendor", "total", "currency")


class PermanentError(Exception):
    """Document can never be processed (corrupt / unreadable). Skips retries, goes to the DLQ."""


# ---------------------------------------------------------------- text loading
def load_text(data: bytes, filename: str = "") -> str:
    if filename.lower().endswith(".pdf") or data[:4] == b"%PDF":
        try:
            from pypdf import PdfReader
            reader = PdfReader(io.BytesIO(data))
            return "\n".join((page.extract_text() or "") for page in reader.pages)
        except Exception as exc:  # noqa: BLE001
            raise PermanentError(f"unreadable PDF: {exc}") from exc
    return data.decode("utf-8", errors="replace")


# ---------------------------------------------------------------- normalisers
_DATE_FORMATS = ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%d.%m.%Y", "%d %b %Y", "%d %B %Y",
                 "%b %d, %Y", "%B %d, %Y", "%d-%b-%Y", "%d-%B-%Y")
# Numeric dates are read as DD/MM/YYYY (Indian convention).


def parse_date(raw: str) -> str | None:
    raw = re.sub(r"\s+", " ", raw.strip().rstrip(".,")).replace("Sept", "Sep")
    for fmt in _DATE_FORMATS:
        try:
            return datetime.strptime(raw, fmt).date().isoformat()
        except ValueError:
            continue
    return None


def parse_amount(raw: str) -> float | None:
    try:
        return round(float(raw.replace(",", "")), 2)
    except ValueError:
        return None


_CURRENCY = {"₹": "INR", "rs": "INR", "inr": "INR", "$": "USD", "usd": "USD",
             "€": "EUR", "eur": "EUR", "£": "GBP", "gbp": "GBP"}
_CURRENCY_RE = re.compile(r"(₹|\bRs\.?|\bINR\b|\$|\bUSD\b|€|\bEUR\b|£|\bGBP\b)", re.I)

# ---------------------------------------------------------------- patterns
_DATE_TOKEN = (r"(\d{4}-\d{2}-\d{2}"
               r"|\d{1,2}[/.\-]\d{1,2}[/.\-]\d{4}"
               r"|\d{1,2}[ \-][A-Za-z]{3,9}[ \-]\d{4}"
               r"|[A-Za-z]{3,9}\.? \d{1,2}, \d{4})")
_DATE_LABELED = re.compile(
    r"(?P<label>due\s*date|payment\s*due(?:\s*by)?|invoice\s*date|inv\.?\s*date|issue\s*date|"
    r"issued(?:\s*on)?|dated?)\s*[:\-]?\s*" + _DATE_TOKEN, re.I)

_INVOICE_NO = re.compile(
    r"(?:invoice\s*(?:no|number|num|#)|inv\s*(?:no|#)|bill\s*(?:no|number))\.?\s*[:#\-]?\s*"
    r"([A-Z0-9][A-Z0-9\-/]{2,24})", re.I)

_VENDOR_LABELED = re.compile(
    r"^\s*(?:vendor|supplier|seller|billed\s*by|from)\s*[:\-]\s*(.+?)\s*$", re.I | re.M)

_AMOUNT = r"((?:\d{1,3}(?:,\d{2,3})+|\d+)(?:\.\d{1,2})?)"
_CUR_OPT = r"(?:₹|Rs\.?|INR|\$|USD|€|EUR|£|GBP)?"
# Highest priority first. (?<!sub) keeps "Subtotal" out of the plain "total" pattern.
_TOTAL_PATTERNS = [re.compile(p, re.I | re.M) for p in (
    r"grand\s*total\s*[:\-]?\s*" + _CUR_OPT + r"\s*" + _AMOUNT,
    r"(?:total\s*due|amount\s*due|balance\s*due|total\s*payable)\s*[:\-]?\s*" + _CUR_OPT + r"\s*" + _AMOUNT,
    r"total\s*amount\s*[:\-]?\s*" + _CUR_OPT + r"\s*" + _AMOUNT,
    r"(?<!sub)(?<!sub )\btotal\s*[:\-]?\s*" + _CUR_OPT + r"\s*" + _AMOUNT,
)]

_NOT_VENDOR = re.compile(r"^(tax\s*)?invoice\b|^bill\b|^page\b|^\W*$", re.I)


# ---------------------------------------------------------------- extraction
def extract_fields(text: str) -> dict:
    out: dict = {f: None for f in FIELDS}

    m = _INVOICE_NO.search(text)
    if m:
        out["invoice_number"] = m.group(1).upper()

    for m in _DATE_LABELED.finditer(text):
        label = re.sub(r"\s+", " ", m.group("label").lower())
        iso = parse_date(m.group(2))
        if iso is None:
            continue
        if "due" in label:
            out["due_date"] = out["due_date"] or iso
        else:
            out["invoice_date"] = out["invoice_date"] or iso

    vm = _VENDOR_LABELED.search(text)
    if vm:
        out["vendor"] = vm.group(1).strip()
    else:
        for line in text.splitlines():
            line = line.strip()
            if line and not _NOT_VENDOR.search(line):
                out["vendor"] = line
                break

    for pattern in _TOTAL_PATTERNS:
        found = pattern.findall(text)
        if found:
            out["total"] = parse_amount(found[-1])
            break

    cm = _CURRENCY_RE.search(text)
    if cm:
        out["currency"] = _CURRENCY.get(cm.group(1).lower().rstrip("."))
    return out


def extract_document(data: bytes, filename: str = "") -> dict:
    """Entry point used by the worker. Returns {"fields": {...}, "confidence": float}."""
    text = load_text(data, filename)
    if len(text.strip()) < 20 or "%%CORRUPT%%" in text:
        raise PermanentError("document is empty or corrupt")
    fields = extract_fields(text)
    found = [k for k in ("invoice_number", "invoice_date", "vendor", "total", "currency") if fields[k] is not None]
    # The vendor fallback (first text line) will "find" something in any prose, so it is not
    # evidence of an invoice on its own: require at least one label-anchored field.
    if not any(fields[k] is not None for k in ("invoice_number", "invoice_date", "total")):
        raise PermanentError("no invoice fields could be extracted")
    # due_date is optional on real invoices, so confidence is over the 5 required fields.
    confidence = round(len(found) / 5, 2)
    return {"fields": fields, "confidence": confidence}
