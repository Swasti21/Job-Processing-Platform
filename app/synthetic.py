"""Synthetic invoice generator with ground-truth labels.

Gives you 5,000 documents for the load demo and labelled cases for the accuracy check
without needing real data. Layouts, date/amount formats and label wording vary, and a
small share of documents get OCR-style typos in labels so extraction is not trivially perfect.
"""
import random
from datetime import date, timedelta

VENDORS = [
    "Sharma Traders Pvt Ltd", "Northwind Supplies", "Acme Industrial Corp", "Bluepeak Logistics LLP",
    "Greenfield Agro Pvt Ltd", "Kaveri Electronics", "Orbit Office Solutions", "Delta Fabrication Works",
    "Himalaya Paper Mills", "Zenith Software Services", "Lotus Packaging Co", "Ridgeway Hardware Ltd",
    "Sunrise Foods Pvt Ltd", "Metro Tools & Dies", "Crescent Textiles", "Pioneer Chemicals Ltd",
]
CUSTOMERS = ["Apex Retail Pvt Ltd", "Horizon Healthcare", "Summit Builders", "Nova Consulting LLP"]
ITEMS = ["Steel bracket", "Copper wire 2mm", "Packaging boxes", "Printer toner", "Safety gloves",
         "LED panel", "Hydraulic hose", "Cleaning supplies", "Cable ties", "Office chairs"]
CURRENCIES = [("₹", "INR", True), ("Rs.", "INR", True), ("$", "USD", False), ("€", "EUR", False)]
DATE_FORMATS = ["%d/%m/%Y", "%Y-%m-%d", "%d %b %Y", "%B %d, %Y", "%d-%m-%Y"]
OCR_SWAPS = {"I": "l", "o": "0", "l": "1", "e": "c", "a": "o"}


def _indian_group(n: float) -> str:
    whole, frac = f"{n:.2f}".split(".")
    if len(whole) > 3:
        head, tail = whole[:-3], whole[-3:]
        parts = []
        while len(head) > 2:
            parts.insert(0, head[-2:])
            head = head[:-2]
        if head:
            parts.insert(0, head)
        whole = ",".join(parts + [tail])
    return f"{whole}.{frac}"


def _fmt_amount(n: float, indian: bool) -> str:
    return _indian_group(n) if indian else f"{n:,.2f}"


def _ocr_noise(label: str, rng: random.Random) -> str:
    idxs = [i for i, ch in enumerate(label) if ch in OCR_SWAPS]
    if not idxs:
        return label
    i = rng.choice(idxs)
    return label[:i] + OCR_SWAPS[label[i]] + label[i + 1:]


def generate_invoice(rng: random.Random, noise_rate: float = 0.10) -> tuple[str, dict]:
    vendor = rng.choice(VENDORS)
    inv_no = f"INV-{rng.randint(2024, 2026)}-{rng.randint(1, 99999):05d}"
    inv_date = date(2025, 1, 1) + timedelta(days=rng.randint(0, 600))
    due_date = inv_date + timedelta(days=rng.choice([15, 30, 45, 60])) if rng.random() > 0.15 else None
    sym, code, indian = rng.choice(CURRENCIES)
    fmt = rng.choice(DATE_FORMATS)

    lines, sub = [], 0.0
    for _ in range(rng.randint(2, 4)):
        qty, price = rng.randint(1, 20), rng.randint(50, 5000)
        sub += qty * price
        lines.append(f"{rng.choice(ITEMS):<20} {qty:>3} x {_fmt_amount(price, indian):>12}  {_fmt_amount(qty * price, indian):>14}")
    tax = round(sub * 0.18, 2)
    total = round(sub + tax, 2)

    def money(x):
        return f"{sym}{_fmt_amount(x, indian)}" if sym != "Rs." else f"Rs. {_fmt_amount(x, indian)}"

    d = inv_date.strftime(fmt)
    dd = due_date.strftime(fmt) if due_date else None
    layout = rng.randrange(4)
    L = {"inv": "Invoice No", "date": "Invoice Date", "due": "Due Date", "total": "Grand Total"}

    if layout == 0:
        out = [vendor, "TAX INVOICE", f"{{inv}}: {inv_no}", f"{{date}}: {d}"]
        if dd:
            out.append(f"{{due}}: {dd}")
        out += ["", f"Bill To: {rng.choice(CUSTOMERS)}", "", *lines, "",
                f"Subtotal: {money(sub)}", f"GST (18%): {money(tax)}", f"{{total}}: {money(total)}"]
    elif layout == 1:
        L.update(inv="INVOICE #", date="Date", total="Total Amount")
        out = [f"{{inv}} {inv_no}", f"From: {vendor}", f"Bill To: {rng.choice(CUSTOMERS)}", f"{{date}}: {d}"]
        if dd:
            out.append(f"{{due}}: {dd}")
        out += ["", *lines, "", f"Subtotal {money(sub)}", f"Tax {money(tax)}", f"{{total}}: {money(total)}"]
    elif layout == 2:
        L.update(inv="Inv No.", date="Dated", due="Payment Due By", total="Total Due")
        out = [f"Supplier: {vendor}", f"{{inv}} {inv_no}    {{date}}: {d}"]
        if dd:
            out.append(f"{{due}}: {dd}")
        out += ["", *lines, "", f"Sub-total: {money(sub)}", f"VAT: {money(tax)}", f"{{total}}: {money(total)}"]
    else:
        L.update(inv="Bill Number", total="Amount Due")
        out = ["INVOICE", f"Vendor: {vendor}", f"{{inv}}: {inv_no}", f"{{date}}: {d}"]
        if dd:
            out.append(f"{{due}}: {dd}")
        out += ["", *lines, "", f"Subtotal: {money(sub)}", f"Taxes: {money(tax)}", f"{{total}}: {money(total)}"]

    if rng.random() < noise_rate:  # simulate an OCR typo in one label
        key = rng.choice(list(L))
        L[key] = _ocr_noise(L[key], rng)
    text = "\n".join(out).format(**L) + "\n"

    truth = {"invoice_number": inv_no, "invoice_date": inv_date.isoformat(),
             "due_date": due_date.isoformat() if due_date else None,
             "vendor": vendor, "total": total, "currency": code}
    return text, truth


def generate_corrupt(rng: random.Random) -> tuple[str, None]:
    return "%%CORRUPT%%" + "\x00" * rng.randint(5, 40), None


def generate_batch(count: int, seed: int | None = None, corrupt_rate: float = 0.0, noise_rate: float = 0.10):
    """Yield (filename, text, truth_or_None)."""
    rng = random.Random(seed)
    for i in range(count):
        if rng.random() < corrupt_rate:
            text, truth = generate_corrupt(rng)
        else:
            text, truth = generate_invoice(rng, noise_rate)
        yield f"invoice_{i + 1:05d}.txt", text, truth
