import pytest

from app.evaluation import score
from app.extraction import PermanentError, extract_document, extract_fields, parse_date
from app.synthetic import generate_batch


def test_parse_date_formats():
    assert parse_date("14/03/2025") == "2025-03-14"       # DD/MM, Indian convention
    assert parse_date("2025-03-14") == "2025-03-14"
    assert parse_date("14 Mar 2025") == "2025-03-14"
    assert parse_date("March 14, 2025") == "2025-03-14"
    assert parse_date("not a date") is None


def test_subtotal_and_tax_are_not_the_total():
    text = "Acme Corp\nInvoice No: INV-1\nSubtotal: $1,000.00\nTotal Tax: $180.00\nGrand Total: $1,180.00\n"
    f = extract_fields(text)
    assert f["total"] == 1180.00 and f["currency"] == "USD"


def test_indian_grouping():
    f = extract_fields("Vendor: X Traders\nInvoice No: A-123\nTotal Amount: ₹1,02,011.50\n")
    assert f["total"] == 102011.50 and f["currency"] == "INR"


def test_invoice_and_due_date_on_one_line_and_separate_lines():
    f = extract_fields("Inv No. A-77    Dated: 01/02/2026\nPayment Due By: 03/03/2026\n")
    assert (f["invoice_date"], f["due_date"]) == ("2026-02-01", "2026-03-03")


def test_vendor_from_header_line_or_label():
    assert extract_fields("Northwind Supplies\nTAX INVOICE\nInvoice No: X-100\n")["vendor"] == "Northwind Supplies"
    assert extract_fields("INVOICE\nFrom: Kaveri Electronics\nInvoice No: X-100\n")["vendor"] == "Kaveri Electronics"


def test_missing_due_date_is_none():
    assert extract_fields("Vendor: A B\nInvoice No: X-100\nInvoice Date: 2026-01-01\n")["due_date"] is None


@pytest.mark.parametrize("blob", [b"", b"short", b"%%CORRUPT%%" + b"\x00" * 30,
                                  b"this document has no invoice fields whatsoever, only prose"])
def test_unprocessable_documents_raise_permanent_error(blob):
    with pytest.raises(PermanentError):
        extract_document(blob, "x.txt")


def test_pdf_extraction():
    reportlab = pytest.importorskip("reportlab.pdfgen.canvas")
    import io
    buf = io.BytesIO()
    c = reportlab.Canvas(buf)
    for i, line in enumerate(["Northwind Supplies", "Invoice No: INV-2026-00042",
                              "Invoice Date: 2026-01-05", "Grand Total: $1,180.00"]):
        c.drawString(72, 750 - 20 * i, line)
    c.save()
    out = extract_document(buf.getvalue(), "inv.pdf")["fields"]
    assert out["invoice_number"] == "INV-2026-00042" and out["total"] == 1180.0


def test_accuracy_guard_on_synthetic_data():
    """Regression guard, not a real-world accuracy claim (the data comes from our own generator)."""
    pairs = [(extract_document(t.encode(), fn)["fields"], truth)
             for fn, t, truth in generate_batch(200, seed=11)]
    assert score(pairs)["field_accuracy"] >= 0.90
