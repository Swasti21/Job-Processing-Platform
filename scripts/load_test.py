"""Submit a synthetic batch to a running API and watch it finish; prints throughput + accuracy.

    python scripts/load_test.py --count 5000
    python scripts/load_test.py --count 5000 --url http://localhost:8000
"""
import argparse
import time

import httpx


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default="http://localhost:8000")
    ap.add_argument("--count", type=int, default=5000)
    ap.add_argument("--corrupt-rate", type=float, default=0.02)
    args = ap.parse_args()

    with httpx.Client(base_url=args.url, timeout=120) as c:
        t0 = time.time()
        batch = c.post("/api/batches/synthetic", json={"count": args.count, "corrupt_rate": args.corrupt_rate}).json()
        bid = batch["id"]
        print(f"submitted {args.count} docs in {time.time() - t0:.1f}s  (batch {bid})")
        t1 = time.time()
        while True:
            b = c.get(f"/api/batches/{bid}").json()
            print(f"\r{b['completed_count']:>6} completed  {b['failed_count']:>4} failed  {b['progress']:>6.1%}", end="", flush=True)
            if b["status"] == "DONE":
                break
            time.sleep(1)
        elapsed = time.time() - t1
        print(f"\nfinished in {elapsed:.1f}s  ->  {args.count / elapsed:.0f} docs/sec")
        print("status counts:", c.get(f"/api/batches/{bid}/status-counts").json())
        acc = c.get(f"/api/batches/{bid}/accuracy")
        if acc.status_code == 200:
            print("accuracy:", acc.json())


if __name__ == "__main__":
    main()
