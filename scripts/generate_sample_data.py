"""Generate small sample CSVs with deliberately bad records so the DQ framework has something to catch.

Usage:  python scripts/generate_sample_data.py --out data/landing
Then upload the three sub-folders (customers/, products/, orders/) to your Databricks Volume.
Run again with --batch 2 to create a second orders file and demo incremental loading.
"""
import argparse
import csv
import random
from datetime import date, timedelta
from pathlib import Path

CATEGORIES = ["Electronics", "Grocery", "Fashion", "Home", "Sports"]
COUNTRIES = ["IN", "US", "GB", "DE", "SG"]
STATUSES = ["PLACED", "SHIPPED", "DELIVERED", "CANCELLED"]


def write(path, header, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)
    print(f"wrote {len(rows):>6} rows -> {path}")


def customers(n, rnd):
    rows = []
    for i in range(1, n + 1):
        email = f"user{i}@example.com"
        country = rnd.choice(COUNTRIES)
        name = f"Customer {i}"
        signup = date(2023, 1, 1) + timedelta(days=rnd.randint(0, 700))
        r = rnd.random()
        if r < 0.03:
            email = f"user{i}@invalid"          # bad email format -> quarantine
        elif r < 0.05:
            name = ""                            # missing name -> quarantine
        elif r < 0.07:
            country = "ZZ"                       # unexpected value -> warning only
        rows.append([f"C{i:05d}", name, email, country, signup.isoformat()])
    return rows


def products(n, rnd):
    rows = []
    for i in range(1, n + 1):
        price = round(rnd.uniform(2, 500), 2)
        if rnd.random() < 0.04:
            price = -1                           # bad price -> quarantine
        rows.append([f"P{i:04d}", f"Product {i}", rnd.choice(CATEGORIES), price])
    return rows


def orders(n, n_customers, n_products, rnd, start_id):
    rows = []
    for i in range(start_id, start_id + n):
        cust = f"C{rnd.randint(1, n_customers):05d}"
        prod = f"P{rnd.randint(1, n_products):04d}"
        qty = rnd.randint(1, 5)
        amount = round(qty * rnd.uniform(2, 500), 2)
        d = (date(2025, 1, 1) + timedelta(days=rnd.randint(0, 300))).isoformat()
        status = rnd.choice(STATUSES)
        r = rnd.random()
        if r < 0.02:
            cust = ""                            # missing customer
        elif r < 0.04:
            amount = -abs(amount)                # negative amount
        elif r < 0.06:
            d = "31/02/2025"                     # unparseable date
        elif r < 0.08:
            cust = "C99999"                      # customer does not exist (referential)
        elif r < 0.10:
            status = "UNKNOWN"                   # not in allowed list
        elif r < 0.12:
            prod = "P9999"                       # unknown product -> warning only
        elif r < 0.14:
            qty = "abc"                          # wrong type
        rows.append([f"O{i:07d}", cust, prod, d, qty, amount, status])
    # inject a few duplicate order ids
    rows.extend(rnd.sample(rows, k=max(1, n // 100)))
    return rows


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="data/landing")
    ap.add_argument("--orders", type=int, default=5000)
    ap.add_argument("--batch", type=int, default=1)
    a = ap.parse_args()
    rnd = random.Random(42 + a.batch)
    out = Path(a.out)
    n_c, n_p = 500, 50
    if a.batch == 1:
        write(out / "customers" / "customers_1.csv",
              ["CustomerID", "FullName", "Email", "Country", "SignupDate"], customers(n_c, rnd))
        write(out / "products" / "products_1.csv",
              ["ProductID", "ProductName", "Category", "UnitPrice"], products(n_p, rnd))
    write(out / "orders" / f"orders_{a.batch}.csv",
          ["OrderID", "CustomerID", "ProductID", "OrderDate", "Quantity", "Amount", "Status"],
          orders(a.orders, n_c, n_p, rnd, start_id=1 + (a.batch - 1) * a.orders))
