"""Synthetic data generator for the retail lakehouse.

Produces realistic, deliberately *messy* source data, the same kind of problems
real pipelines hit in production:

* CDC feeds (customers, products, orders) with duplicate events, out-of-order
  events and late-arriving events from an earlier batch.
* Order items and payments with bad rows: null keys, negative or unparseable
  prices, zero quantities and orphan product ids.
* Clickstream events delivered at-least-once (duplicate event ids).
* Supplier inventory CSVs whose schema drifts: batch 2 adds `warehouse_code`.

Data is split into two arrival batches so the pipeline's incremental behaviour
can be demonstrated: batch 1 covers 1-14 Sep 2026, batch 2 covers 15-21 Sep.
Everything is seeded, so output is identical on every run.

Usage:
    generate-data --out ./landing --batch 1
    generate-data --out ./landing --batch 2
"""

from __future__ import annotations

import argparse
import csv
import json
import random
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path

SEED = 42
START = datetime(2026, 9, 1)
BATCH1_END = datetime(2026, 9, 15)  # exclusive
BATCH2_END = datetime(2026, 9, 22)  # exclusive

FIRST_NAMES = [
    "Olivia",
    "Liam",
    "Aisha",
    "Noah",
    "Mia",
    "Arjun",
    "Chloe",
    "Lucas",
    "Priya",
    "Jack",
    "Zara",
    "Ethan",
    "Ava",
    "Hiroshi",
    "Sofia",
    "Leo",
    "Isla",
    "Ravi",
    "Grace",
    "Mateo",
    "Ruby",
    "Omar",
    "Emily",
    "Kai",
    "Hannah",
    "Wei",
    "Lily",
]
LAST_NAMES = [
    "Smith",
    "Nguyen",
    "Patel",
    "Williams",
    "Brown",
    "Chen",
    "Singh",
    "Jones",
    "Taylor",
    "Kumar",
    "Wilson",
    "Lee",
    "Martin",
    "Ali",
    "Thompson",
    "Garcia",
    "Walker",
    "Kim",
    "White",
    "Murphy",
    "Rossi",
    "Harris",
    "Clarke",
    "Das",
]
CITIES = [
    ("Melbourne", "VIC"),
    ("Sydney", "NSW"),
    ("Brisbane", "QLD"),
    ("Perth", "WA"),
    ("Adelaide", "SA"),
    ("Hobart", "TAS"),
    ("Canberra", "ACT"),
    ("Geelong", "VIC"),
    ("Newcastle", "NSW"),
    ("Gold Coast", "QLD"),
    ("Darwin", "NT"),
]
SEGMENTS = ["consumer", "consumer", "consumer", "small_business", "corporate"]
CATEGORIES = {
    "electronics": (49, 899),
    "home_kitchen": (15, 320),
    "fashion": (20, 180),
    "sports_outdoors": (25, 450),
    "beauty": (8, 95),
    "toys_games": (10, 140),
    "books": (12, 60),
    "garden": (18, 260),
}
ADJECTIVES = ["Classic", "Pro", "Eco", "Smart", "Ultra", "Compact", "Deluxe", "Essential"]
NOUNS = {
    "electronics": ["Headphones", "Speaker", "Monitor", "Keyboard", "Smartwatch", "Charger"],
    "home_kitchen": ["Kettle", "Blender", "Pan Set", "Knife Block", "Toaster", "Mixer"],
    "fashion": ["Jacket", "Sneakers", "Backpack", "Sunglasses", "Hoodie", "Scarf"],
    "sports_outdoors": ["Yoga Mat", "Tent", "Bike Helmet", "Dumbbells", "Water Bottle"],
    "beauty": ["Serum", "Moisturiser", "Shampoo", "Lip Balm", "Sunscreen"],
    "toys_games": ["Puzzle", "Board Game", "Building Set", "Plush Toy", "Card Game"],
    "books": ["Cookbook", "Novel", "Travel Guide", "Workbook", "Biography"],
    "garden": ["Hose", "Planter", "Pruner", "Seed Kit", "Garden Lights"],
}
SUPPLIERS = ["S01", "S02", "S03", "S04", "S05"]
WAREHOUSES = ["MEL1", "SYD1"]
PAYMENT_TYPES = ["credit_card", "credit_card", "credit_card", "paypal", "afterpay", "voucher"]


def _ts(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def _batch_of(dt: datetime) -> int:
    return 1 if dt < BATCH1_END else 2


@dataclass
class World:
    """All events, each tagged with the batch it *arrives* in."""

    customers_cdc: list[dict] = field(default_factory=list)
    products_cdc: list[dict] = field(default_factory=list)
    orders_cdc: list[dict] = field(default_factory=list)
    order_items: list[dict] = field(default_factory=list)
    payments: list[dict] = field(default_factory=list)
    clickstream: list[dict] = field(default_factory=list)
    inventory: list[dict] = field(default_factory=list)


class _Lsn:
    """Monotonic log sequence number, like the LSN in SQL Server CDC."""

    def __init__(self) -> None:
        self.value = 1000

    def next(self) -> int:
        self.value += 1
        return self.value


def build_world(seed: int = SEED, n_customers: int = 400, n_products: int = 120) -> World:
    rng = random.Random(seed)
    lsn = _Lsn()
    w = World()

    # ---------------- customers (CDC, SCD2 source) ----------------
    customers = {}
    for i in range(1, n_customers + 1):
        cid = f"C{i:04d}"
        first, last = rng.choice(FIRST_NAMES), rng.choice(LAST_NAMES)
        city, state = rng.choice(CITIES)
        created = START - timedelta(days=rng.randint(1, 400))
        row = {
            "customer_id": cid,
            "full_name": f"{first} {last}",
            "email": f"{first.lower()}.{last.lower()}{i}@example.com",
            "city": city,
            "state": state,
            "segment": rng.choice(SEGMENTS),
        }
        customers[cid] = row
        w.customers_cdc.append({**row, "op": "I", "change_ts": _ts(created), "lsn": lsn.next(), "_batch": 1})

    # Segment changes during 1-14 Sep; three of these updates arrive late (in batch 2).
    for j, cid in enumerate(rng.sample(sorted(customers), 12)):
        new_seg = rng.choice(sorted(s for s in set(SEGMENTS) if s != customers[cid]["segment"]))
        customers[cid] = {**customers[cid], "segment": new_seg}
        when = START + timedelta(days=rng.randint(2, 12), hours=rng.randint(0, 23))
        late = j < 3
        w.customers_cdc.append({**customers[cid], "op": "U", "change_ts": _ts(when), "lsn": lsn.next(), "_batch": 2 if late else 1})
    # Batch 2 changes: customers move city (SCD2 history).
    for cid in rng.sample(sorted(customers), 30):
        city, state = rng.choice(CITIES)
        customers[cid] = {**customers[cid], "city": city, "state": state}
        when = BATCH1_END + timedelta(days=rng.randint(0, 6), hours=rng.randint(0, 23))
        w.customers_cdc.append({**customers[cid], "op": "U", "change_ts": _ts(when), "lsn": lsn.next(), "_batch": 2})
    # Two customers close their accounts (soft delete).
    for cid in rng.sample(sorted(customers), 2):
        when = BATCH1_END + timedelta(days=rng.randint(1, 5))
        w.customers_cdc.append({**customers[cid], "op": "D", "change_ts": _ts(when), "lsn": lsn.next(), "_batch": 2})

    # ---------------- products (CDC, SCD2 source) ----------------
    products = {}
    for i in range(1, n_products + 1):
        pid = f"P{i:03d}"
        cat = rng.choice(sorted(CATEGORIES))
        lo, hi = CATEGORIES[cat]
        row = {
            "product_id": pid,
            "product_name": f"{rng.choice(ADJECTIVES)} {rng.choice(NOUNS[cat])}",
            "category": cat,
            "list_price": round(rng.uniform(lo, hi), 2),
            "supplier_id": rng.choice(SUPPLIERS),
        }
        products[pid] = row
        w.products_cdc.append({**row, "op": "I", "change_ts": _ts(START - timedelta(days=30)), "lsn": lsn.next(), "_batch": 1})
    # Price changes mid-period: the as-of join in gold must pick the right version.
    for pid in rng.sample(sorted(products), 15):
        new_price = round(products[pid]["list_price"] * rng.choice([0.8, 0.85, 1.1, 1.15]), 2)
        products[pid] = {**products[pid], "list_price": new_price}
        when = START + timedelta(days=rng.randint(8, 18), hours=rng.randint(0, 23))
        w.products_cdc.append({**products[pid], "op": "U", "change_ts": _ts(when), "lsn": lsn.next(), "_batch": _batch_of(when)})

    # Price history lookup for order lines (price at purchase time).
    price_history: dict[str, list[tuple[datetime, float]]] = {}
    for ev in w.products_cdc:
        price_history.setdefault(ev["product_id"], []).append((datetime.strptime(ev["change_ts"], "%Y-%m-%d %H:%M:%S"), ev["list_price"]))
    for v in price_history.values():
        v.sort()

    def price_at(pid: str, when: datetime) -> float:
        price = price_history[pid][0][1]
        for ts, p in price_history[pid]:
            if ts <= when:
                price = p
        return price

    # ---------------- orders, items, payments ----------------
    statuses = ["created", "approved", "shipped", "delivered"]
    cust_ids = sorted(customers)
    prod_ids = sorted(products)
    order_no = 0
    day = START
    while day < BATCH2_END:
        for _ in range(rng.randint(35, 60)):
            order_no += 1
            oid = f"O{order_no:06d}"
            cid = rng.choice(cust_ids)
            purchase = day + timedelta(hours=rng.randint(7, 22), minutes=rng.randint(0, 59))
            batch = _batch_of(purchase)

            # Order lifecycle events.
            cancelled = rng.random() < 0.04
            path = ["created", "canceled"] if cancelled else statuses
            t = purchase
            events = []
            for status in path:
                if status != "created":
                    t = t + timedelta(hours=rng.randint(2, 60))
                if t >= BATCH2_END:
                    break
                events.append(
                    {
                        "order_id": oid,
                        "customer_id": cid,
                        "status": status,
                        "order_purchase_ts": _ts(purchase),
                        "op": "I" if status == "created" else "U",
                        "change_ts": _ts(t),
                        "lsn": lsn.next(),
                        "_batch": _batch_of(t),
                    }
                )
            w.orders_cdc.extend(events)

            # Order lines.
            n_items = rng.choices([1, 2, 3], weights=[60, 30, 10])[0]
            lines_total = 0.0
            for item_no in range(1, n_items + 1):
                pid = rng.choice(prod_ids)
                qty = rng.choices([1, 2, 3], weights=[75, 20, 5])[0]
                unit_price = price_at(pid, purchase)
                freight = round(rng.uniform(0, 15), 2)
                lines_total += qty * unit_price + freight
                w.order_items.append(
                    {
                        "order_id": oid,
                        "order_item_id": item_no,
                        "product_id": pid,
                        "quantity": qty,
                        "unit_price": f"{unit_price:.2f}",
                        "freight_value": f"{freight:.2f}",
                        "_batch": batch,
                    }
                )

            # Payments: sometimes split across two methods (this is what breaks a
            # naive items x payments join, the revenue "fan-out" bug).
            total = round(lines_total, 2)
            if rng.random() < 0.02:
                total = round(total + rng.choice([-5.0, 3.5, 10.0]), 2)  # genuine mismatch
            if rng.random() < 0.15:
                first = round(total * rng.uniform(0.2, 0.6), 2)
                parts = [first, round(total - first, 2)]
            else:
                parts = [total]
            for seq, amount in enumerate(parts, start=1):
                w.payments.append(
                    {
                        "order_id": oid,
                        "payment_sequential": seq,
                        "payment_type": "voucher" if seq == 1 and len(parts) > 1 else rng.choice(PAYMENT_TYPES),
                        "payment_installments": rng.choice([1, 1, 1, 2, 3, 4]),
                        "payment_value": f"{amount:.2f}",
                        "_batch": batch,
                    }
                )

            # Clickstream leading up to the purchase.
            session = f"S{order_no:06d}"
            browse = rng.sample(prod_ids, rng.randint(2, 5))
            et = purchase - timedelta(minutes=rng.randint(5, 40))
            for pid in browse:
                et += timedelta(seconds=rng.randint(20, 240))
                w.clickstream.append(_click(session, cid, pid, "page_view", et))
                if rng.random() < 0.45:
                    et += timedelta(seconds=rng.randint(5, 60))
                    w.clickstream.append(_click(session, cid, pid, "add_to_cart", et))
            w.clickstream.append(_click(session, cid, None, "purchase", purchase))
        # Anonymous browsing sessions that do not convert.
        for k in range(rng.randint(60, 120)):
            session = f"A{day:%m%d}{k:04d}"
            et = day + timedelta(hours=rng.randint(0, 23), minutes=rng.randint(0, 59))
            for pid in rng.sample(prod_ids, rng.randint(1, 4)):
                et += timedelta(seconds=rng.randint(15, 200))
                w.clickstream.append(_click(session, None, pid, "page_view", et))
                if rng.random() < 0.12:
                    w.clickstream.append(_click(session, None, pid, "add_to_cart", et + timedelta(seconds=30)))
        day += timedelta(days=1)

    # ---------------- inject data-quality problems ----------------
    # Bad order lines, all of which must end up in quarantine, not in gold.
    for row in rng.sample(w.order_items, 12):
        row["unit_price"] = f"-{row['unit_price']}"
    for row in rng.sample(w.order_items, 6):
        row["quantity"] = 0
    for row in rng.sample(w.order_items, 5):
        row["unit_price"] = "N/A"
    for row in rng.sample(w.order_items, 4):
        row["product_id"] = "P999"  # orphan: not in the product master
    for row in rng.sample(w.order_items, 3):
        row["order_id"] = ""
    for row in rng.sample(w.payments, 4):
        row["payment_value"] = "error"

    # Duplicate CDC events (the source re-sends rows; same lsn, same data).
    for feed, rate in ((w.orders_cdc, 0.03), (w.customers_cdc, 0.02), (w.products_cdc, 0.02)):
        dups = [dict(r) for r in rng.sample(feed, int(len(feed) * rate))]
        feed.extend(dups)
    # Late-arriving stale order events: an old "approved" event for an order that
    # has already been delivered turns up in batch 2. Latest-state must ignore it.
    for ev in rng.sample([e for e in w.orders_cdc if e["status"] == "approved" and e["_batch"] == 1], 8):
        w.orders_cdc.append({**ev, "_batch": 2})
    # Duplicate order lines re-sent in a later file.
    w.order_items.extend({**r, "_batch": 2} for r in rng.sample([r for r in w.order_items if r["_batch"] == 1], 10))
    # At-least-once clickstream delivery.
    w.clickstream.extend(dict(e) for e in rng.sample(w.clickstream, int(len(w.clickstream) * 0.04)))
    rng.shuffle(w.clickstream)
    rng.shuffle(w.orders_cdc)  # arrival order != event order

    # ---------------- supplier inventory snapshots ----------------
    by_supplier: dict[str, list[str]] = {}
    for pid, p in products.items():
        by_supplier.setdefault(p["supplier_id"], []).append(pid)
    for snap in (datetime(2026, 9, 7), datetime(2026, 9, 14), datetime(2026, 9, 21)):
        drift = snap >= BATCH1_END  # batch 2 files gain a warehouse_code column
        for sup, pids in sorted(by_supplier.items()):
            for pid in sorted(pids):
                warehouses = WAREHOUSES if drift else [None]
                running_low = rng.random() < 0.12  # decided per product, across warehouses
                for wh in warehouses:
                    qty = rng.randint(0, 2) if running_low else max(5, int(rng.gauss(40, 20)))
                    row = {
                        "supplier_id": sup,
                        "product_id": pid,
                        "qty_on_hand": qty,
                        "unit_cost": f"{products[pid]['list_price'] * rng.uniform(0.45, 0.65):.2f}",
                        "snapshot_date": snap.strftime("%Y-%m-%d"),
                        "_batch": _batch_of(snap),
                    }
                    if wh is not None:
                        row["warehouse_code"] = wh
                    w.inventory.append(row)
    return w


_click_counter = {"n": 0}


def _click(session: str, cid: str | None, pid: str | None, etype: str, when: datetime) -> dict:
    _click_counter["n"] += 1
    return {
        "event_id": f"E{_click_counter['n']:08d}",
        "session_id": session,
        "customer_id": cid,
        "product_id": pid,
        "event_type": etype,
        "event_ts": _ts(when),
        "_batch": _batch_of(when),
    }


def _strip(rows: list[dict]) -> list[dict]:
    return [{k: v for k, v in r.items() if k != "_batch"} for r in rows]


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as f:
        for r in rows:
            f.write(json.dumps(r) + "\n")


def _write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    cols: list[str] = []
    for r in rows:
        for k in r:
            if k not in cols:
                cols.append(k)
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=cols)
        writer.writeheader()
        writer.writerows(rows)


def write_batch(out_dir: str | Path, batch: int, seed: int = SEED) -> dict[str, int]:
    """Write one arrival batch of landing files. Returns row counts per source."""
    _click_counter["n"] = 0
    w = build_world(seed)
    out = Path(out_dir)
    pick = lambda rows: _strip([r for r in rows if r["_batch"] == batch])  # noqa: E731

    counts = {}
    for name, rows in (
        ("customers_cdc", w.customers_cdc),
        ("products_cdc", w.products_cdc),
        ("orders_cdc", w.orders_cdc),
        ("clickstream", w.clickstream),
    ):
        selected = pick(rows)
        _write_jsonl(out / name / f"batch_{batch:03d}.json", selected)
        counts[name] = len(selected)
    for name, rows in (("order_items", w.order_items), ("payments", w.payments)):
        selected = pick(rows)
        _write_csv(out / name / f"batch_{batch:03d}.csv", selected)
        counts[name] = len(selected)

    inv = [r for r in w.inventory if r["_batch"] == batch]
    files = {}
    for r in inv:
        files.setdefault((r["supplier_id"], r["snapshot_date"]), []).append(r)
    for (sup, snap), rows in sorted(files.items()):
        _write_csv(out / "inventory" / f"{sup}_{snap}.csv", _strip(rows))
    counts["inventory"] = len(inv)
    return counts


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Generate synthetic retail landing data")
    parser.add_argument("--out", default="./landing", help="Landing folder (local path or /Volumes/... path)")
    parser.add_argument("--batch", type=int, choices=[1, 2], required=True)
    parser.add_argument("--seed", type=int, default=SEED)
    args = parser.parse_args(argv)
    counts = write_batch(args.out, args.batch, args.seed)
    print(f"Wrote batch {args.batch} to {args.out}:")
    for k, v in counts.items():
        print(f"  {k:<15} {v:>6} rows")


if __name__ == "__main__":
    main()
