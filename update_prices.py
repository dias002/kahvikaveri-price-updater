"""
update_prices.py — обновление цен Kahvikaveri (donor_id=8)
Запускается через GitHub Actions. Использует curl_cffi (без Selenium).
"""

import asyncio
import time
import re
import os
import argparse
import mysql.connector
from bs4 import BeautifulSoup
from curl_cffi.requests import AsyncSession

RATIO_PRICE = 1.6
DONOR_ID = 8
CONCURRENCY = 5
SLEEP_BETWEEN = 1.0
BATCH_LOG = 20
RETRIES = 3

DB_CONFIG = {
    "host":     os.environ["DB_HOST"],
    "port":     int(os.environ.get("DB_PORT", 3306)),
    "user":     os.environ["DB_USER"],
    "password": os.environ["DB_PASS"],
    "database": os.environ["DB_NAME"],
}


# ── Парсинг ────────────────────────────────────────────────────────────────────

def _extract_float(text):
    text = text.replace(",", ".")
    m = re.search(r"(\d+[.,]?\d*)", text)
    return float(m.group(1)) if m else 0.0


def parse_price(soup):
    price_info = {"price": 0.0, "price_old": 0.0, "sale_item": "0"}
    price_container = soup.find("p", class_="price")
    if not price_container:
        return price_info
    ins_price = price_container.find("ins")
    if ins_price:
        price_info["price"] = _extract_float(ins_price.get_text(strip=True))
        del_price = price_container.find("del")
        if del_price:
            price_info["price_old"] = _extract_float(del_price.get_text(strip=True))
            price_info["sale_item"] = "1"
    else:
        span = price_container.find("span", class_="woocommerce-Price-amount")
        if span:
            price_info["price"] = _extract_float(span.get_text(strip=True))
    return price_info


def parse_item(html):
    soup = BeautifulSoup(html, "html.parser")
    published = 1 if soup.find("button", {"name": "add-to-cart"}) else 0
    price_info = parse_price(soup) if published else {"price": 0.0, "price_old": 0.0, "sale_item": "0"}
    return published, price_info


# ── HTTP ───────────────────────────────────────────────────────────────────────

async def fetch(session, url):
    for attempt in range(RETRIES):
        try:
            r = await session.get(url, timeout=20, impersonate="chrome120")
            if r.status_code == 200:
                return r.text
            if r.status_code in (403, 429):
                await asyncio.sleep(3 * (attempt + 1))
                continue
            return None
        except Exception:
            await asyncio.sleep(2 * (attempt + 1))
    return None


# ── БД ────────────────────────────────────────────────────────────────────────

def connect_db():
    for attempt in range(5):
        try:
            return mysql.connector.connect(**DB_CONFIG)
        except Exception as e:
            print(f"  DB попытка {attempt+1}/5: {e}")
            time.sleep(5)
    raise RuntimeError("Не удалось подключиться к DB")


def get_items(conn, limit=None, start_id=None):
    query = """
        SELECT id, purchase_url, price, purchase
        FROM shop_items
        WHERE donor_id = %s
          AND purchase_url IS NOT NULL AND purchase_url != ''
    """
    params = [DONOR_ID]
    if start_id:
        query += " AND id >= %s"
        params.append(start_id)
    query += " ORDER BY id ASC"
    if limit:
        query += " LIMIT %s"
        params.append(limit)
    cur = conn.cursor()
    cur.execute(query, params)
    return cur.fetchall()


def update_item(conn, item_id, price, purchase, price_old, sale_item, published):
    cur = conn.cursor()
    cur.execute(
        "UPDATE shop_items SET price=%s, purchase=%s, price_old=%s, sale_item=%s, published=%s WHERE id=%s",
        (price, purchase, price_old, sale_item, published, item_id),
    )
    conn.commit()


def set_unpublished(conn, item_id):
    cur = conn.cursor()
    cur.execute("UPDATE shop_items SET published=0 WHERE id=%s", (item_id,))
    conn.commit()


# ── Главный цикл ───────────────────────────────────────────────────────────────

async def run(items, dry_run, conn):
    total = len(items)
    updated = price_changed = unpublished = errors = 0
    sem = asyncio.Semaphore(CONCURRENCY)

    async with AsyncSession() as session:

        async def process(idx, item_id, purchase_url, old_price, old_purchase):
            nonlocal updated, price_changed, unpublished, errors
            async with sem:
                html = await fetch(session, purchase_url)
                await asyncio.sleep(SLEEP_BETWEEN)

            if not html:
                errors += 1
                print(f"[{idx}/{total}] id={item_id} — не удалось загрузить", flush=True)
                return

            published, price_info = parse_item(html)

            if not published:
                if not dry_run:
                    set_unpublished(conn, item_id)
                unpublished += 1
                print(f"[{idx}/{total}] id={item_id} — снят с публикации", flush=True)
                return

            new_purchase = price_info["price"]
            sale_item = price_info["sale_item"]
            new_price_old_raw = price_info["price_old"]

            if new_purchase <= 0:
                errors += 1
                print(f"[{idx}/{total}] id={item_id} — цена=0, пропускаем", flush=True)
                return

            new_price = round(new_purchase * RATIO_PRICE, 2)
            new_price_old = round(new_price_old_raw * RATIO_PRICE, 2) if sale_item == "1" else 0.0
            changed = abs(float(old_purchase or 0) - new_purchase) > 0.01

            if not dry_run:
                update_item(conn, item_id, new_price, new_purchase, new_price_old, sale_item, published)

            updated += 1
            if changed:
                price_changed += 1
                print(f"[{idx}/{total}] id={item_id}  {old_purchase} → {new_purchase} EUR | продажа: {new_price}", flush=True)
            elif idx % BATCH_LOG == 0:
                print(f"[{idx}/{total}] id={item_id} без изменений ({new_purchase} EUR)", flush=True)

        tasks = [
            process(idx, item_id, url, old_price, old_purchase)
            for idx, (item_id, url, old_price, old_purchase) in enumerate(items, 1)
        ]
        await asyncio.gather(*tasks)

    return updated, price_changed, unpublished, errors


def main(limit=None, dry_run=False, start_id=None):
    conn = connect_db()
    items = get_items(conn, limit, start_id)
    total = len(items)
    print(f"Товаров для обновления: {total}", flush=True)

    updated, price_changed, unpublished, errors = asyncio.run(run(items, dry_run, conn))
    conn.close()

    print(f"\n{'='*55}", flush=True)
    print(f"Готово. Всего: {total} | Обновлено: {updated} | "
          f"Цена изменилась: {price_changed} | Сняты: {unpublished} | Ошибки: {errors}", flush=True)
    print(f"{'='*55}", flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--start-id", type=int, default=None)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    main(limit=args.limit, dry_run=args.dry_run, start_id=args.start_id)
