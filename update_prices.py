"""
update_prices.py — обновление цен Kahvikaveri (donor_id=8)
Запускается через GitHub Actions.
"""

import time
import random
import re
import os
import argparse
import mysql.connector
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException

RATIO_PRICE = 1.6
DONOR_ID = 8
SLEEP_BETWEEN = 2.0
CF_WAIT_TIMEOUT = 25
BATCH_LOG = 10

DB_CONFIG = {
    "host":     os.environ["DB_HOST"],
    "port":     int(os.environ.get("DB_PORT", 3306)),
    "user":     os.environ["DB_USER"],
    "password": os.environ["DB_PASS"],
    "database": os.environ["DB_NAME"],
}


# ── Парсинг цены ───────────────────────────────────────────────────────────────

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


# ── Chrome ─────────────────────────────────────────────────────────────────────

def make_driver():
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument(
        "user-agent=Mozilla/5.0 (X11; Linux x86_64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)
    driver = webdriver.Chrome(options=options)
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    return driver


def get_page(driver, url):
    driver.get(url)
    try:
        WebDriverWait(driver, CF_WAIT_TIMEOUT).until(
            lambda d: "Just a moment" not in d.title and d.title.strip() != ""
        )
    except TimeoutException:
        if "Just a moment" in driver.title:
            print(f"  CF timeout: {url}")
            return None
    time.sleep(random.uniform(1.0, 2.5))
    return BeautifulSoup(driver.page_source, "html.parser")


# ── База данных ────────────────────────────────────────────────────────────────

def connect_db():
    for attempt in range(5):
        try:
            conn = mysql.connector.connect(**DB_CONFIG)
            return conn
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

def main(limit=None, dry_run=False, start_id=None):
    conn = connect_db()
    items = get_items(conn, limit, start_id)
    total = len(items)
    print(f"Товаров для обновления: {total}", flush=True)

    updated = price_changed = unpublished = errors = 0
    driver = make_driver()

    try:
        for idx, (item_id, purchase_url, old_price, old_purchase) in enumerate(items, 1):
            try:
                soup = get_page(driver, purchase_url)

                if not soup:
                    try:
                        driver.quit()
                    except Exception:
                        pass
                    time.sleep(5)
                    driver = make_driver()
                    errors += 1
                    continue

                published = 1 if soup.find("button", {"name": "add-to-cart"}) else 0

                if not published:
                    if not dry_run:
                        set_unpublished(conn, item_id)
                    unpublished += 1
                    print(f"[{idx}/{total}] id={item_id} — снят с публикации", flush=True)
                    time.sleep(SLEEP_BETWEEN)
                    continue

                price_info = parse_price(soup)
                new_purchase = price_info["price"]
                sale_item = price_info["sale_item"]
                new_price_old_raw = price_info["price_old"]

                if new_purchase <= 0:
                    print(f"[{idx}/{total}] id={item_id} — цена=0, пропускаем", flush=True)
                    errors += 1
                    time.sleep(SLEEP_BETWEEN)
                    continue

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

            except Exception as e:
                err = str(e)
                print(f"[{idx}/{total}] id={item_id} ошибка: {err[:120]}", flush=True)
                errors += 1
                if any(x in err for x in ["invalid session id", "no such window", "chrome not reachable"]):
                    print("  ⟳ Chrome упал, пересоздаём...", flush=True)
                    try:
                        driver.quit()
                    except Exception:
                        pass
                    time.sleep(3)
                    driver = make_driver()
                elif any(x in err for x in ["MySQL Connection not available", "Lost connection", "not connected"]):
                    print("  ⟳ MySQL упал, переподключаемся...", flush=True)
                    conn = connect_db()

            time.sleep(SLEEP_BETWEEN)

    finally:
        try:
            driver.quit()
        except Exception:
            pass
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
