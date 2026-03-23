"""
Hourly update: fetch latest prices from real-time API and update
prices_latest.json and metadata.json.
Runs in GitHub Actions from the repo root (web/data/public/).
"""

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import json
import os
from datetime import datetime

BASE_URL = "https://sedeaplicaciones.minetur.gob.es/ServiciosRESTCarburantes/PreciosCarburantes"
PRODUCTS = {1: "g95", 3: "g98", 4: "gA"}

session = requests.Session()
session.mount("https://", HTTPAdapter(max_retries=Retry(
    total=4, backoff_factor=2, status_forcelist=[500, 502, 503, 504]
)))


def fetch_product(product_id):
    url = f"{BASE_URL}/EstacionesTerrestres/FiltroProducto/{product_id}"
    resp = session.get(url, timeout=60)
    resp.raise_for_status()
    return resp.json()


def parse_decimal(value):
    if not value or not isinstance(value, str):
        return None
    try:
        return round(float(value.strip().replace(",", ".")), 3)
    except ValueError:
        return None


def main():
    # Fetch all 3 products
    all_prices = {}  # ideess -> {g95, g98, gA, fecha}
    api_fecha = None

    for pid, key in PRODUCTS.items():
        print(f"Fetching product {pid} ({key})...")
        data = fetch_product(pid)
        stations = data.get("ListaEESSPrecio", [])
        api_fecha = data.get("Fecha", "")
        print(f"  {len(stations)} stations")

        for s in stations:
            ideess = s.get("IDEESS", "").strip()
            if not ideess:
                continue
            price = parse_decimal(s.get("PrecioProducto", ""))
            if ideess not in all_prices:
                all_prices[ideess] = {"g95": None, "g98": None, "gA": None}
            all_prices[ideess][key] = price

    # Build prices_latest.json: {id: [g95, g98, gA, fecha_str]}
    # Parse API date for the fecha field
    fecha_str = ""
    if api_fecha:
        try:
            dt = datetime.strptime(api_fecha.split(".")[0], "%d/%m/%Y %H:%M:%S")
            fecha_str = dt.strftime("%Y-%m-%d")
        except ValueError:
            fecha_str = datetime.utcnow().strftime("%Y-%m-%d")

    prices_latest = {}
    for ideess, p in all_prices.items():
        prices_latest[ideess] = [p["g95"], p["g98"], p["gA"], fecha_str]

    with open("prices_latest.json", "w", encoding="utf-8") as f:
        json.dump(prices_latest, f, ensure_ascii=False)
    print(f"prices_latest.json: {len(prices_latest)} stations")

    # Update metadata.json
    meta_path = "metadata.json"
    if os.path.exists(meta_path):
        with open(meta_path, "r", encoding="utf-8") as f:
            metadata = json.load(f)
    else:
        metadata = {}

    metadata["ultima_fecha_datos"] = fecha_str
    metadata["actualizado"] = datetime.utcnow().strftime("%Y-%m-%dT%H:%M")
    metadata["n_estaciones"] = len(prices_latest)

    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(metadata, f, ensure_ascii=False)
    print(f"metadata.json updated: {metadata['actualizado']}")


if __name__ == "__main__":
    main()
