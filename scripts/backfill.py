"""
Backfill missing historical days into provincial CSVs and rebuild stats.
Usage: python scripts/backfill.py [--from YYYY-MM-DD] [--to YYYY-MM-DD]
Defaults: from = day after last date in madrid.csv, to = yesterday
"""

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import json
import os
import re
import csv
import sys
import unicodedata
from datetime import datetime, timedelta, date
import time

import pandas as pd
import duckdb

BASE_URL = "https://sedeaplicaciones.minetur.gob.es/ServiciosRESTCarburantes/PreciosCarburantes"
PRODUCTS = {1: "gasolina_95", 3: "gasolina_98", 4: "gasoleo_a"}
PRICES_DIR = "prices"
REF_DATE = "2026-02-28"

session = requests.Session()
session.headers.update({
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36'
})
session.mount("https://", HTTPAdapter(max_retries=Retry(
    total=8, backoff_factor=3, status_forcelist=[500, 502, 503, 504],
    allowed_methods=["GET"],
)))


def parse_decimal(value):
    if not value or not isinstance(value, str):
        return None
    try:
        return round(float(value.strip().replace(",", ".")), 3)
    except ValueError:
        return None


def safe_filename(provincia):
    safe = unicodedata.normalize("NFD", provincia.lower())
    safe = "".join(c for c in safe if unicodedata.category(c) != "Mn")
    return re.sub(r"[^a-z0-9]+", "_", safe).strip("_") + ".csv"


def fetch_day(day):
    """Fetch all 3 products for a given date, return dict {ideess: {provincia, g95, g98, gA}}."""
    combined = {}
    for pid, name in PRODUCTS.items():
        url = f"{BASE_URL}/EstacionesTerrestresHist/FiltroProducto/{day.strftime('%d-%m-%Y')}/{pid}"
        print(f"  Fetching {name} for {day}...", end=" ")
        try:
            resp = session.get(url, timeout=60)
            resp.raise_for_status()
            data = resp.json()
            stations = data.get("ListaEESSPrecio", [])
            print(f"{len(stations)} stations")
        except Exception as e:
            print(f"ERROR: {e}")
            continue

        col = {"gasolina_95": "g95", "gasolina_98": "g98", "gasoleo_a": "gA"}[name]
        for s in stations:
            ideess = s.get("IDEESS", "").strip()
            if not ideess:
                continue
            if ideess not in combined:
                combined[ideess] = {
                    "provincia": s.get("Provincia", "").strip().upper(),
                    "g95": None, "g98": None, "gA": None,
                }
            combined[ideess][col] = parse_decimal(s.get("PrecioProducto", ""))

    return combined


def find_missing_dates():
    """Find dates missing from madrid.csv (representative province)."""
    madrid_path = os.path.join(PRICES_DIR, "madrid.csv")
    if not os.path.exists(madrid_path):
        print("ERROR: madrid.csv not found")
        sys.exit(1)

    existing = set()
    with open(madrid_path, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            existing.add(row["fecha"])

    first = min(existing)
    yesterday = date.today() - timedelta(days=1)
    d = date.fromisoformat(first)
    missing = []
    while d <= yesterday:
        if str(d) not in existing:
            missing.append(d)
        d += timedelta(days=1)
    return missing


def append_day_to_csvs(day, combined, province_files):
    """Append one day's data to provincial CSVs."""
    day_str = str(day)
    by_province = {}
    for ideess, info in combined.items():
        prov = info["provincia"]
        if prov not in by_province:
            by_province[prov] = []
        by_province[prov].append({
            "fecha": day_str,
            "id": ideess,
            "g95": info["g95"],
            "g98": info["g98"],
            "gA": info["gA"],
        })

    for prov, rows in by_province.items():
        filename = province_files.get(prov) or safe_filename(prov)
        province_files[prov] = filename
        filepath = os.path.join(PRICES_DIR, filename)

        new_df = pd.DataFrame(rows)

        if os.path.exists(filepath):
            existing = pd.read_csv(filepath, dtype=str)
            existing = existing[existing["fecha"] != day_str]
            combined_df = pd.concat([existing, new_df.astype(str)], ignore_index=True)
        else:
            combined_df = new_df.astype(str)

        combined_df.to_csv(filepath, index=False)

    return len(combined)


def rebuild_stats(province_files):
    """Rebuild provincial_stats.json and metadata.json from all CSVs."""
    print("\nRebuilding provincial_stats.json...")
    all_frames = []
    for prov, filename in province_files.items():
        filepath = os.path.join(PRICES_DIR, filename)
        if os.path.exists(filepath):
            df = pd.read_csv(filepath)
            df["provincia"] = prov
            for col in ["g95", "g98", "gA"]:
                df[col] = pd.to_numeric(df[col], errors="coerce")
            all_frames.append(df)

    if not all_frames:
        print("No data to rebuild stats.")
        return

    all_df = pd.concat(all_frames, ignore_index=True)
    con = duckdb.connect()
    con.register("prices", all_df)

    stats_query = """
    SELECT fecha, provincia,
        COUNT(*) AS n,
        ROUND(AVG(g95), 4) AS media_g95, ROUND(MIN(g95), 4) AS min_g95, ROUND(MAX(g95), 4) AS max_g95,
        ROUND(AVG(g98), 4) AS media_g98, ROUND(MIN(g98), 4) AS min_g98, ROUND(MAX(g98), 4) AS max_g98,
        ROUND(AVG(gA), 4) AS media_gA, ROUND(MIN(gA), 4) AS min_gA, ROUND(MAX(gA), 4) AS max_gA
    FROM prices
    WHERE g95 IS NOT NULL OR g98 IS NOT NULL OR gA IS NOT NULL
    GROUP BY fecha, provincia ORDER BY fecha, provincia
    """
    stats_df = con.execute(stats_query).fetchdf()
    records = stats_df.to_dict(orient="records")

    by_prov = {}
    for r in records:
        p = r.pop("provincia")
        if p not in by_prov:
            by_prov[p] = []
        by_prov[p].append({k: (v if v == v else None) for k, v in r.items()})

    with open("provincial_stats.json", "w", encoding="utf-8") as f:
        json.dump(by_prov, f, ensure_ascii=False)
    print(f"  provincial_stats.json: {len(by_prov)} provinces")

    # Update metadata
    latest_date = str(con.execute("SELECT MAX(fecha) FROM prices").fetchone()[0])
    ref_query = f"""
    SELECT ROUND(MEDIAN(g95),4), ROUND(MEDIAN(g98),4), ROUND(MEDIAN(gA),4)
    FROM prices WHERE fecha = '{REF_DATE}'
    """
    hoy_query = f"""
    SELECT ROUND(MEDIAN(g95),4), ROUND(MEDIAN(g98),4), ROUND(MEDIAN(gA),4)
    FROM prices WHERE fecha = '{latest_date}'
    """
    ref = con.execute(ref_query).fetchone()
    hoy = con.execute(hoy_query).fetchone()

    meta_path = "metadata.json"
    metadata = {}
    if os.path.exists(meta_path):
        with open(meta_path, "r") as f:
            metadata = json.load(f)

    metadata["ultima_fecha_datos"] = latest_date
    metadata["n_dias"] = int(con.execute("SELECT COUNT(DISTINCT fecha) FROM prices").fetchone()[0])
    metadata["fecha_referencia"] = REF_DATE

    if ref and hoy and ref[0] and hoy[0]:
        metadata["var_mediana"] = {
            "g95": round((hoy[0] - ref[0]) / ref[0] * 100, 1),
            "g98": round((hoy[1] - ref[1]) / ref[1] * 100, 1),
            "gA": round((hoy[2] - ref[2]) / ref[2] * 100, 1),
        }
        metadata["mediana_referencia"] = {"g95": ref[0], "g98": ref[1], "gA": ref[2]}
        metadata["mediana_actual"] = {"g95": hoy[0], "g98": hoy[1], "gA": hoy[2]}

    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(metadata, f, ensure_ascii=False)
    print(f"  metadata.json updated: {metadata.get('n_dias')} days, latest={latest_date}")

    con.close()


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Backfill missing historical days")
    parser.add_argument("--from", dest="from_date", type=str, help="Start date YYYY-MM-DD")
    parser.add_argument("--to", dest="to_date", type=str, help="End date YYYY-MM-DD")
    args = parser.parse_args()

    # Load province mapping
    mapping_path = "province_files.json"
    if os.path.exists(mapping_path):
        with open(mapping_path, "r", encoding="utf-8") as f:
            province_files = json.load(f)
    else:
        province_files = {}

    # Determine dates to backfill
    if args.from_date and args.to_date:
        start = date.fromisoformat(args.from_date)
        end = date.fromisoformat(args.to_date)
        missing = []
        d = start
        while d <= end:
            missing.append(d)
            d += timedelta(days=1)
    else:
        missing = find_missing_dates()

    if not missing:
        print("No missing dates found!")
        return

    print(f"Days to backfill: {len(missing)}")
    print(f"  From: {missing[0]}")
    print(f"  To:   {missing[-1]}")
    print()

    # Fetch and append each day
    for day in missing:
        print(f"\n{'='*50}")
        print(f"Processing {day}")
        print(f"{'='*50}")
        combined = fetch_day(day)
        if combined:
            n = append_day_to_csvs(day, combined, province_files)
            print(f"  -> {n} stations across provinces")
        else:
            print(f"  → No data returned for {day}")

        # Save province mapping after each day
        with open(mapping_path, "w", encoding="utf-8") as f:
            json.dump(province_files, f, ensure_ascii=False)

        # Brief pause between days
        time.sleep(2)

    # Rebuild stats from all CSVs
    rebuild_stats(province_files)

    print(f"\n{'='*50}")
    print(f"DONE: Backfilled {len(missing)} days")
    print(f"{'='*50}")


if __name__ == "__main__":
    main()
