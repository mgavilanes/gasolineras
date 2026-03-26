"""
Daily update: fetch yesterday's historical data, rebuild provincial CSVs
and provincial_stats.json, update metadata with variations.
Runs in GitHub Actions from the repo root (web/data/public/).
"""

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import json
import os
import re
import unicodedata
from datetime import datetime, timedelta

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


def fetch_historical(day, product_id):
    """Fetch historical data for a specific day. Falls back to real-time."""
    url = f"{BASE_URL}/EstacionesTerrestresHist/FiltroProducto/{day.strftime('%d-%m-%Y')}/{product_id}"
    try:
        resp = session.get(url, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        if data.get("ListaEESSPrecio"):
            return data
    except Exception:
        pass

    # Fallback to real-time
    url_rt = f"{BASE_URL}/EstacionesTerrestres/FiltroProducto/{product_id}"
    resp = session.get(url_rt, timeout=60)
    resp.raise_for_status()
    return resp.json()


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


def main():
    yesterday = (datetime.utcnow() - timedelta(days=1)).date()
    yesterday_str = str(yesterday)
    print(f"Processing historical data for {yesterday_str}")

    # Load existing province file mapping
    mapping_path = "province_files.json"
    if os.path.exists(mapping_path):
        with open(mapping_path, "r", encoding="utf-8") as f:
            province_files = json.load(f)
    else:
        province_files = {}

    # Fetch yesterday's data for all 3 products
    product_data = {}
    for pid, name in PRODUCTS.items():
        print(f"  Fetching {name} (ID {pid})...")
        data = fetch_historical(yesterday, pid)
        stations = data.get("ListaEESSPrecio", [])
        print(f"    {len(stations)} stations")
        product_data[name] = stations

    # Parse into rows keyed by (ideess)
    combined = {}
    for name, stations in product_data.items():
        price_col = f"precio_{name}"
        for s in stations:
            ideess = s.get("IDEESS", "").strip()
            if not ideess:
                continue
            if ideess not in combined:
                combined[ideess] = {
                    "provincia": s.get("Provincia", "").strip().upper(),
                }
            combined[ideess][price_col] = parse_decimal(s.get("PrecioProducto", ""))

    # Append to each province CSV
    os.makedirs(PRICES_DIR, exist_ok=True)

    by_province = {}
    for ideess, info in combined.items():
        prov = info["provincia"]
        if prov not in by_province:
            by_province[prov] = []
        by_province[prov].append({
            "fecha": yesterday_str,
            "id": ideess,
            "g95": info.get("precio_gasolina_95"),
            "g98": info.get("precio_gasolina_98"),
            "gA": info.get("precio_gasoleo_a"),
        })

    for prov, rows in by_province.items():
        filename = province_files.get(prov) or safe_filename(prov)
        province_files[prov] = filename
        filepath = os.path.join(PRICES_DIR, filename)

        new_df = pd.DataFrame(rows)

        if os.path.exists(filepath):
            existing = pd.read_csv(filepath, dtype=str)
            # Remove existing rows for this date (in case of re-run)
            existing = existing[existing["fecha"] != yesterday_str]
            combined_df = pd.concat([existing, new_df.astype(str)], ignore_index=True)
        else:
            combined_df = new_df.astype(str)

        combined_df.to_csv(filepath, index=False)
        print(f"  {prov}: {len(rows)} rows → {filename}")

    # Save updated mapping
    with open(mapping_path, "w", encoding="utf-8") as f:
        json.dump(province_files, f, ensure_ascii=False)

    # Rebuild provincial_stats.json from all CSVs
    print("Rebuilding provincial_stats.json...")
    all_frames = []
    for prov, filename in province_files.items():
        filepath = os.path.join(PRICES_DIR, filename)
        if os.path.exists(filepath):
            df = pd.read_csv(filepath)
            df["provincia"] = prov
            for col in ["g95", "g98", "gA"]:
                df[col] = pd.to_numeric(df[col], errors="coerce")
            all_frames.append(df)

    if all_frames:
        all_df = pd.concat(all_frames, ignore_index=True)
        con = duckdb.connect()
        con.register("prices", all_df)

        stats_query = """
        SELECT
            fecha,
            provincia,
            COUNT(*) AS n,
            ROUND(AVG(g95), 4) AS media_g95,
            ROUND(MIN(g95), 4) AS min_g95,
            ROUND(MAX(g95), 4) AS max_g95,
            ROUND(AVG(g98), 4) AS media_g98,
            ROUND(MIN(g98), 4) AS min_g98,
            ROUND(MAX(g98), 4) AS max_g98,
            ROUND(AVG(gA), 4) AS media_gA,
            ROUND(MIN(gA), 4) AS min_gA,
            ROUND(MAX(gA), 4) AS max_gA
        FROM prices
        WHERE g95 IS NOT NULL OR g98 IS NOT NULL OR gA IS NOT NULL
        GROUP BY fecha, provincia
        ORDER BY fecha, provincia
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

        # Update metadata with variations from reference date
        ref_query = f"""
        SELECT
            ROUND(AVG(g95), 4) AS avg_g95, ROUND(AVG(g98), 4) AS avg_g98, ROUND(AVG(gA), 4) AS avg_gA,
            ROUND(MEDIAN(g95), 4) AS med_g95, ROUND(MEDIAN(g98), 4) AS med_g98, ROUND(MEDIAN(gA), 4) AS med_gA
        FROM prices WHERE fecha = '{REF_DATE}'
        """
        hoy_query = f"""
        SELECT
            ROUND(AVG(g95), 4) AS avg_g95, ROUND(AVG(g98), 4) AS avg_g98, ROUND(AVG(gA), 4) AS avg_gA,
            ROUND(MEDIAN(g95), 4) AS med_g95, ROUND(MEDIAN(g98), 4) AS med_g98, ROUND(MEDIAN(gA), 4) AS med_gA
        FROM prices WHERE fecha = '{yesterday_str}'
        """
        ref = con.execute(ref_query).fetchone()
        hoy = con.execute(hoy_query).fetchone()

        if ref and hoy and ref[0] and hoy[0]:
            meta_path = "metadata.json"
            if os.path.exists(meta_path):
                with open(meta_path, "r") as f:
                    metadata = json.load(f)
            else:
                metadata = {}

            metadata["n_dias"] = int(con.execute("SELECT COUNT(DISTINCT fecha) FROM prices").fetchone()[0])
            metadata["fecha_referencia"] = REF_DATE
            metadata["var_media"] = {
                "g95": round((hoy[0] - ref[0]) / ref[0] * 100, 1) if ref[0] else None,
                "g98": round((hoy[1] - ref[1]) / ref[1] * 100, 1) if ref[1] else None,
                "gA": round((hoy[2] - ref[2]) / ref[2] * 100, 1) if ref[2] else None,
            }
            metadata["var_mediana"] = {
                "g95": round((hoy[3] - ref[3]) / ref[3] * 100, 1) if ref[3] else None,
                "g98": round((hoy[4] - ref[4]) / ref[4] * 100, 1) if ref[4] else None,
                "gA": round((hoy[5] - ref[5]) / ref[5] * 100, 1) if ref[5] else None,
            }
            metadata["mediana_referencia"] = {"g95": ref[3], "g98": ref[4], "gA": ref[5]}
            metadata["mediana_actual"] = {"g95": hoy[3], "g98": hoy[4], "gA": hoy[5]}

            with open(meta_path, "w", encoding="utf-8") as f:
                json.dump(metadata, f, ensure_ascii=False)
            print(f"  metadata.json updated with variations")

        con.close()

    print("Done.")


if __name__ == "__main__":
    main()
