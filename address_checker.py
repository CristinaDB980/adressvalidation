import os
import time
from urllib.parse import quote
import pandas as pd
import requests

# ─────────────────────────────────────────────
# KONFIGURATION
# ─────────────────────────────────────────────

API_KEY = os.getenv("API_KEY")

INPUT_CANDIDATES = [
    "data/adress_template.xlsx",
    "adress_template.xlsx",
    "data/addresses_template.xlsx",
    "addresses_template.xlsx",
    "data/addresses_template.csv",
    "addresses_template.csv"
]

OUTPUT_FILE = "results/output.csv"
RADIUS_METERS = 50
PLACES_TYPE = "clothing_store"

# Output-Ordner erzeugen
os.makedirs("results", exist_ok=True)

# ─────────────────────────────────────────────
# PRIORISIERTE KEYWORDS
# ─────────────────────────────────────────────

PRIORITY_KEYWORDS = [
    ("triumph", "TRIUMPH"), ("sloggi", "SLOGGI"),
    ("lingerie", "Lingerie"), ("dessous", "Lingerie"),
    ("unterwäsche", "Lingerie"), ("bielizna", "Lingerie"),
    ("intimo", "Lingerie"), ("biancheria intima", "Lingerie"),
    ("lencería", "Lingerie"), ("ropa interior", "Lingerie"),
    ("lenjerie intimă", "Lingerie"), ("fehérnemű", "Lingerie"),
    ("donje rublje", "Lingerie"), ("žensko rublje", "Lingerie"),
    ("underwear", "Lingerie"), ("intimates", "Lingerie"),
    ("underkläder", "Lingerie"), ("damunderkläder", "Lingerie"),
    ("mode", "Clothing"), ("bekleidung", "Clothing"),
    ("clothing", "Clothing"), ("fashion", "Clothing"),
    ("vêtements", "Clothing"), ("abbigliamento", "Clothing"),
    ("ropa", "Clothing"), ("moda", "Clothing"),
    ("odzież", "Clothing"), ("oblečení", "Clothing"),
    ("giyim", "Clothing"), ("ρούχα", "Clothing"),
    ("kläder", "Clothing")
]

# ─────────────────────────────────────────────
# HELFERFUNKTIONEN
# ─────────────────────────────────────────────

def load_input_df() -> pd.DataFrame:
    """
    Robust:
    - versucht Excel (.xlsx, .xls) zuerst
    - dann CSV
    - Sonderzeichen bleiben erhalten
    """

    # Excel zuerst
    for path in INPUT_CANDIDATES:
        if path.lower().endswith((".xlsx", ".xls")):
            try:
                print(f"Versuche Excel: {path}")
                return pd.read_excel(path, dtype=str, keep_default_na=False)
            except Exception as e:
                print(f"Fehler beim Laden von {path}: {e}")

    # CSV danach
    for path in INPUT_CANDIDATES:
        if path.lower().endswith(".csv"):
            try:
                print(f"Versuche CSV: {path}")
                return pd.read_csv(path, sep=None, engine="python", dtype=str, keep_default_na=False)
            except Exception as e:
                print(f"Fehler beim Laden von {path}: {e}")

    raise FileNotFoundError(f"Keine gültige Eingabedatei gefunden. Versucht: {INPUT_CANDIDATES}")


def safe_str(x) -> str:
    return "" if x is None else str(x).strip()


def build_address(row: pd.Series) -> str:
    """
    Keine Transformation – exakt wie im Input verwenden.
    """
    return f"{safe_str(row.get('Street'))} {safe_str(row.get('House No.'))}, " \
           f"{safe_str(row.get('Postl Code'))} {safe_str(row.get('City'))}, " \
           f"{safe_str(row.get('Cty'))}"


def geocode(address: str) -> dict:
    encoded = quote(address, safe="/:, ")
    url = f"https://maps.googleapis.com/maps/api/geocode/json?address={encoded}&key={API_KEY}"
    return requests.get(url, timeout=20).json()


def places_nearby(lat: float, lng: float) -> dict:
    url = (
        "https://maps.googleapis.com/maps/api/place/nearbysearch/json"
        f"?location={lat},{lng}&radius={RADIUS_METERS}&type={PLACES_TYPE}&key={API_KEY}"
    )
    return requests.get(url, timeout=20).json()


def pick_best_shop_with_priority(row, places_json: dict):
    """
    Matching in dieser Reihenfolge:
    1. Name (Spalte B)
    2. c/o name (Spalte C)
    3. Name 2 (Spalte M)
    4. Google Places
    """

    input_fields = [
        safe_str(row.get("Name")).lower(),
        safe_str(row.get("c/o name")).lower(),
        safe_str(row.get("Name 2")).lower()
    ]

    # 1–3: Input-Spalten durchsuchen
    for key, level in PRIORITY_KEYWORDS:
        k = key.lower()
        for field in input_fields:
            if k in field:
                return True, level, row.get("Name", ""), row.get("Name 2", "")

    # 4: Google Places durchsuchen
    if places_json.get("status") != "OK":
        return False, "", "", ""

    for key, level in PRIORITY_KEYWORDS:
        k = key.lower()
        for shop in places_json.get("results", []):
            name = safe_str(shop.get("name")).lower()
            vicinity = safe_str(shop.get("vicinity")).lower()
            types = " ".join(shop.get("types", [])).lower()

            if k in f"{name} {vicinity} {types}":
                return True, level, shop.get("name", ""), shop.get("vicinity", "")

    return False, "", "", ""

# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

def main():

    if not API_KEY:
        raise RuntimeError("API_KEY fehlt in Github Actions! ENV: API_KEY")

    df = load_input_df()
    result_df = df.copy()  # Input vollständig erhalten

    # Neue Spalten
    result_df["Changes"] = ""
    result_df["InputAddress"] = ""
    result_df["GoogleAddress"] = ""
    result_df["Status"] = ""
    result_df["Lat"] = ""
    result_df["Lng"] = ""
    result_df["StoreFound"] = ""
    result_df["MatchLevel"] = ""
    result_df["StoreName"] = ""
    result_df["StoreVicinity"] = ""
    result_df["Correct Address"] = ""
    result_df["Correct Store Name"] = ""

    for i, row in df.iterrows():

        input_address = build_address(row)
        geo = geocode(input_address)

        if geo.get("status") != "OK" or not geo.get("results"):
            result_df.loc[i, [
                "InputAddress","GoogleAddress","Status","Lat","Lng",
                "StoreFound","MatchLevel","StoreName","StoreVicinity",
                "Correct Address","Correct Store Name","Changes"
            ]] = [
                input_address, "", "Invalid", "", "", "NO", "", "", "", "", "", "yes"
            ]
            continue

        g0 = geo["results"][0]
        google_address = g0.get("formatted_address", "")

        loc = g0.get("geometry", {}).get("location", {})
        lat = loc.get("lat", "")
        lng = loc.get("lng", "")

        places = places_nearby(lat, lng)

        found, level, store_name, store_vicinity = pick_best_shop_with_priority(row, places)

        changes = "no" if safe_str(input_address).lower() == safe_str(google_address).lower() else "yes"

        result_df.loc[i, [
            "InputAddress","GoogleAddress","Status","Lat","Lng",
            "StoreFound","MatchLevel","StoreName","StoreVicinity",
            "Correct Address","Correct Store Name","Changes"
        ]] = [
            input_address, google_address, "OK", lat, lng,
            "YES" if found else "NO", level, store_name, store_vicinity,
            google_address, store_name, changes
        ]

        time.sleep(0.1)

    # Sonderzeichen bleiben erhalten
    result_df.to_csv(OUTPUT_FILE, index=False, encoding="utf-8-sig")

    print(f"✅ Fertig → gespeichert unter: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
