import os 
import time
from urllib.parse import quote
import pandas as pd
import requests

# ── Konfiguration ─────────────────────────────────────────────────────────────
API_KEY = os.getenv("API_KEY")
INPUT_CANDIDATES = [
    "data/addresses_template.csv",
    "addresses_template.csv"
]
OUTPUT_FILE = "output.csv"
RADIUS_METERS = 50
PLACES_TYPE = "clothing_store"

# ── Prioritätsliste ───────────────────────────────────────────────────────────
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

# ── Helpers ──────────────────────────────────────────────────────────────────

def load_input_df() -> pd.DataFrame:
    last_err = None
    for path in INPUT_CANDIDATES:
        try:
            # sep=None lässt Pandas Auto-Detection machen
            return pd.read_csv(path, sep=None, engine="python", dtype=str, keep_default_na=False)
        except Exception as e:
            last_err = e
    raise FileNotFoundError(f"Keine Eingabedatei gefunden. Versucht: {INPUT_CANDIDATES}. Fehler: {last_err}")

def safe_str(x) -> str:
    return "" if x is None else str(x).strip()

def build_address(row: pd.Series) -> str:
    # Keine Datentransformation → alles 1:1 übernehmen
    street = safe_str(row.get("Street"))
    house  = safe_str(row.get("House No."))
    post   = safe_str(row.get("Postl Code"))
    city   = safe_str(row.get("City"))
    cty    = safe_str(row.get("Cty"))

    # korrektes Format
    return f"{street} {house}, {post} {city}, {cty}"

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
    """Neue Matching-Reihenfolge:
       1. Name (B), 2. c/o Name (C), 3. Name 2 (M), 4. Google Places → Keywords
    """

    # --- 1) Prüfe Felder aus Input ---
    input_fields = [
        safe_str(row.get("Name")).lower(),
        safe_str(row.get("c/o name")).lower(),
        safe_str(row.get("Name 2")).lower()
    ]

    for key, level in PRIORITY_KEYWORDS:
        k = key.lower()
        for field in input_fields:
            if k in field:
                # sofortiger Treffer → höchste Priorität
                return True, level, row.get("Name", ""), row.get("Name 2", "")

    # --- 2) Google Places durchsuchen ---
    if places_json.get("status") != "OK":
        return False, "", "", ""

    results = places_json.get("results", [])

    for key, level in PRIORITY_KEYWORDS:
        k = key.lower()
        for shop in results:
            name = safe_str(shop.get("name")).lower()
            vicinity = safe_str(shop.get("vicinity")).lower()
            types = " ".join(shop.get("types", [])).lower()
            if k in f"{name} {vicinity} {types}":
                return True, level, shop.get("name", ""), shop.get("vicinity", "")

    return False, "", "", ""

# ── Main ─────────────────────────────────────────────────────────────────────

def main():

    if not API_KEY:
        raise RuntimeError("API_KEY fehlt!")

    df = load_input_df()

    # Originalspalten komplett & unverändert kopieren
    result_df = df.copy()

    # Neue Spalten anhängen
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

        # Ungültige Adresse
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

        pls = places_nearby(lat, lng)
        found, level, store_name, store_vicinity = pick_best_shop_with_priority(row, pls)

        # ADDRESS CHANGE CHECK
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

    # UTF-8 ohne Veränderung → Sonderzeichen bleiben exakt erhalten
    result_df.to_csv(OUTPUT_FILE, index=False, encoding="utf-8-sig")

    print(f"✅ Fertig → Output gespeichert unter: {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
