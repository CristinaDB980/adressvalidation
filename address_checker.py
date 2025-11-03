import os
import time
from urllib.parse import quote

import pandas as pd
import requests

# ── Konfiguration ─────────────────────────────────────────────────────────────
API_KEY = os.getenv("API_KEY")  # in GitHub Actions als Secret übergeben
INPUT_CANDIDATES = [
    "data/addresses_template.csv",  # Standardpfad
    "addresses_template.csv"        # Fallback im Repo-Root
]
OUTPUT_FILE = "output.csv"
RADIUS_METERS = 50
PLACES_TYPE = "clothing_store"

# Prioritäten: 1) Marken 2) Lingerie/Dessous (EU) 3) Bekleidung/Mode (EU)
PRIORITY_KEYWORDS = [
    ("triumph", "TRIUMPH"), ("sloggi", "SLOGGI"),
    ("lingerie", "Lingerie"), ("dessous", "Lingerie"), ("unterwäsche", "Lingerie"),
    ("bielizna", "Lingerie"), ("spodní prádlo", "Lingerie"), ("spodná bielizeň", "Lingerie"),
    ("intimo", "Lingerie"), ("biancheria intima", "Lingerie"),
    ("lencería", "Lingerie"), ("ropa interior", "Lingerie"),
    ("lenjerie intimă", "Lingerie"), ("fehérnemű", "Lingerie"),
    ("donje rublje", "Lingerie"), ("žensko rublje", "Lingerie"),
    ("spodnje perilo", "Lingerie"), ("εσώρουχα", "Lingerie"),
    ("iç giyim", "Lingerie"), ("iç çamaşırı", "Lingerie"),
    ("underwear", "Lingerie"), ("intimates", "Lingerie"),
    ("underkläder", "Lingerie"), ("damunderkläder", "Lingerie"),
    ("undertøj", "Lingerie"), ("lingeri", "Lingerie"),
    ("undertøy", "Lingerie"), ("alusvaatteet", "Lingerie"),
    ("naisten alusvaatteet", "Lingerie"),
    ("mode", "Clothing"), ("bekleidung", "Clothing"), ("kleidung", "Clothing"),
    ("clothing", "Clothing"), ("fashion", "Clothing"), ("apparel", "Clothing"),
    ("vêtements", "Clothing"), ("prêt-à-porter", "Clothing"),
    ("abbigliamento", "Clothing"), ("abbigliamento donna", "Clothing"),
    ("ropa", "Clothing"), ("moda", "Clothing"), ("roupa", "Clothing"),
    ("kleding", "Clothing"), ("modewinkel", "Clothing"),
    ("odzież", "Clothing"), ("odzież damska", "Clothing"),
    ("oblečení", "Clothing"), ("dámské oblečení", "Clothing"),
    ("îmbrăcăminte", "Clothing"),
    ("giyim", "Clothing"), ("kadın giyim", "Clothing"),
    ("ρούχα", "Clothing"), ("dametøj", "Clothing"),
    ("kläder", "Clothing"), ("tøj", "Clothing"),
    ("vaatteet", "Clothing")
]

# ── Helpers ──────────────────────────────────────────────────────────────────
def load_input_df() -> pd.DataFrame:
    last_err = None
    for path in INPUT_CANDIDATES:
        try:
            # sep=None mit engine="python" → Pandas „erschnüffelt“ ; oder , automatisch
            return pd.read_csv(path, sep=None, engine="python")
        except Exception as e:
            last_err = e
    raise FileNotFoundError(f"Keine Eingabedatei gefunden. Versucht: {INPUT_CANDIDATES}. Letzter Fehler: {last_err}")

def safe_str(x) -> str:
    return "" if pd.isna(x) else str(x).strip()

def build_address(row: pd.Series) -> str:
    # "Street House, PostalCode City, Country"
    return f"{safe_str(row.get('Street'))} {safe_str(row.get('House'))}, " \
           f"{safe_str(row.get('PostalCode'))} {safe_str(row.get('City'))}, " \
           f"{safe_str(row.get('Country'))}"

def geocode(address: str) -> dict:
    # ordentlich encoden, Sonderzeichen bleiben erhalten
    encoded = quote(address, safe="/:, ")
    url = f"https://maps.googleapis.com/maps/api/geocode/json?address={encoded}&key={API_KEY}"
    return requests.get(url, timeout=20).json()

def places_nearby(lat: float, lng: float) -> dict:
    url = (
        "https://maps.googleapis.com/maps/api/place/nearbysearch/json"
        f"?location={lat},{lng}&radius={RADIUS_METERS}&type={PLACES_TYPE}&key={API_KEY}"
    )
    return requests.get(url, timeout=20).json()

def pick_best_shop(places_json: dict) -> tuple[bool, str, str, str]:
    """Suche priorisiert nach Keywords in name/vicinity/types."""
    if places_json.get("status") != "OK":
        return False, "", "", ""
    results = places_json.get("results", [])
    for key, level in PRIORITY_KEYWORDS:
        k = key.lower()
        for shop in results:
            name = (shop.get("name") or "").lower()
            vicinity = (shop.get("vicinity") or "").lower()
            types = " ".join(shop.get("types", [])).lower()
            if k in " ".join([name, vicinity, types]):
                return True, level, shop.get("name", ""), shop.get("vicinity", "")
    return False, "", "", ""

# ── Main ─────────────────────────────────────────────────────────────────────
def main():
    if not API_KEY:
        raise RuntimeError("API_KEY fehlt (in GitHub Actions als Secret `GOOGLE_MAPS_API` → env `API_KEY`).")

    df = load_input_df()

    # 👉 Output-DF startet als 1:1 Kopie der Input-Spalten (bleiben links stehen)
    result_df = df.copy()

    # Platzhalter-Spalten für rechts (werden befüllt)
    result_df["InputAddress"]   = ""
    result_df["GoogleAddress"]  = ""
    result_df["Status"]         = ""
    result_df["Lat"]            = None
    result_df["Lng"]            = None
    result_df["StoreFound"]     = ""
    result_df["MatchLevel"]     = ""
    result_df["StoreName"]      = ""
    result_df["StoreVicinity"]  = ""

    total = len(df)
    for i, row in df.iterrows():
        input_address = build_address(row)
        geo = geocode(input_address)

        if geo.get("status") != "OK" or not geo.get("results"):
            result_df.loc[i, ["InputAddress","GoogleAddress","Status","Lat","Lng",
                              "StoreFound","MatchLevel","StoreName","StoreVicinity"]] = [
                input_address, "", "Invalid", None, None, "NO", "", "", ""
            ]
            continue

        g0 = geo["results"][0]
        google_address = g0.get("formatted_address", "")
        loc = g0.get("geometry", {}).get("location", {})
        lat, lng = loc.get("lat"), loc.get("lng")

        pls = places_nearby(lat, lng)
        found, level, store_name, store_vicinity = pick_best_shop(pls)

        # rechts anhängen (links bleiben deine Originalspalten unverändert)
        result_df.loc[i, ["InputAddress","GoogleAddress","Status","Lat","Lng",
                          "StoreFound","MatchLevel","StoreName","StoreVicinity"]] = [
            input_address, google_address, "OK", lat, lng,
            "YES" if found else "NO", level, store_name, store_vicinity
        ]

        # leichtes Throttling gegen Rate Limits
        time.sleep(0.1)

    # CSV mit UTF-8 (Sonderzeichen bleiben korrekt)
    result_df.to_csv(OUTPUT_FILE, index=False, encoding="utf-8")
    print(f"✅ Fertig. Vergleichs-Output gespeichert in: {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
