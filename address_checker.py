import os
import requests
import pandas as pd
import time

API_KEY = os.environ.get("API_KEY")

df = pd.read_csv('data/addresses_template.csv')

def build_address(row):
    return f"{row['Street']} {row['House']}, {row['PostalCode']} {row['City']}, {row['Country']}"

results = []

# Priorität nach Marke:
PRIORITY_KEYWORDS = [
    ("triumph", "TRIUMPH"),      # höchste Priorität
    ("sloggi", "SLOGGI"),
    ("lingerie", "Dessous"),
    ("dessous", "Dessous"),
    ("unterwäsche", "Unterwäsche"),
    ("bielizna", "Unterwäsche"),  # polnisch
    ("damen", "Damenbekleidung")
]

for i, row in df.iterrows():
    address = build_address(row)
    print(f"🔄 Prüfe Adresse {i+1}/{len(df)}: {address}")

    # 1) Geocoding
    geo_url = f"https://maps.googleapis.com/maps/api/geocode/json?address={address}&key={API_KEY}"
    geo_data = requests.get(geo_url).json()
    if geo_data.get('status') != "OK":
        results.append([address, "Invalid", None, None, "NO", ""])
        continue

    lat = geo_data['results'][0]['geometry']['location']['lat']
    lng = geo_data['results'][0]['geometry']['location']['lng']

    # 2) Places API (nur 1 Call)
    places_url = f"https://maps.googleapis.com/maps/api/place/nearbysearch/json?location={lat},{lng}&radius=50&type=clothing_store&key={API_KEY}"
    places_data = requests.get(places_url).json()

    found = False
    store_name = ""
    store_addr = ""

    if places_data.get('status') == "OK":
        for keyword, label in PRIORITY_KEYWORDS:
            for shop in places_data.get('results', []):
                name = shop.get('name', '').lower()
                if keyword in name:
                    found = True
                    store_name = shop.get('name', '')
                    store_addr = shop.get('vicinity', '')
                    break
            if found:
                break

    results.append([
        address,
        "OK",
        lat,
        lng,
        "YES" if found else "NO",
        store_name,
        store_addr
    ])
    time.sleep(0.1)

pd.DataFrame(
    results,
    columns=["InputAddress", "Status", "Lat", "Lng", "StoreFound", "StoreName", "StoreAddress"]
).to_csv("output.csv", index=False)

print("✅ Fertig! Ergebnis in output.csv gespeichert.")

