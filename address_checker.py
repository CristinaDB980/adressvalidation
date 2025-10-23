import os
import requests
import pandas as pd
import time

# ✅ Google API Key aus GitHub Secrets (Name muss in Actions: API_KEY sein)
API_KEY = os.environ.get("API_KEY")

# ✅ CSV mit Adressen laden (Pfad anpassen falls anders)
df = pd.read_csv('addresses_template.csv')

def build_address(row):
    """Baut die vollständige Adresse korrekt zusammen."""
    return f"{row['Street']} {row['House']}, {row['PostalCode']} {row['City']}, {row['Country']}"

results = []

for i, row in df.iterrows():
    address = build_address(row)
    print(f"🔄 Prüfe Adresse {i+1}/{len(df)}: {address}")

    # ✅ 1) Geocoding API
    geo_url = f"https://maps.googleapis.com/maps/api/geocode/json?address={address}&key={API_KEY}"
    geo_data = requests.get(geo_url).json()

    if geo_data.get('status') != "OK":
        results.append([address, "Invalid", None, None, None])
        continue

    lat = geo_data['results'][0]['geometry']['location']['lat']
    lng = geo_data['results'][0]['geometry']['location']['lng']

    # ✅ 2) Places API – nur 1 Request pro Adresse
    places_url = f"https://maps.googleapis.com/maps/api/place/nearbysearch/json?location={lat},{lng}&radius=50&type=clothing_store&key={API_KEY}"
    places_data = requests.get(places_url).json()

    found = False
    store_name = ""

    if places_data.get('status') == "OK":
        for shop in places_data.get('results', []):
            name = shop.get('name', '').lower()
            if any(x in name for x in ["triumph", "sloggi", "lingerie", "dessous", "unterwäsche", "bielizna", "damen"]):
                found = True
                store_name = shop.get('name', '')
                break

    results.append([address, "OK", lat, lng, "YES" if found else "NO", store_name])
    time.sleep(0.1)  # Verhindert Rate-Limits

# ✅ Speichere Ergebnis als output.csv im Root direkt (wichtig für GitHub Actions)
output_file = "output.csv"
pd.DataFrame(results, columns=["Address", "Status", "Lat", "Lng", "StoreFound", "StoreName"]).to_csv(output_file, index=False)

print(f"✅ Fertig! Ergebnisse gespeichert in: {output_file}")
