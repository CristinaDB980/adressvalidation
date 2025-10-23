import pandas as pd
import requests
import time

API_KEY = os.environ.get("MAPS_API_KEY")

df = pd.read_csv('data/addresses_template.csv')

def build_address(row):
    return f"{row['Street']} {row['House']}, {row['PostalCode']} {row['City']}, {row['Country']}"

results = []
for i, row in df.iterrows():
    address = build_address(row)
    geo_url = f"https://maps.googleapis.com/maps/api/geocode/json?address={address}&key={API_KEY}"
    geo_data = requests.get(geo_url).json()

    if geo_data['status'] != "OK":
        results.append([address, "Invalid", None, None, None])
        continue

    lat = geo_data['results'][0]['geometry']['location']['lat']
    lng = geo_data['results'][0]['geometry']['location']['lng']

    places_url = f"https://maps.googleapis.com/maps/api/place/nearbysearch/json?location={lat},{lng}&radius=50&type=clothing_store&key={API_KEY}"
    places_data = requests.get(places_url).json()

    found = False
    if places_data['status'] == "OK":
        for shop in places_data.get('results', []):
            name = shop.get('name', '').lower()
            if any(x in name for x in ["triumph", "sloggi", "lingerie", "dessous", "unterwäsche"]):
                found = True
                break

    results.append([address, "OK", lat, lng, "YES" if found else "NO"])
    time.sleep(0.1)  # avoid rate limits

pd.DataFrame(results, columns=["Address","Status","Lat","Lng","StoreFound"]).to_csv("results/output.csv", index=False)

