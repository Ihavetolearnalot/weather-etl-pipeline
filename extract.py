import requests      # Für HTTP-Anfragen an APIs
import pandas as pd  # Für Datentabellen (DataFrames)
from datetime import datetime  # Für Zeitstempel

def extract_weather(city: str, lat: float, lon: float) -> pd.DataFrame:
    # Die URL der kostenlosen Wetter-API (kein API-Key nötig!)
    url = "https://api.open-meteo.com/v1/forecast"
    
    # Parameter die wir der API mitgeben — was wollen wir haben?
    params = {
        "latitude": lat,           # Breitengrad der Stadt
        "longitude": lon,          # Längengrad der Stadt
        "hourly": "temperature_2m,precipitation,windspeed_10m",  # Welche Messwerte
        "forecast_days": 7         # 7 Tage Vorhersage
    }
    
    # API aufrufen — response ist die Antwort des Servers
    response = requests.get(url, params=params)
    
    # JSON in ein Python-Dictionary umwandeln
    # JSON ist das Format in dem APIs Daten zurückschicken
    data = response.json()
    
    # Aus dem Dictionary ein DataFrame bauen
    # Ein DataFrame ist wie eine Excel-Tabelle in Python
    df = pd.DataFrame({
        "city": city,
        "timestamp": data["hourly"]["time"],              # Zeitpunkt der Messung
        "temperature": data["hourly"]["temperature_2m"],  # Temperatur in 2m Höhe
        "precipitation": data["hourly"]["precipitation"], # Niederschlag in mm
        "windspeed": data["hourly"]["windspeed_10m"],     # Windgeschwindigkeit
        "extracted_at": datetime.now()                    # Wann haben WIR die Daten geholt
    })
    return df  # DataFrame zurückgeben

if __name__ == "__main__":
    # Diese Liste enthält unsere Städte mit Koordinaten
    # Tupel-Format: (Name, Breitengrad, Längengrad)
    cities = [
        ("Heilbronn", 49.14, 9.22),
        ("Stuttgart", 48.78, 9.18),
        ("Frankfurt", 50.11, 8.68),
    ]
    
    # Für jede Stadt extract_weather aufrufen
    # Das ist eine "List Comprehension" — kompaktes Python für eine Schleife
    # Gleiche wie: for city, lat, lon in cities: dfs.append(extract_weather(...))
    dfs = [extract_weather(city, lat, lon) for city, lat, lon in cities]
    
    # Alle drei DataFrames zu einem zusammenfügen
    # ignore_index=True → Index neu durchnummerieren (0,1,2,3...)
    df = pd.concat(dfs, ignore_index=True)
    
    # Ergebnis ausgeben
    print(f"✅ {len(df)} Zeilen extrahiert")  # len() = Anzahl Zeilen
    print(df.head())  # head() zeigt die ersten 5 Zeilen