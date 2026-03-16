from prefect import flow, task  # flow = komplette Pipeline, task = einzelner Schritt
import duckdb
import pandas as pd
import requests
from datetime import datetime

# @task = dieser Funktion wird ein einzelner Schritt in der Pipeline
# name = Name der in Prefect angezeigt wird
# retries = wie oft soll Prefect es nochmal versuchen wenn es fehlschlägt
@task(name="Wetterdaten extrahieren", retries=3)
def extract():
    cities = [
        ("Heilbronn", 49.14, 9.22),
        ("Stuttgart", 48.78, 9.18),
        ("Frankfurt", 50.11, 8.68),
    ]
    
    dfs = []
    for city, lat, lon in cities:
        url = "https://api.open-meteo.com/v1/forecast"
        params = {
            "latitude": lat,
            "longitude": lon,
            "hourly": "temperature_2m,precipitation,windspeed_10m",
            "forecast_days": 7
        }
        response = requests.get(url, params=params)
        data = response.json()
        
        df = pd.DataFrame({
            "city": city,
            "timestamp": data["hourly"]["time"],
            "temperature": data["hourly"]["temperature_2m"],
            "precipitation": data["hourly"]["precipitation"],
            "windspeed": data["hourly"]["windspeed_10m"],
            "extracted_at": datetime.now()
        })
        dfs.append(df)
    
    df_all = pd.concat(dfs, ignore_index=True)
    print(f"✅ {len(df_all)} Zeilen extrahiert")
    return df_all  # Daten weitergeben an den nächsten Task

@task(name="Daten laden", retries=2)
def load(df: pd.DataFrame):
    con = duckdb.connect("weather.duckdb")
    
    # Tabelle leeren und neu befüllen
    # OR REPLACE = wenn Tabelle schon existiert, ersetzen
    con.execute("CREATE OR REPLACE TABLE weather_raw AS SELECT * FROM df")
    
    count = con.execute("SELECT COUNT(*) FROM weather_raw").fetchone()[0]
    print(f"✅ {count} Zeilen in DuckDB geladen")
    con.close()

@task(name="Daten transformieren", retries=2)
def transform():
    con = duckdb.connect("weather.duckdb")
    
    con.execute("""
        CREATE OR REPLACE TABLE weather_transformed AS
        SELECT
            city,
            timestamp,
            temperature,
            precipitation,
            windspeed,
            ROUND(temperature * 9/5 + 32, 1) AS temperature_fahrenheit,
            CASE 
                WHEN windspeed < 10  THEN 'Windstill'
                WHEN windspeed < 30  THEN 'Leichte Brise'
                WHEN windspeed < 60  THEN 'Mäßiger Wind'
                ELSE                      'Starker Wind'
            END AS wind_kategorie,
            CASE
                WHEN HOUR(CAST(timestamp AS TIMESTAMP)) BETWEEN 6  AND 11 THEN 'Morgen'
                WHEN HOUR(CAST(timestamp AS TIMESTAMP)) BETWEEN 12 AND 17 THEN 'Mittag'
                WHEN HOUR(CAST(timestamp AS TIMESTAMP)) BETWEEN 18 AND 22 THEN 'Abend'
                ELSE                                        'Nacht'
            END AS tageszeit,
            CASE 
                WHEN precipitation > 0 THEN 'Ja'
                ELSE                        'Nein'
            END AS regen,
            extracted_at
        FROM weather_raw
    """)
    
    print("✅ Transformation abgeschlossen!")
    con.close()

# @flow = die komplette Pipeline die alle Tasks zusammenbindet
@flow(name="Wetter Pipeline")
def weather_pipeline():
    print("🚀 Pipeline gestartet!")
    
    # Tasks nacheinander ausführen
    # Prefect verwaltet die Reihenfolge und das Error-Handling automatisch
    df = extract()      # Schritt 1: Daten holen
    load(df)            # Schritt 2: Daten speichern
    transform()         # Schritt 3: Daten transformieren
    
    print("🎉 Pipeline erfolgreich abgeschlossen!")

if __name__ == "__main__":
    weather_pipeline()  # Pipeline einmal manuell starten