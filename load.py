import duckdb
import pandas as pd
from extract import extract_weather  # Unsere extract Funktion wiederverwenden!
from datetime import datetime

def load_to_duckdb(df: pd.DataFrame, db_path: str = "weather.duckdb"):
    # Verbindung zur Datenbank herstellen
    # DuckDB erstellt die Datei automatisch wenn sie nicht existiert
    con = duckdb.connect(db_path)
    
    # Tabelle erstellen falls sie noch nicht existiert
    # IF NOT EXISTS = kein Fehler wenn Tabelle schon da ist
    con.execute("""
        CREATE TABLE IF NOT EXISTS weather_raw (
            city          VARCHAR,    -- Name der Stadt
            timestamp     TIMESTAMP,  -- Zeitpunkt der Messung
            temperature   FLOAT,      -- Temperatur in °C
            precipitation FLOAT,      -- Niederschlag in mm
            windspeed     FLOAT,      -- Windgeschwindigkeit in km/h
            extracted_at  TIMESTAMP   -- Wann wir die Daten geholt haben
        )
    """)
    
    # DataFrame in die Tabelle einfügen
    # DuckDB kann direkt mit pandas DataFrames arbeiten — sehr praktisch!
    con.execute("INSERT INTO weather_raw SELECT * FROM df")
    
    # Wie viele Zeilen sind jetzt in der Tabelle?
    count = con.execute("SELECT COUNT(*) FROM weather_raw").fetchone()[0]
    print(f"✅ Daten geladen! {count} Zeilen total in der Datenbank")
    
    # Kurze Zusammenfassung pro Stadt ausgeben
    summary = con.execute("""
        SELECT 
            city,
            COUNT(*)                    AS anzahl_messungen,
            ROUND(AVG(temperature), 1)  AS avg_temperatur,
            ROUND(MIN(temperature), 1)  AS min_temperatur,
            ROUND(MAX(temperature), 1)  AS max_temperatur
        FROM weather_raw
        GROUP BY city
        ORDER BY city
    """).fetchdf()  # fetchdf() = Ergebnis als pandas DataFrame
    
    print("\n📊 Zusammenfassung pro Stadt:")
    print(summary)
    
    con.close()  # Verbindung schließen

if __name__ == "__main__":
    print("🔄 Daten werden extrahiert...")
    cities = [
        ("Heilbronn", 49.14, 9.22),
        ("Stuttgart", 48.78, 9.18),
        ("Frankfurt", 50.11, 8.68),
    ]
    
    # Daten von allen Städten holen
    dfs = [extract_weather(city, lat, lon) for city, lat, lon in cities]
    df = pd.concat(dfs, ignore_index=True)
    
    print("💾 Daten werden in DuckDB gespeichert...")
    load_to_duckdb(df)