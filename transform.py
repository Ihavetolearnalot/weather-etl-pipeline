import duckdb

def transform(db_path: str = "weather.duckdb"):
    con = duckdb.connect(db_path)
    
    # Transformierte Tabelle erstellen
    # OR REPLACE = Tabelle neu erstellen wenn sie schon existiert
    con.execute("""
        CREATE OR REPLACE TABLE weather_transformed AS
        
        SELECT
            city,
            timestamp,
            temperature,
            precipitation,
            windspeed,
            
            -- Temperatur in Fahrenheit umrechnen (für internationale Nutzer)
            ROUND(temperature * 9/5 + 32, 1)  AS temperature_fahrenheit,
            
            -- Windstärke kategorisieren
            CASE 
                WHEN windspeed < 10  THEN 'Windstill'
                WHEN windspeed < 30  THEN 'Leichte Brise'
                WHEN windspeed < 60  THEN 'Mäßiger Wind'
                ELSE                      'Starker Wind'
            END AS wind_kategorie,
            
            -- Tageszeit bestimmen
            CASE
                WHEN HOUR(timestamp) BETWEEN 6  AND 11 THEN 'Morgen'
                WHEN HOUR(timestamp) BETWEEN 12 AND 17 THEN 'Mittag'
                WHEN HOUR(timestamp) BETWEEN 18 AND 22 THEN 'Abend'
                ELSE                                        'Nacht'
            END AS tageszeit,
            
            -- Regnet es?
            CASE 
                WHEN precipitation > 0 THEN 'Ja'
                ELSE                        'Nein'
            END AS regen,
            
            extracted_at
            
        FROM weather_raw
    """)
    
    print("✅ Transformation abgeschlossen!")
    
    # Ergebnis prüfen
    result = con.execute("""
        SELECT 
            city,
            tageszeit,
            ROUND(AVG(temperature), 1) AS avg_temperatur,
            MAX(wind_kategorie)        AS wind,
            SUM(CASE WHEN regen = 'Ja' THEN 1 ELSE 0 END) AS regenstunden
        FROM weather_transformed
        GROUP BY city, tageszeit
        ORDER BY city, tageszeit
    """).fetchdf()
    
    print("\n🌤️ Wetter-Übersicht nach Tageszeit:")
    print(result.to_string(index=False))
    
    con.close()

if __name__ == "__main__":
    transform()