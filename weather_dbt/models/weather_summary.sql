-- Dieses Model erstellt eine Zusammenfassung der Wetterdaten pro Stadt und Tag
-- dbt führt dieses SQL automatisch aus und erstellt eine Tabelle/View in DuckDB

{{ config(materialized='table') }}
-- materialized='table' = dbt erstellt eine echte Tabelle (nicht nur eine View)

SELECT
    city,                                           -- Stadtname
    DATE(timestamp) AS datum,                       -- Nur das Datum (ohne Uhrzeit)
    ROUND(AVG(temperature), 1)    AS avg_temp,      -- Durchschnittstemperatur pro Tag
    ROUND(MIN(temperature), 1)    AS min_temp,      -- Tiefstwert
    ROUND(MAX(temperature), 1)    AS max_temp,      -- Höchstwert
    ROUND(AVG(windspeed), 1)      AS avg_wind,      -- Durchschnittliche Windgeschwindigkeit
    SUM(precipitation)            AS total_regen,   -- Gesamtniederschlag pro Tag
    COUNT(*)                      AS anzahl_messungen

 FROM {{ source('main', 'weather_raw') }}
-- ref() = dbt-Funktion um auf andere Modelle oder Tabellen zu verweisen
-- Hier verweisen wir auf unsere weather_raw Tabelle in DuckDB

GROUP BY city, DATE(timestamp)
ORDER BY city, datum