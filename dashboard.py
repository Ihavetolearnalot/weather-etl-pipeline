import streamlit as st      # Web-Dashboard Framework
import duckdb               # Datenbank
import pandas as pd         # Datentabellen
import plotly.express as px # Interaktive Charts

# Verbindung zur Datenbank herstellen
con = duckdb.connect("weather.duckdb")

# Titel des Dashboards
st.title("🌤️ Wetter Dashboard")
st.markdown("Wettervorhersage für deutsche Städte")

# Sidebar — Dropdown mit allen Städten aus der Datenbank
städte = con.execute("SELECT DISTINCT city FROM weather_transformed ORDER BY city").fetchdf()
ausgewählte_stadt = st.sidebar.selectbox(
    "Stadt auswählen",
    städte["city"].tolist()  # tolist() = DataFrame-Spalte in Python-Liste umwandeln
)

# Daten für die ausgewählte Stadt laden
# f-String = Variable direkt in den SQL-String einbauen
df = con.execute(f"""
    SELECT * FROM weather_transformed
    WHERE city = '{ausgewählte_stadt}'
    ORDER BY timestamp
""").fetchdf()  # fetchdf() = Ergebnis als pandas DataFrame

# Drei Spalten nebeneinander — wie ein KPI-Block
col1, col2, col3 = st.columns(3)
col1.metric("Ø Temperatur", f"{df['temperature'].mean():.1f}°C")  # mean() = Durchschnitt
col2.metric("Min", f"{df['temperature'].min():.1f}°C")            # min() = kleinster Wert
col3.metric("Max", f"{df['temperature'].max():.1f}°C")            # max() = größter Wert

# Liniendiagramm mit Plotly — interaktiv, zoombar
fig = px.line(df, x="timestamp", y="temperature", title="Temperaturverlauf")
st.plotly_chart(fig, use_container_width=True)  # use_container_width = volle Breite

# Verbindung schließen
con.close()