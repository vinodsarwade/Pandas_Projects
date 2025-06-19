import requests
import json
import pandas as pd
import sqlite3

latitude = 19.0760
longitude = 72.8777
url = f"https://api.open-meteo.com/v1/forecast?latitude={latitude}&longitude={longitude}&current=temperature_2m,wind_speed_10m"
conn = sqlite3.connect("weather.db")

def Extract_data():
    responce = requests.get(url)
    data = responce.json()
    return data

def transform_data(data):
    df = pd.DataFrame(data)
    df = df['current']
    return df

def load_to_db(transform):
    transform.to_sql("weather",conn, if_exists='replace')
    result = pd.read_sql('SELECT * FROM weather', conn)
    return result


data = Extract_data()
transform = transform_data(data)
print(load_to_db(transform))

conn.close()
