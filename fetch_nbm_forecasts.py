import requests
import pandas as pd
import datetime as dt
import sqlite3
import logging

def init_db(db_path="nbm_forecast_data.db"):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS nbm_forecasts (
            Name TEXT,
            Retreive_DT TEXT,
            startTime TEXT,
            endTime TEXT,
            isDaytime INTEGER,
            temperature REAL,
            temperatureUnit TEXT,
            temperatureTrend TEXT,
            probabilityOfPrecipitation REAL,
            windSpeed REAL,
            windDirection TEXT,
            shortForecast TEXT
        )
    """)
    conn.commit()
    return conn


def get_nbm_forecast(name, lat, lon):
    try:
        """Fetch daily max temperature forecast using NWS API for a given lat/lon."""
        # Step 1: Get forecast endpoint for the lat/lon
        points_url = f"https://api.weather.gov/points/{lat},{lon}"
        headers = {"User-Agent": "weather-research (sidruzdzel@gmail.com)"}
    
        resp = requests.get(points_url, headers=headers)
        resp.raise_for_status()
        forecast_url = resp.json()["properties"]["forecast"]
    
        # Step 2: Get forecast data
        forecast_resp = requests.get(forecast_url, headers=headers)
        forecast_resp.raise_for_status()
        forecast_data = forecast_resp.json()
    
        # Step 3: Extract useful info
        periods = forecast_data["properties"]["periods"]
        retreive_dt = dt.datetime.now()
        # cut out detailedForecast in the interest of saving memory
        data = pd.DataFrame(columns=['Name','Retreive_DT','startTime', 'endTime', 'isDaytime', 'temperature', 'temperatureUnit', 'temperatureTrend', 'probabilityOfPrecipitation', 'windSpeed', 'windDirection', 'shortForecast'])
    except Exception as e:
        logging.error('API Failed',exc_info=True)
        return pd.DataFrame()
    
    for i,p in enumerate(periods):
        try:
            data.loc[i,'Name'] = name
            data.loc[i,'Retreive_DT'] = retreive_dt
            data.loc[i,'probabilityOfPrecipitation'] = p['probabilityOfPrecipitation']['value']
            for col in ['startTime', 'endTime', 'isDaytime', 'temperature', 'temperatureUnit', 'temperatureTrend', 'windSpeed', 'windDirection', 'shortForecast']:
                data.loc[i,col] = p[col]
        except Exception as e:
            logging.error('Failed to parse row',exc_info=True)
            
    return data


def main():
    # Log to console with INFO level or higher
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


    locations = pd.read_csv('locations.csv')
    
    df = pd.DataFrame()
    for _, row in locations.iterrows():
        df = pd.concat([df,get_nbm_forecast(row.Name, row.Lat, row.Lon)])
        
    conn = init_db()
    df.to_sql("nbm_forecasts", conn, if_exists="append", index=False)

    # df = pd.read_sql("SELECT * FROM nbm_forecasts", sqlite3.connect("nbm_forecast_data.db"))
    
if __name__ == "__main__":
    main()