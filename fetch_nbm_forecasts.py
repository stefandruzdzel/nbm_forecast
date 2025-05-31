import requests
import pandas as pd
import datetime as dt
import sqlite3
import logging
import pyarrow as pa
import pyarrow.parquet as pq
import s3fs
import pytz


def mtn_now():
    mountain_tz = pytz.timezone("America/Denver")
    return dt.datetime.now(mountain_tz)

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
        retreive_dt = mtn_now()
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


def store_in_s3(df):
    # S3 bucket base path
    s3_base_path = "s3://weather-forecasts-dzel/NBM"
    
    # Initialize S3 filesystem (uses your AWS credentials automatically)
    fs = s3fs.S3FileSystem(profile='default')
    
    date_str = mtn_now().strftime('%Y-%m-%d')
 
    # Convert to pyarrow Table
    table = pa.Table.from_pandas(df)
    
    # Create a filename with timestamp for uniqueness (e.g., time you run the job)
    timestamp = mtn_now().strftime("%H%M%S")
    s3_path = f"{s3_base_path}/date={date_str}/forecast_{timestamp}.parquet"
    
    # Write parquet directly to S3
    with fs.open(s3_path, 'wb') as f:
        pq.write_table(table, f)
    
    print(f"Written partition for {date_str} to {s3_path}")
    
    
def main():
# if 1 == 1:
    # Log to console with INFO level or higher
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    locations = pd.read_csv('locations.csv')
    
    df = pd.DataFrame()
    for _, row in locations.iterrows():
        df = pd.concat([df,get_nbm_forecast(row.Name, row.Lat, row.Lon)])
    
    try:
        store_in_s3(df)
    except Exception as e:
        logging.error('Failed to store in S3',exc_info=True)
        
        
if __name__ == "__main__":
    main()