import pandas as pd
import requests
from datetime import date, datetime
import time
import json
import os
import s3fs


API_KEY = os.getenv('API_KEY')
URL = 'https://raw.githubusercontent.com/xosasx/nigerian-local-government-areas/master/csv/lgas.csv'

def read_data(url): 
    lga_df = pd.read_csv(url,error_bad_lines=False)
    lagos_df = lga_df[lga_df.state_name =='Lagos']
    return lagos_df


def openweather_api(df: pd.DataFrame, api_key: str) -> pd.DataFrame:
    json_list = []
    state_list = []
    lga_list = []

    try:
        for i, col in df.iterrows():
            lat = col['latitude']
            long = col['longitude']
            state = col['state_name']
            lga = col['name']

            url = f'https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={long}&appid={api_key}'

            response = requests.get(url)

            if response.status_code == 200:
                data = response.json()
                json_list.append(data)
                state_list.append(state)
                lga_list.append(lga)

                time.sleep(2)

            else:
                print(f"Request failed for coordinates ({lat}, {long})")

        normalized_data = pd.json_normalize(json_list)
        weather_data = pd.DataFrame(normalized_data)
        weather_data['state_name'] = state_list
        weather_data['lga_name'] = lga_list

        return weather_data

    except Exception as e:
        print(e)

def transform_data(df: pd.DataFrame):
    if 'weather' in df.columns:
        weather_data = df.explode('weather').reset_index(drop=True)
        weather_data = pd.concat([weather_data.drop(['weather'], axis=1),
                                  weather_data['weather'].apply(pd.Series)], axis=1)

        data = {
            "station_name": weather_data['name'],
            "state": weather_data['state_name'],
            "lga": weather_data['lga_name'],
            "weather_type": weather_data['main'],
            "description": weather_data['description'],
            "longitude": weather_data["coord.lon"],
            "latitude": weather_data['coord.lat'],
            "temp(F)": weather_data['main.temp'],
            "temp_min(F)": weather_data['main.temp_min'],
            "temp_max(F)": weather_data['main.temp_max'],
            "pressure(hPa)": weather_data['main.pressure'],
            "humidity(%)": weather_data['main.humidity'],
            "wind_speed(m/s)": weather_data['wind.speed'],
            "wind_deg": weather_data['wind.deg'],
            "wind_gust(m/s)": weather_data['wind.gust'],
            "cloudiness(%)": weather_data['clouds.all'],
            "sea_atmo_pressure(hPa)": weather_data['main.sea_level'],
            "grnd_atmo_pressure(hPa)": weather_data['main.grnd_level'],
        }

        cleaned_data = pd.DataFrame(data)

        return cleaned_data
    else:
        return pd.DataFrame()  # Return an empty DataFrame if 'weather' column is not present


if __name__ == "__main__":
    lga = read_data(URL)

    raw_weather_data_filename = f'Lagos_LGA_raw_Weather_data_{date.today()}_{datetime.now().strftime("%H.%M.%S")}.csv'
    cleaned_weather_data_filename = f'Lagos_LGA_cleaned_Weather_data_{date.today()}_{datetime.now().strftime("%H.%M.%S")}.csv'

    raw_weather_df = openweather_api(lga,API_KEY)
    raw_weather_df.to_csv(f's3//sammy-ec2-raw-weatherdata/{raw_weather_data_filename}', index=False)       
    # raw_weather_df.to_csv(f'Lagos_LGA_raw_Weather_data_{date.today()}_{datetime.now().strftime("%H.%M.%S")}.csv', index=False)

    cleaned_weather_df = transform_data(raw_weather_df)
    cleaned_weather_df.to_csv(f's3//sammy-ec2-clean-weatherdata/{cleaned_weather_data_filename}', index=False)
    # cleaned_weather_df.to_csv(f'Lagos_LGA_cleaned_Weather_data_{date.today()}_{datetime.now().strftime("%H.%M.%S")}.csv', index=False)

    print('weather data collected successfully')
