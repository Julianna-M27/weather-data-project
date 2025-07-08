import requests
from dotenv import load_dotenv
import os

load_dotenv()


API_KEY = os.getenv("WEATHER_API_KEY")


api_url = f"http://api.weatherstack.com/current?access_key={API_KEY}&query=New York"


def fetch_data():
    try:
        print("Fetching weather data...")
        response = requests.get(api_url)
        response.raise_for_status
        print("API response recieved sucessfully.")
        print(response.json())
   
    except requests.exceptions.RequestException  as e:
        print(f"An error occured: {e}")
        raise


# mock fetching data for api
def mock_data():
    return {'request': {'type': 'City', 'query': 'New York, United States of America', 'language': 'en', 'unit': 'm'}, 'location': {'name': 'New York', 'country': 'United States of America', 'region': 'New York', 'lat': '40.714', 'lon': '-74.006', 'timezone_id': 'America/New_York', 'localtime': '2025-07-07 20:02', 'localtime_epoch': 1751918520, 'utc_offset': '-4.0'}, 'current': {'observation_time': '12:02 AM', 'temperature': 27, 'weather_code': 113, 'weather_icons': ['https://cdn.worldweatheronline.com/images/wsymbols01_png_64/wsymbol_0001_sunny.png'], 'weather_descriptions': ['Sunny'], 'astro': {'sunrise': '05:33 AM', 'sunset': '08:29 PM', 'moonrise': '06:17 PM', 'moonset': '02:20 AM', 'moon_phase': 'Waxing Gibbous', 'moon_illumination': 86}, 'air_quality': {'co': '364.45', 'no2': '101.935', 'o3': '27', 'so2': '15.17', 'pm2_5': '40.145', 'pm10': '42.18', 'us-epa-index': '2', 'gb-defra-index': '2'}, 'wind_speed': 16, 'wind_degree': 170, 'wind_dir': 'S', 'pressure': 1014, 'precip': 0, 'humidity': 72, 'cloudcover': 0, 'feelslike': 32, 'uv_index': 0, 'visibility': 16, 'is_day': 'yes'}}


print(mock_data())
