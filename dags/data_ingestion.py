import dlt
from dlt.sources.helpers import requests
import os
from dotenv import load_dotenv
from os.path import dirname, join
from requests.exceptions import RequestException
import logging

# setting logging context
logger = logging.getLogger("weather_pipeline")
logger.setLevel(logging.INFO)

# commenting this out to stream the logs directly to airflow
# file_handler = logging.FileHandler("pipeline_run.log")
# file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
# logger.addHandler(file_handler)


# Airflow will capture this logs
console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
logger.addHandler(console_handler)


# Weather data for the following cities
CITIES = ["Bangalore", "Hyderabad", "Pune", "Delhi", "Chennai", "Kolkata", "Kochi"]

# raw tables
GEOCODE_TABLE = "raw_geocode"
WEATHER_TABLE = "raw_weather"

# Get the API_KEY
env_path = join(dirname(__file__), ".env")

# making sure to load the env from docker not from local system. 
load_dotenv(env_path, override=False)

api_key = os.environ.get("API_KEY")

# configure the dlt pipeline
pipeline = dlt.pipeline(
    pipeline_name="weather_data",
    destination="duckdb",
    dataset_name="raw_data"
)

# resource-1 - geocode data
@dlt.resource(table_name=GEOCODE_TABLE)
def get_geocode():
    for city in CITIES:
        try:
            url = f"http://api.openweathermap.org/geo/1.0/direct?q={city}&appid={api_key}"
            response = requests.get(url)
            response.raise_for_status()
            geocdode_data = response.json()

            # ingesting the city data
            for item in geocdode_data:
                item['requested_city'] = city 

            logger.info(f"Geocode data for {city} loaded to table {GEOCODE_TABLE}.")
            yield geocdode_data

        except RequestException as e:
            logger.error(f"API Error for {city}", exc_info=True)
            continue

        except Exception as e:
            logger.error(f"Unexpected error for {city}: {e}", exc_info=True)
            continue
        
    

# transformer as we are using resource-1 data in weather-url
@dlt.transformer(data_from=get_geocode, table_name=WEATHER_TABLE)
def get_weather(geocode_list):
    for geocode in geocode_list:

        # city, latitude, longitude from the upstream geocode data
        city = geocode["name"]
        lat = geocode["lat"]
        lon = geocode["lon"]
        rqst_city = geocode["requested_city"]

        try:
            url = f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={api_key}"
            response = requests.get(url)
            response.raise_for_status()
            weather_data = response.json()

            # ingesting the city data
            weather_data['requested_city'] = rqst_city 

            logger.info(f"Weather data for {city} loaded to table {WEATHER_TABLE}")
            yield weather_data

        except RequestException as e:
            logger.error(f"API error for coordinates {lat},{lon}: {e}", exc_info=True)
            continue

        except Exception as e:
            logger.error(f"Unexpected error for coordinates {lat},{lon}: {e}", exc_info=True)
            continue
    

# source as a final sink for resource and transformers
@dlt.source
def all_raw_data():
    return get_geocode, get_weather


if __name__ == "__main__":
    logger.info("Pipeline run started")
    load_info = pipeline.run(all_raw_data())
    logger.info(load_info)
    logger.info("Pipeline run completed")