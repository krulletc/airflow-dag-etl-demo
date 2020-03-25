import requests
import config as Config
import json
from airflow.hooks.postgres_hook import PostgresHook
import json
import numpy as np
from datetime import datetime
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

API_URL = "https://api.darksky.net/forecast"
API_Key = Config.API_KEY
LAT = Config.LAT
LONG = Config.LONG


def get_weather(**kwargs):
    """
    Time Machine Request format:
    https://api.darksky.net/forecast/[key]/[latitude],[longitude],[time]
    """
    execution_date = kwargs["execution_date"]
    # target_date = execution_date.subtract(hours=1).to_iso8601_string()
    target_date = execution_date.to_iso8601_string()

    request_url = API_URL + "/" + API_Key + "/" + LAT + "," + LONG + "," + target_date
    print("Request URL: " + request_url)
    result = requests.get(request_url)

    if result.status_code == 200:

        print("success")

        json_data = result.json()
        print(json.dumps(json_data, indent=4))

        # file_name = str(datetime.now().date()) + ".json"
        # tot_name = os.path.join(os.path.dirname(__file__), "data", file_name)
        # with open(tot_name, "w") as outputfile:
        #     json.dump(json_data, outputfile)

        # Pass data to the next part via xcoms
        kwargs["task_instance"].xcom_push(key="weather-data", value=json_data)
    else:
        print("Error in API call. 200 not received.")


def load_data(**kwargs):
    """
    Processes json data, checks the types and enters into the Postgres database
    """

    # file_name = str(datetime.now().date()) + ".json"
    # tot_name = os.path.join(os.path.dirname(__file__), "src/data", file_name)

    # # open the json datafile and read it in
    # with open(tot_name, "r") as inputfile:
    #     doc = json.load(inputfile)

    doc = kwargs["task_instance"].xcom_pull(
        key="weather-data", task_ids="weather_extract"
    )

    # transform the data to the correct types and convert
    latitude = float(doc["latitude"])
    longitude = float(doc["longitude"])
    date = str(doc["daily"]["data"][0]["time"])
    summary = str(doc["daily"]["data"][0]["summary"])
    max_temp = float(doc["daily"]["data"][0]["temperatureMax"]) * 1.8 + 32
    min_temp = float(doc["daily"]["data"][0]["temperatureMin"]) * 1.8 + 32

    # check for NaNs in the numeric values and then enter into the database
    valid_data = True
    for valid in np.isnan([latitude, longitude, max_temp, min_temp]):
        if valid is False:
            print("Invalid data found")
            valid_data = False
            break

    row = (latitude, longitude, date, summary, max_temp, min_temp)
    insert_cmd = """INSERT INTO weather_table
                    (latitude, longitude, date, summary, max_temp, min_temp)
                    VALUES
                    (%s, %s, %s, %s, %s, %s);"""

    if valid_data is True:
        print("Loading weather data")
        pg_hook = PostgresHook(postgres_conn_id="weather_id")
        pg_hook.run(insert_cmd, parameters=row)


with DAG(
    dag_id="weather_dag",
    start_date=datetime(2020, 3, 2),
    schedule_interval="@daily",
    catchup=True,
) as dag:
    # start = DummyOperator(task_id="weather_start")
    extract = PythonOperator(
        task_id="weather_extract", python_callable=get_weather, provide_context=True
    )
    transform_load = PythonOperator(
        task_id="load_data", python_callable=load_data, provide_context=True
    )
    extract >> transform_load
