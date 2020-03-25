import requests
import config as Config
import json
from datetime import datetime
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

API_URL = "https://api.darksky.net/forecast"
API_Key = Config.API_KEY
LAT = Config.LAT
LONG = Config.LONG
time = "255657600"


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

        json_data = result.json()
        file_name = str(datetime.now().date()) + ".json"
        tot_name = os.path.join(os.path.dirname(__file__), "data", file_name)
        print("success")
        print(json_data)
        # with open(tot_name, "w") as outputfile:
        #     json.dump(json_data, outputfile)
    else:
        print("Error in API call. 200 not received.")


with DAG(
    dag_id="weather_dag",
    start_date=datetime(2020, 2, 25),
    schedule_interval="@daily",
    catchup=True,
) as dag:
    start = DummyOperator(task_id="weather_start")
    extract = PythonOperator(
        task_id="weather_extract", python_callable=get_weather, provide_context=True
    )
    start >> extract

