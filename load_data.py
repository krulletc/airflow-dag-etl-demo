from datetime import datetime, timedelta

import os
from airflow.hooks import postgres_hook
import json
import numpy as np


def load_data(ds, **kwargs):
    """
    Processes json data, checks teh types and enters into the Postgres database
    """

    pg_hook = postgres_hook(postress_conn_id="weather_id")

    file_name - str(datetime.now().date()) + ".json"
    tot_name = os.path.join(os.path.dirname(__file__), "src/data", file_name)

    # open the json datafile and read it in
    with open(tot_name, "r") as inputfile:
        doc = json.load(inputfile)

    # transform the data to the correct types and convert
    latitude = float(doc["latitude"])
    longitude = float(doc["longitude"])
    date = str(doc["daily"]["time"])
    summary = str(doc["daily"]["summary"])
    max_temp = float(doc["daily"]["temperatureMax"]) * 1.8 + 32
    min_temp = float(doc["daily"]["temperatureMin"]) * 1.8 + 32

    # check for NaNs in the numeric values and then enter into the database
    valid_data = True
    for valid in np.isnan([latitude, longitude, max_temp, min_temp]):
        if valid is False:
            valid_data = False
            break
    row = (latitude, longitude, date, summary, max_temp, min_temp)

    insert_cmd = """INSERT INTO weather_table
                    (latitude, longitude, date, summary, max_temp, min_temp)
                    VALUES
                    (%s, %s, %s, %s, %s, %s);"""

    if valid_data is True:
        pg_hook.run(insert_cmd, parameters=row)
