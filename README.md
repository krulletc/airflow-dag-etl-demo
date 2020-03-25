# airflow-dag-etl-mentorship
Stephen Kilbourn's H1 2020 Mentorship


## Docker Setup
This allows you to run Apache Airflow locally. A docker-compose file creates:
We will create a docker-compose file with:
- airflow webserver
- airflow scheduler (with LocalExecutor)
- PostgreSQL DB

For the first run of docker-compose and each time when you want to clean your Postgres data folder, you need to run postgres service in a separate terminal window and wait until it creates a database and init data folder.
After that run, you can comment out the `initdb` section of the `docker-compose.yml`

To start your docker containers, run `docker-compose up`
You can then view the Airflow dashboard at http://localhost:8080

## API call
The initial api call is using the DarkSky Time Machine weather forecast api.
https://darksky.net/dev/docs#time-machine-request
The API key, lat,  are and longitude currently stored in a `config.py` file
