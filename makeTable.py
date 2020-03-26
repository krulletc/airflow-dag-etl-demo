from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database

import psycopg2


def make_database():

    dbname = "WeatherDB"
    username = "Stephen"
    tablename = "weather_table"

    engine = create_engine("postgresql+psycopg2://%s@localhost/%s" % (username, dbname))

    if not database_exists(engine.url):
        create_database(engine.url)

    conn = psycopg2.connect(database=dbname, user=username)

    curr = conn.cursor()

    create_table = (
        """CREATE TABLE IF NOT EXISTS %s
                    (
                        latitude: REAL,
                        longitude: REAL,
                        date: Date,
                        summary: TEXT,
                        max_temp: REAL,
                        min_temp: REAL
                    )
                    """
        % tablename
    )

    curr.execute(create_table)
    conn.commit()
    conn.close()


if __name__ == "__main__":
    make_database()
