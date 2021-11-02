import os
import pandas as pd
from sqlalchemy import create_engine
from loguru import logger


class Postgres:
    def __init__(self):
        self.host = os.environ["host"]
        self.user = os.environ["user"]
        self.password = os.environ["password"]

        try:
            self.conn = create_engine(
                f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}/postgres"
            )

        except Exception as e:
            logger.error(f"Database connection error: {e}")

    def initialize(self, table_settings):
        return self.conn.execute(
            f'CREATE TABLE IF NOT EXISTS {table_settings["table_name"]} ({", ".join(table_settings["columns"])})'
        )

    def insert(self, params, table_name):
        data = pd.DataFrame(params)
        data.index += 1
        return data.to_sql(table_name, con=self.conn, if_exists="append", index=False)

    def get_all_values(self, table_name, column: list):
        data = pd.read_sql_query(
            f"""select {", ".join(column)} from {table_name}""", con=self.conn
        )
        return data

    def execute(self, sql: str):
        return self.conn.execute(sql)
