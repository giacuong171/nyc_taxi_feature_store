import os
import random
from datetime import datetime
from time import sleep

import pandas as pd

from postgresql_client import PostgresSQLClient

TABLE_NAME = "nyc_taxi"
NUM_ROWS = 100000


def main():
    pc = PostgresSQLClient(
        database="k6",
        user="k6",
        password="k6",
        host="172.17.0.1",
    )

    kafka_df = pd.read_parquet("../airflow/data/stream/stream.parquet")
    # drop na
    kafka_df = kafka_df.dropna()
    # print(kafka_df.head())
    raw_columns = kafka_df.columns
    # remove column lower case
    kafka_df.columns = [col.lower() for col in raw_columns]

    # Get all columns from the devices table
    try:
        columns = pc.get_columns(table_name=TABLE_NAME)
        print(columns)
        print(len(columns))
    except Exception as e:
        print(f"Failed to get schema for table with error: {e}")

    # import pdb;pdb.set_trace()

    for _ in range(NUM_ROWS):
        # Randomize values for feature columns
        row = kafka_df.sample()
        ##convert to list
        tmp_arr = []
        for col_name in columns[1:]:
            try:
                if col_name in ["pickup_datetime", "dropoff_datetime"]:
                    tmp_arr.append(str(row[col_name].values[0]))
                else:
                    tmp_arr.append(row[col_name].values.tolist()[0])
            except KeyError:
                tmp_arr.append(None)

        created_time = str(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        data = [created_time] + tmp_arr
        columns_str = ",".join(columns)
        placeholders = ",".join(
            ["%s"] * len(data)
        )  # Create placeholders for each value

        query = f"""
            INSERT INTO {TABLE_NAME} ({columns_str})
            VALUES ({placeholders})
        """
        # import pdb;pdb.set_trace()
        pc.execute_query(query, tuple(data))
        sleep(1)


if __name__ == "__main__":
    main()
