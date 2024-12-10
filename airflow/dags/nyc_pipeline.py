from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator

with DAG(dag_id="nyc_pipeline", start_date=datetime(2024, 7, 8), schedule=None) as dag:
    # This is often used for tasks which are more suitable for executing commands
    # For example, submit a job to a Spark cluster, initiate a new cluster,
    # run containers, upgrade software packages on Linux systems,
    # or installing a PyPI package
    system_maintenance_task = BashOperator(
        task_id="system_maintenance_task",
        # bash_command='apt-get update && apt-get upgrade -y'
        bash_command='echo "Install some pypi libs..."',
    )

    @task
    def download_nyc_yellow_dataset():
        import os
        import numpy as np
        import pandas as pd
        import requests

        SAVE_DIR = "/opt/airflow/data/"
        base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
        years = ["2022", "2023"]
        months = [f"{i:02}" for i in range(1, 13)]
        # "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"
        data_type = "yellow_tripdata_"
        for year in years:
            for month in months:
                url_download = base_url + data_type + year + "-" + month + ".parquet"
                print(url_download)
                file_path = os.path.join(
                    SAVE_DIR, data_type + year + "-" + month + ".parquet"
                )
                if os.path.exists(file_path):
                    print("File already exists: " + file_path)
                    continue
                try:
                    r = requests.get(url_download, allow_redirects=True)
                    open(file_path, "wb").write(r.content)
                except:
                    print("Error in downloading file: " + url_download)
                    continue

    @task
    def download_nyc_green_dataset():
        import os
        import numpy as np
        import pandas as pd
        import requests

        SAVE_DIR = "/opt/airflow/data/"
        base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
        years = ["2022", "2023"]
        months = [f"{i:02}" for i in range(1, 13)]
        # "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"
        data_type = "green_tripdata_"
        for year in years:
            for month in months:
                url_download = base_url + data_type + year + "-" + month + ".parquet"
                print(url_download)
                file_path = os.path.join(
                    SAVE_DIR, data_type + year + "-" + month + ".parquet"
                )
                if os.path.exists(file_path):
                    print("File already exists: " + file_path)
                    continue
                try:
                    r = requests.get(url_download, allow_redirects=True)
                    open(file_path, "wb").write(r.content)
                except:
                    print("Error in downloading file: " + url_download)
                    continue

    @task
    def remove_missing_value():
        import pandas as pd
        import os
        import logging

        DATA_DIR = "/opt/airflow/data/"
        for file in os.listdir(DATA_DIR):
            if file.endswith(".parquet"):
                df = pd.read_parquet(os.path.join(DATA_DIR, file), engine="pyarrow")
                # check columns not have missing data
                df = df.dropna(axis=1, how="any")
                if "store_and_fwd_flag" in df.columns:
                    df = df.drop(columns=["store_and_fwd_flag"])
                    logging.info("Dropped column store_and_fwd_flag from file: " + file)
                else:
                    logging.info("Column store_and_fwd_flag not found in file: " + file)
                df = df.dropna()
                # sorted columns
                df = df.reindex(sorted(df.columns), axis=1)
                df.to_parquet(os.path.join(DATA_DIR, file))
                logging.info(f"The {file} is now clean. Saved!")

    @task
    def set_types_for_data():
        import os
        import pandas as pd

        DATA_DIR = "/opt/airflow/data/"
        for file in os.listdir(DATA_DIR):
            if file.endswith(".parquet"):
                df = pd.read_parquet(os.path.join(DATA_DIR, file), engine="pyarrow")
                if "payment_type" in df.columns:
                    df.payment_type = df.payment_type.astype(int)
                if "trip_type" in df.columns:
                    df.trip_type = df.trip_type.astype(int)
                if "passenger_count" in df.columns:
                    df.passenger_count = df.passenger_count.astype(int)
                if "RatecodeID" in df.columns:
                    df.RatecodeID = df.RatecodeID.astype(int)
                df.to_parquet(os.path.join(DATA_DIR, file))

    @task
    def transform_data():
        import os
        import pandas as pd

        DATA_DIR = "/opt/airflow/data/"
        for file in os.listdir(DATA_DIR):
            if file.endswith(".parquet"):
                df = pd.read_parquet(os.path.join(DATA_DIR, file), engine="pyarrow")
                if file.startswith("green"):
                    df.rename(
                        columns={
                            "lpep_pickup_datetime": "pickup_datetime",
                            "lpep_dropoff_datetime": "dropoff_datetime",
                            "ehail_fee": "fee",
                        },
                        inplace=True,
                    )
                if file.startswith("yellow"):
                    df.rename(
                        columns={
                            "tpep_pickup_datetime": "pickup_datetime",
                            "tpep_dropoff_datetime": "dropoff_datetime",
                            "Airport_fee": "fee",
                        },
                        inplace=True,
                    )
                df.columns = map(str.lower, df.columns)
                # drop fee column
                if "fee" in df.columns:
                    df.drop(columns=["fee"], inplace=True)
                df.to_parquet(os.path.join(DATA_DIR, file))

    @task
    def create_streamming_data():
        import os

        import pandas as pd

        DATA_DIR = "/opt/airflow/data/"
        streamming_path = os.path.join(DATA_DIR, "stream")
        os.makedirs(streamming_path, exist_ok=True)
        df_list = []
        for file in os.listdir(DATA_DIR):
            df = pd.read_parquet(os.path.join(DATA_DIR, file), engine="pyarrow")
            # get random 1000 rows
            if df.shape[0] < 10000:
                continue
            df = df.sample(n=10000)
            df["content"] = [file.split("_")[0]] * 10000
            df_list.append(df)
        df = pd.concat(df_list)
        df.to_parquet(os.path.join(streamming_path, "stream.parquet"))

    (
        system_maintenance_task
        >> [download_nyc_yellow_dataset(), download_nyc_green_dataset()]
        >> remove_missing_value()
        >> set_types_for_data()
        >> transform_data()
        >> create_streamming_data()
    )
