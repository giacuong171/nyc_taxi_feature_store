import os
from datetime import datetime

import pandas as pd
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from great_expectations_provider.operators.great_expectations import (
    GreatExpectationsOperator,
)
from airflow import DAG


import yaml

DATA_DIR = "/opt/airflow/data/"
POSTGRES_CONN_ID = "postgres_default"
deltalake_config = "/opt/airflow/config/deltalake_config.yaml"


def load_cfg(cfg_file):
    """
    Load configuration from a YAML config file
    """
    cfg = None
    with open(cfg_file, "r") as f:
        try:
            cfg = yaml.safe_load(f)
        except yaml.YAMLError as exc:
            print(exc)

    return cfg


def upload_local_directory_to_minio(minio_client, local_path, bucket_name, minio_path):
    import glob

    assert os.path.isdir(local_path)

    for local_file in glob.glob(local_path + "/**"):
        if not os.path.isfile(local_file):
            upload_local_directory_to_minio(
                minio_client,
                local_file,
                bucket_name,
                minio_path + "/" + os.path.basename(local_file),
            )
        else:
            remote_path = os.path.join(minio_path, local_file[1 + len(local_path) :])
            minio_client.fput_object(bucket_name, remote_path, local_file)


with DAG(dag_id="nyc2deltalake", start_date=datetime(2023, 7, 1), schedule=None) as dag:
    system_maintenance_task = BashOperator(
        task_id="system_maintenance_task",
        bash_command="echo 'Install some dependencies' ",
        # bash_command="apt-get update && apt-get upgrade -y",
        # bash_command="pip install minio==7.1.16 deltalake==0.10.2 delta-spark==3.1.0 ",
    )

    @task
    def generate_deltalake():
        import numpy as np
        import pandas as pd
        from glob import glob
        from deltalake import DeltaTable
        from deltalake.writer import write_deltalake
        import urllib3

        dtypes = {
            "dolocationid": "int64",
            "dropoff_datetime": "datetime64[us]",
            "extra": "float64",
            "fare_amount": "float64",
            "improvement_surcharge": "float64",
            "mta_tax": "float64",
            "pickup_datetime": "datetime64[us]",
            "pulocationid": "int64",
            "tip_amount": "float64",
            "tolls_amount": "float64",
            "total_amount": "float64",
            "trip_distance": "float64",
            "vendorid": "int64",
        }
        http_client = urllib3.PoolManager(cert_reqs="CERT_NONE")
        urllib3.disable_warnings()
        # Upload files.
        all_fps = glob(os.path.join(DATA_DIR, "green_*.parquet"))
        for fp in all_fps:
            print(f"Create deltalake {fp}")
            df = pd.read_parquet(os.path.join(DATA_DIR, fp), engine="pyarrow")
            df = df.astype(dtypes)
            write_deltalake(os.path.join(DATA_DIR, "delta_lake"), df, mode="append")

    @task
    def parquet2deltalake():
        from minio import Minio
        from glob import glob
        import os
        import urllib3

        print("FINISH INSTALL PACKAGE")

        cfg = load_cfg(deltalake_config)
        datalake_cfg = cfg["datalake"]
        airflow_data = cfg["airflow_data"]
        # Create a client with the MinIO server playground, its access key
        # and secret key.
        # For fixing this bug: [Errno 111] Connection refused')); 527)
        http_client = urllib3.PoolManager(cert_reqs="CERT_NONE")
        urllib3.disable_warnings()
        # secure set to false if this bug https://github.com/minio/minio/issues/8161
        client = Minio(
            endpoint=datalake_cfg["endpoint"],
            access_key=datalake_cfg["access_key"],
            secret_key=datalake_cfg["secret_key"],
            http_client=http_client,
            secure=False,
        )

        # Create bucket if not exist.
        found = client.bucket_exists(datalake_cfg["bucket_name"])
        # print(found)
        if not found:
            client.make_bucket(bucket_name=datalake_cfg["bucket_name"])
        else:
            print(
                f'Bucket {datalake_cfg["bucket_name"]} already exists, skip creating!'
            )

        upload_local_directory_to_minio(
            client,
            airflow_data["folder_path"],
            datalake_cfg["bucket_name"],
            datalake_cfg["folder_name"],
        )
        # # Upload files.
        # all_fps = glob(os.path.join(DATA_DIR, "green_*.parquet"))

        # for fp in all_fps:
        #     print(f"Uploading {fp}")
        #     client.fput_object(
        #         bucket_name=datalake_cfg["bucket_name"],
        #         object_name=os.path.join(
        #             datalake_cfg["folder_name"], os.path.basename(fp)
        #         ),
        #         file_path=fp,
        #     )

    (system_maintenance_task >> generate_deltalake() >> parquet2deltalake())
    # >> parquet2deltalake()
