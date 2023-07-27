from __future__ import annotations

import os
import json
import requests
import awswrangler as wr
import boto3
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv

import pendulum

from airflow.decorators import dag, task


@dag(
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["meteo_api"],
)
def etl_meteo_api():

    @task()
    def extract():
        """
        Extract task
        """
        x = requests.get('https://api.open-meteo.com/v1/forecast?latitude=-21.22&longitude=-44.99&hourly=temperature_2m')
        text = x.text
        dictionary = json.loads(text)
        
        return dictionary


    @task()
    def transform(raw_data: dict):
        """
        Transform task
        """
        df = pd.json_normalize(raw_data)
        df = df.explode(['hourly.time', 'hourly.temperature_2m'])

        return df

    @task()
    def load(transformed_data: pd.DataFrame):
        """
        Load task
        """
        load_dotenv()
        now = datetime.now()

        year = now.strftime("%Y")
        month = now.strftime("%m")
        day = now.strftime("%d")
        time = now.strftime("%H:%M:%S")

        session = boto3.Session(
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'), # AWS Secret Manager
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
            region_name="us-east-2"
        )

        wr.s3.to_parquet(
            df=transformed_data,
            path='s3://dee-tutorial/open-meteo/' + year + '/' + month + '/' + day + '/' + time + '.parquet',
            boto3_session=session,
        )

    # main flow
    raw_data = extract()
    transformed_data = transform(raw_data)
    load(transformed_data)


etl_meteo_api()
