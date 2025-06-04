from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
import pendulum

import requests
import pandas as pd
import time
from datetime import datetime
import os
import boto3

WORKFLOW_SCHEDULE = "0 */2 * * *" #every 2 hours

# Define the default args dictionary
default_args = {
    'owner': 'Dinglin',
    'depends_on_past': False,
    'start_date': pendulum.today('UTC').add(days=-1),  # Updated from days_ago(1)
    'retries': 1,
}

# Set up constants
def weather_upload(**kwargs):
    WEATHER_STATIONS = ["KORD", "KENW", "KMDW", "KPNT"]
    BASE_URL = "https://api.weather.gov/stations/{station}/observations/latest"
    S3_BUCKET = "hayden-rudolph-shlipak-mwaa"  # <-- replace with your actual bucket name
    S3_DIR = "weather_data"

    s3_client = boto3.client("s3")  # No credentials passed


    # Timestamp of the collection
    time_of_collection = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')

    # Prepare list for collected records
    records = []

    # Collect data
    for station in WEATHER_STATIONS:
        url = BASE_URL.format(station=station)
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json().get("properties", {})
            records.append({
                "timeOfCollection": time_of_collection,
                "timestamp": data.get("timestamp"),
                "station": station,
                "temperature": data.get("temperature", {}).get("value"),
                "dewpoint": data.get("dewpoint", {}).get("value"),
                "windSpeed": data.get("windSpeed", {}).get("value"),
                "barometricPressure": data.get("barometricPressure", {}).get("value"),
                "visibility": data.get("visibility", {}).get("value"),
                "precipitationLastHour": data.get("precipitationLastHour", {}).get("value"),
                "relativeHumidity": data.get("relativeHumidity", {}).get("value"),
                "heatIndex": data.get("heatIndex", {}).get("value")
            })
        else:
            print(f"Failed to fetch data for station {station}")
        time.sleep(2)

    # Create DataFrame
    df = pd.DataFrame(records)

    # Save locally first (optional)
    filename = f"weather_obs_{datetime.utcnow().strftime('%Y%m%dT%H%M%S')}.csv"
    local_path = f"{filename}"
    df.to_csv(local_path, index=False)

    # Upload to S3
    s3_key = os.path.join(S3_BUCKET, S3_DIR, local_path)
    s3_client.upload_file(local_path, S3_BUCKET, s3_key)
    print(f"✅ Data uploaded to s3://{S3_BUCKET}/{s3_key}")


dag = DAG(
    'weather_upload_dag',
    default_args = default_args,
    description = 'Get weather data every two hours and upload to S3',
    schedule=WORKFLOW_SCHEDULE,  # Updated from schedule_interval
    tags=["de300"],
    end_date=datetime(2025, 6, 6),  # runs for 6/4 and 6/5
    catchup=False,

)

task_weather_upload = PythonOperator(
    task_id='task_weather_upload',
    python_callable=weather_upload,
    dag=dag,
)