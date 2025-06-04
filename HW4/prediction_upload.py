from airflow import DAG
from airflow.operators.python import PythonOperator
import pendulum

import boto3
import pandas as pd
import numpy as np
import os
import io
from datetime import datetime, timedelta
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import OneHotEncoder

# Schedule: no automatic schedule, you will trigger this manually at 20h and 40h
WORKFLOW_SCHEDULE = None

# Default args
default_args = {
    'owner': 'Dinglin',
    'depends_on_past': False,
    'start_date': pendulum.today('UTC').add(days=-1),
    'retries': 1,
}

# Main function
def train_and_predict(**kwargs):
    S3_BUCKET = "hayden-rudolph-shlipak-mwaa"
    WEATHER_DIR = "hayden-rudolph-shlipak-mwaa/weather_data"
    PREDICTIONS_DIR = "hayden-rudolph-shlipak-mwaa/predictions"

    s3_client = boto3.client("s3")

    # Read all weather data files from S3
    response = s3_client.list_objects_v2(Bucket=S3_BUCKET, Prefix=WEATHER_DIR)
    files = [obj['Key'] for obj in response.get('Contents', []) if obj['Key'].endswith('.csv')]

    all_dfs = []
    for file in files:
        obj = s3_client.get_object(Bucket=S3_BUCKET, Key=file)
        df = pd.read_csv(io.BytesIO(obj['Body'].read()))
        all_dfs.append(df)

    if not all_dfs:
        print("No data available to train.")
        return

    df = pd.concat(all_dfs, ignore_index=True)

    # Clean & prepare data
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df.dropna(subset=['temperature'], inplace=True)
    df['hour'] = df['timestamp'].dt.hour
    df['minute'] = df['timestamp'].dt.minute

    encoder = OneHotEncoder(sparse_output=False).fit(df[['station']])
    station_encoded = encoder.transform(df[['station']])

    features = np.hstack([
        station_encoded,
        df[['dewpoint', 'windSpeed', 'barometricPressure', 'visibility', 'relativeHumidity']].fillna(0),
        df[['hour', 'minute']]
    ])

    target = df['temperature']

    model = LinearRegression()
    model.fit(features, target)

    # Future predictions (next 8 hours at 30min intervals = 16 steps)
    now = datetime.utcnow()
    future_times = pd.date_range(now, periods=16, freq="30min")

    prediction_rows = []
    for station in ["KORD", "KENW", "KMDW", "KPNT"]:
        for time in future_times:
            hour = time.hour
            minute = time.minute
            avg_features = df[['dewpoint', 'windSpeed', 'barometricPressure', 'visibility', 'relativeHumidity']].mean().values
            station_onehot = encoder.transform([[station]])[0]
            full_features = np.hstack([station_onehot, avg_features, [hour, minute]])
            predicted_temp = model.predict([full_features])[0]

            prediction_rows.append({
                "prediction_time": time,
                "station": station,
                "predicted_temp": predicted_temp
            })

    pred_df = pd.DataFrame(prediction_rows)

    # Save locally
    filename = f"predictions_{datetime.utcnow().strftime('%Y%m%dT%H%M%S')}.csv"
    local_path = f"{filename}"
    pred_df.to_csv(local_path, index=False)

    # Upload to S3
    s3_key = os.path.join(PREDICTIONS_DIR, filename)
    s3_client.upload_file(local_path, S3_BUCKET, s3_key)

    print(f"Predictions uploaded to s3://{S3_BUCKET}/{s3_key}")

# Define DAG
dag = DAG(
    'regression_prediction_dag',
    default_args=default_args,
    description='Train linear regression model and predict temperature',
    schedule_interval = "0 */20 * * *",
    tags=["de300"]
)

task_train_predict = PythonOperator(
    task_id='task_train_predict',
    python_callable=train_and_predict,
    dag=dag,
)
