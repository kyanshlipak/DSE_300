# IMPORTS
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pendulum

import boto3
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import os
import io

# === DAG Metadata ===
default_args = {
    'owner': 'Dinglin',
    'depends_on_past': False,
    'start_date': pendulum.today('UTC').add(days=-1),
    'retries': 1,
}

SCHEDULE = "0 3 * * *"  # Run once daily at 3 AM UTC

# === S3 Parameters ===
S3_BUCKET = "hayden-rudolph-shlipak-mwaa"  # <-- replace with your actual bucket name
INPUT_DIR = "hayden-rudolph-shlipak-mwaa/weather_data"
OUTPUT_DIR = "output_graphs"


# === Main Task ===
def create_dashboard_from_s3(**kwargs):
	s3 = boto3.client("s3")

	# Target yesterday's date
	target_date = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")
	print(f"📆 Target date: {target_date}")

	s3_resource = boto3.resource('s3')
	bucket = s3_resource.Bucket(S3_BUCKET)

	# Temporary directory to save files
	local_dir = "/tmp/weather_csvs"
	os.makedirs(local_dir, exist_ok=True)

	csv_paths = []

	for obj in bucket.objects.filter(Prefix=INPUT_DIR):
		s3_key = obj.key
		if s3_key.endswith("/"):  # Skip directories
			continue

		relative_path = os.path.relpath(s3_key, INPUT_DIR)
		local_file_path = os.path.join(local_dir, relative_path)
		os.makedirs(os.path.dirname(local_file_path), exist_ok=True)

		bucket.download_file(s3_key, local_file_path)
		print(f"📥 Downloaded s3://{S3_BUCKET}/{s3_key} to {local_file_path}")
		csv_paths.append(local_file_path)

	print(f"📂 Found {len(csv_paths)} CSV files in S3 bucket {S3_BUCKET} under {INPUT_DIR}")

	# Read CSVs
	combined_dfs = []
	for path in csv_paths:
		df = pd.read_csv(path)
		if df.empty:
			continue
		df["timestamp"] = pd.to_datetime(df["timestamp"])
		df["date"] = df["timestamp"].dt.date.astype(str)
		if df["date"].iloc[0] == target_date:
			combined_dfs.append(df)

	if not combined_dfs:
		print(f"No data found for {target_date}")
		return

	df_all = pd.concat(combined_dfs).sort_values("timestamp")
	print(f"📊 Loaded {len(df_all)} records from {len(combined_dfs)} files")

	# Mapping of variable names to y-axis labels with units
	y_axis_labels = {
		"temperature": "Temperature (°C)",
		"visibility": "Visibility (m)",
		"relativeHumidity": "Relative Humidity (%)"
	}

	for var in ["temperature", "visibility", "relativeHumidity"]:
		plt.figure(figsize=(10, 5))
		for station, group in df_all.groupby("station"):
			plt.plot(
				group["timestamp"],
				group[var],
				label=station,
				marker='o',
				markersize=10,
				linewidth=3
			)

		plt.title(f"{var} on {target_date}", fontsize=18)
		plt.xlabel("Time (UTC)", fontsize=14)
		plt.ylabel(y_axis_labels.get(var, var), fontsize=14)
		plt.xticks(fontsize=12)
		plt.yticks(fontsize=12)
		plt.legend(fontsize=12)
		plt.tight_layout()

		# Format timestamps to HH:MM
		ax = plt.gca()
		ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
		plt.setp(ax.get_xticklabels(), rotation=45, ha='right')

		# Save PNG to file first
		filename = f"{var}_{target_date}.png"
		local_path = os.path.join("/tmp", filename)
		plt.savefig(local_path)
		plt.close()

		# Upload to S3
		s3_key = os.path.join(S3_BUCKET, OUTPUT_DIR, filename)
		s3.upload_file(local_path, S3_BUCKET, s3_key)
		print(f"✅ Uploaded to s3://{S3_BUCKET}/{s3_key}")

# === Define DAGs ===
dag = DAG(
    'weather_dashboard_dag',
    default_args=default_args,
    description='Create daily weather dashboard from S3 data',
    schedule=SCHEDULE,
    catchup=False,
    tags=["de300"],
	end_date=datetime(2025, 6, 6),  # runs for 6/4 and 6/5
)

dag_immediate = DAG(
    'weather_dashboard_on_upload',
    default_args=default_args,
	description='Create weather dashboard immediately on upload',
    schedule=None,
    catchup=False,
    tags=["de300"]
)

# === Define Task ===
dashboard_task = PythonOperator(
    task_id='create_dashboard_from_s3',
    python_callable=create_dashboard_from_s3,
    dag=dag,
)

# === Define Task for immediate-trigger DAG ===
dashboard_task_immediate = PythonOperator(
    task_id='create_dashboard_from_s3',
    python_callable=create_dashboard_from_s3,
    dag=dag_immediate,
)