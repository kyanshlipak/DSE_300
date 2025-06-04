# Homework 4: Apache Airflow

## Overview

This assignment involves building a simple data pipeline using Apache Airflow to collect, store, and analyze weather observation data. The pipeline is deployed on AWS using Managed Workflows for Apache Airflow (MWAA) and stores data in an S3 bucket.

The three primary tasks implemented in this pipeline are:

1. **Data Collection** – Collect weather data every two hours from four specified weather stations.
2. **Dashboard Generation** – Generate daily plots for key weather variables. This DAG runs every day at 3am.
3. **Temperature Prediction** – Train and apply a linear regression model to forecast temperature.

## Team Information

Please refer to `info.txt` for team member names and the corresponding MWAA and S3 environment details.

---

## How to Run the Code

### Prerequisites

- Python 3.8+
- Apache Airflow (via MWAA)
- boto3, pandas, requests, matplotlib, scikit-learn (install in MWAA environment using requirements.txt)
- Your environment must be deployed with a valid IAM role and policies granting S3 and MWAA permissions.

### DAG Descriptions

#### 1. `weather_upload_dag`

- **Schedule**: Every 2 hours
- **Function**: Pulls latest weather observations from NOAA for stations `["KORD", "KENW", "KMDW", "KPNT"]`.
- **Output**: CSV files saved to S3 at /`hayden-rudolph-shlipak-mwaa/weather_data/`.

#### 2. `daily_weather_dashboard_dag`

- **Schedule**: Daily
- **Function**: Aggregates the last 24 hours of observations and creates plots for:
  - Temperature
  - Visibility
  - Relative Humidity
- **Output**: PNG images saved to S3 at /`hayden-rudolph-shlipak-mwaa/output_graphs/`.

#### 3. `temperature_prediction_dag`

- **Schedule**: Triggered only twice (after 20 and 40 hours)
- **Function**:
  - Trains linear regression models per station.
  - Uses available predictors (e.g., dewpoint, humidity) to predict temperature 0.5–8 hours into the future.
- **Output**: CSV of model predictions saved to S3 at `predictions/`.

---

## Expected Outputs

| Output Directory   | File Type | Description                                          |
| ------------------ | --------- | ---------------------------------------------------- |
| `weather_data/`  | `.csv`  | Timestamped weather observations from each run       |
| `output_graphs/` | `.png`  | Daily graphs of temp, RH, and visibility per station |
| `predictions/`   | `.csv`  | Model-generated temperature predictions              |

---

## File Structure

```bash
hayden-rudolph-shlipak-mwaa/
├── dags/
│   ├── plot_weather.py
│   ├── weather_upload.py
│   └── prediction_upload.py
├── hayden-rudolph-shlipak-mwaa/
│   ├── weather_data/
│   ├── output_graphs/
│   ├── predictions/
├── info.txt
├── readme.md
```


## Generative AI Disclosure

We used Generative AI tools for:

* **Tool** : ChatGPT (OpenAI GPT-4o)
* **Purpose** : Formatting this README.md and clarifying Airflow DAG logic.
* **Prompts** :
* “Given this outline, put together markdown formatting for readme.”
* “Clarify how to schedule a DAG in Apache Airflow for every 2 hours.”
