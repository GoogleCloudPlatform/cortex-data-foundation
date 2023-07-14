# Copyright 2023 Google LLC

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#     https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Holiday Calendar DAG"""

import holidays
import configparser
import pandas as pd

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta

default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}


def load_holidays():
    holiday_ip_path = ("/home/airflow/gcs/dags/holiday_calendar/"
                       "holiday_calendar.ini")
    config = configparser.ConfigParser()
    config.read(holiday_ip_path)
    country_list = config.get("holiday", "country_list")
    country_list = country_list.split(",")
    years = config.get("holiday", "year_list")
    years = years.split(",")
    years = list(map(int, years))
    dataset_cdc_processed = config.get("holiday", "dataset_cdc_processed")
    location = config.get("holiday", "location").lower()
    target_table = f"{dataset_cdc_processed}.holiday_calendar"
    project_id = config.get("holiday", "project_id")
    write_mode = config.get("holiday", "write_mode")
    column_names = ["HolidayDate", "Description", "CountryCode", "Year"]
    df = pd.DataFrame(columns=column_names)
    for country in country_list:
        for year in years:
            temp = pd.DataFrame(holidays.country_holidays(
                country=country, years=year).items(),
                                columns=["HolidayDate", "Description"])
            temp["CountryCode"] = country
            temp["Year"] = str(year)
            df = pd.concat([df, temp])
    df["HolidayDate"] = pd.to_datetime(df.HolidayDate, format="%Y-%m-%d")
    df["WeekDay"] = df["HolidayDate"].apply(lambda x: x.day_name())
    df["QuarterOfYear"] = df["HolidayDate"].apply(lambda x: x.quarter)
    df["Week"] = df["HolidayDate"].apply(lambda x: x.week)
    df["HolidayDate"] = df["HolidayDate"].apply(
                                            lambda x: x.strftime("%Y-%m-%d"))
    # TODO (vladkol): migrate from pandas to bigquery.Client.insert_rows
    df.to_gbq(target_table,
              table_schema=[
                  {"name": "HolidayDate", "type": "STRING"},
                  {"name": "Description", "type": "STRING"},
                  {"name": "CountryCode", "type": "STRING"},
                  {"name": "Year", "type": "STRING"},
                  {"name": "WeekDay", "type": "STRING"},
                  {"name": "QuarterOfYear", "type": "INTEGER"},
                  {"name": "Week", "type": "INTEGER"},],
              project_id=project_id,
              location=location,
              progress_bar=False,
              if_exists=write_mode) # type: ignore


with DAG(
        "Holiday_Calendar",
        default_args=default_args,
        description="Holiday Calendar For Multiple Years",
        schedule_interval="@yearly",
        start_date=datetime(2021, 1, 1),
        catchup=False,
        max_active_runs=1,
        tags=["API"],
) as dag:
    start_task = DummyOperator(task_id="start")
    t1 = PythonOperator(
        task_id="calendar",
        python_callable=load_holidays,
        dag=dag,
    )
    stop_task = DummyOperator(task_id="stop")

start_task >> t1 >> stop_task
