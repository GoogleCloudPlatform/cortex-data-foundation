# Copyright 2024 Google LLC

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#     https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Airflow DAG template for Trends Interest Time."""

import ast
import configparser
from datetime import date
from datetime import datetime
from datetime import timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
import pandas as pd
import pandas_gbq
from pytrends.request import TrendReq

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def get_trends():
    ini_path = "/home/airflow/gcs/dags/trends/trends.ini"
    config = configparser.ConfigParser()
    config.read(ini_path)
    ip = config.get("trends", "ip")
    ip = ast.literal_eval(ip)
    write_mode = config.get("trends", "write_mode")
    start_date = config.get("trends", "start_date")
    project_id_src = config.get("trends", "project_id_src")
    dataset_cdc_processed = config.get("trends", "dataset_cdc_processed")
    ## CORTEX-CUSTOMER: Uncomment for more results in additional tables
    # target_table_top =
    #f"{dataset_cdc_processed}.trends_top_related_queries"
    # target_table_rising =
    #f"{dataset_cdc_processed}.trends_rising_related_queries"
    target_trends_table = f"{dataset_cdc_processed}.trends"

    #TODO(): get the last run date for each category and run from there
    # or use initial date
    ## CORTEX-CUSTOMER: Uncomment for more results in additional tables
    # tdf = pd.DataFrame()
    # rdf = pd.DataFrame()
    iot = pd.DataFrame()

    ## start date in the first run be from the start date in the .ini file
    ## The hierarchy texts can be filled from prod_hierarchy_texts sql
    for k, v in ip.items():
        kw_list = k.split(",")
        geo = v.split(",")[0]
        prodh = v.split(",")[1]
        end_date = date.today()
        timeframe = f"{start_date} {end_date}"
        ## CORTEX-CUSTOMER:After first run - can change to shorter rolling
        ## timeframe:
        ##  timeframe="today 7-d"
        gprop = ""
        pytrend = TrendReq()
        pytrend.build_payload(kw_list=kw_list,
                                geo=geo,
                                timeframe=timeframe,
                                gprop=gprop)

        interest_over_timedf = pytrend.interest_over_time()
        interest_over_timedf.drop("isPartial", axis=1, inplace=True)
        interest_over_timedf.columns = ["InterestOverTime"]
        interest_over_timedf["WeekStart"] = interest_over_timedf.index
        interest_over_timedf = interest_over_timedf.reset_index(drop=True)
        interest_over_timedf["CountryCode"] = geo
        interest_over_timedf["HierarchyId"] = prodh
        interest_over_timedf["HierarchyText"] = kw_list[0]
        interest_over_timedf = interest_over_timedf[[
            "WeekStart", "InterestOverTime", "CountryCode", "HierarchyId",
            "HierarchyText"
        ]]
        interest_over_timedf["WeekStart"] = pd.to_datetime(
            interest_over_timedf["WeekStart"]).dt.strftime("%Y-%m-%d")
        frames = [iot, interest_over_timedf]
        iot = pd.concat(frames)

        ## CORTEX-CUSTOMER: Uncomment for more results in additional tables
        # related_queries_output = pytrend.related_queries()
        # df = related_queries_output.values()
        # top_df = list(df)[0]["top"]
        # top_df.columns = ["SearchTerm", "InterestOverTime"]
        # top_df["Trend_Search_Start_Date"] = start_date
        # top_df["Trend_Search_End_Date"] = end_date
        # top_df["Trend_Extract_Date"] = date.today()
        # frames = [tdf, top_df]
        # tdf = pd.concat(frames)

        # rising_df = list(df)[0]["rising"]
        # rising_df.columns = ["SearchTerm", "InterestOverTime"]
        # rising_df["Trend_Search_Start_Date"] = start_date
        # rising_df["Trend_Search_End_Date"] = end_date
        # rising_df["Trend_Extract_Date"] = date.today()
        # frames = [rdf, rising_df]
        # rdf = pd.concat(frames)
    # pandas_gbq.to_gbq(tdf,
    #                   target_table_top,
    #                   project_id=project_id_src,
    #                   if_exists=write_mode)
    # pandas_gbq.to_gbq(rdf,
    #                   target_table_rising,
    #                   project_id=project_id_src,
    #                   if_exists=write_mode)

    pandas_gbq.to_gbq(iot,
                        target_trends_table,
                        project_id=project_id_src,
                        if_exists=write_mode)

with DAG("Trends_Interest_Time",
         default_args=default_args,
         description="Trends - Interest Over Time",
         schedule_interval="@weekly",
         start_date=datetime(2021, 1, 1),
         catchup=False,
         max_active_runs=1,
         tags=["API"]) as dag:
    start_task = EmptyOperator(task_id="start")
    t1 = PythonOperator(task_id="interest_over_time",
                        python_callable=get_trends,
                        dag=dag)

    stop_task = EmptyOperator(task_id="stop")

# pylint: disable=pointless-statement
start_task >> t1 >> stop_task
