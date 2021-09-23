
#Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
#SPDX-License-Identifier: Apache-2.0

import os
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
import awswrangler as wr
from airflow.utils.dates import days_ago

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date" : days_ago(1),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5)
}

def ts_query(**kwargs):
    # get time window for query - uses the delta of the previous and this window
    # must be an idemopotent value, i.e every time it runs it will create the same output
    start = str(kwargs['execution_date']).replace("T", " ")
    finish = str(kwargs['next_execution_date']).replace("T", " ")
    
    # get an idemopotent folder value for the folder
    execution_time = str(kwargs['execution_date']).replace("T", "-")
    s3folder = execution_time[0:13]
    print ("Data will be uploaded to {target}".format(target=s3folder))
    query = "SELECT instance_name, BIN(time, 15s) AS binned_timestamp, ROUND(AVG(measure_value::double), 2) AS avg_cpu_utilization, ROUND(APPROX_PERCENTILE(measure_value::double, 0.9), 2) AS p90_cpu_utilization, ROUND(APPROX_PERCENTILE(measure_value::double, 0.95), 2) AS p95_cpu_utilization, ROUND(APPROX_PERCENTILE(measure_value::double, 0.99), 2) AS p99_cpu_utilization FROM demoAirflow.kinesisdata1 WHERE measure_name = 'cpu_nice' AND time BETWEEN '{start}' AND '{end}' GROUP BY region, instance_name, availability_zone, BIN(time, 15s) ORDER BY binned_timestamp ASC ".format(start=start,end=finish)
    print("Query to be run: {query}".format(query=query))
    try:
        wr.s3.to_csv(df=wr.timestream.query(query), path='s3://demo-airflow-ts-output/{s3folder}/my_file.csv'.format(s3folder=s3folder,))
        print ("Timestream query processed successfully and copied to {s3folder}".format(s3folder=s3folder))
    except ValueError:
        print("Query returned no values - no data uploaded")
    except wr.exceptions.EmptyDataFrame:
        print("Query returned nothing - no data uploaded")

with DAG(
        dag_id=os.path.basename(__file__).replace(".py", ""),
        default_args=default_args,
        dagrun_timeout=timedelta(hours=2),
        # set to every 10 mins for demo
        #schedule_interval="*/10 * * * *"
        # set to every 2 hours for demo
        #schedule_interval="0 */2 * * *"
        # set to every hour
        schedule_interval="0 */1 * * *"

) as dag:

    ts_query=PythonOperator(task_id='ts_query', python_callable=ts_query, dag=dag)
    ts_query
