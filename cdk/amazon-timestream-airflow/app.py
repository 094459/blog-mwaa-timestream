#!/usr/bin/env python3

from aws_cdk import core

from amazon_timestream_airflow.amazon_timestream_airflow_stack import AmazonTimestreamAirflowStack

env_EU=core.Environment(region="eu-west-1", account="704533066374")
ts_props = {'timestream_db': 'airflow-db','timestream_table' : 'airflow-table','s3_export' : 'demo-airflow-ts-output'}

app = core.App()


timestream = AmazonTimestreamAirflowStack(
    scope=app,
    construct_id="timestream",
    env=env_EU,
    ts_props=ts_props
)

app.synth()

#import io
