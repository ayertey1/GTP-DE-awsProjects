from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.dates import days_ago
from airflow.exceptions import AirflowSkipException
from airflow.utils.trigger_rule import TriggerRule
import boto3
import pandas as pd
from io import StringIO
import logging
import json
from datetime import datetime

# === CONFIG ===
REDSHIFT_CONN_ID = 'postgres_conn_id'
S3_BUCKET = 'rawdata-dag-store'
PROCESSED_LOG_KEY = 'logs/processed_files.txt'
ROLE_ARN = 'arn:aws:iam::143320676204:role/RedshiftS3CopyRole'

default_args = {
    'owner': 'airflow',
    'retries': 1,
}

dag = DAG(
    dag_id='etl_pipeline_final_v3',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval='@daily',
    catchup=False,
    tags=['etl', 'mwaa', 'redshift']
)

def transform_and_kpi(**kwargs):
    logger = logging.getLogger(__name__)
    execution_date = kwargs['ds']
    s3 = boto3.client('s3')
    bucket = S3_BUCKET

    # Output paths (partitioned by date)
    merged_key = f'data/processed/{execution_date}/merged_stream_data.csv'
    genre_kpi_key = f'data/processed/{execution_date}/kpis/genre_kpis.csv'
    hourly_kpi_key = f'data/processed/{execution_date}/kpis/hourly_kpis.csv'
    log_key = f'logs/event_log_{execution_date}.csv'

    def log_event(event_type, message):
        log_time = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")
        dag_id = dag.dag_id
        task_id = 'transform_and_compute_kpis'  # Or use kwargs['task_instance'].task_id if dynamic

        # Build a single CSV line
        safe_message = message.replace('"', '""')  # Escape double quotes for CSV
        csv_line = f'{log_time},{dag_id},{task_id},{event_type},"{safe_message}"\n'


        try:
            existing = s3.get_object(Bucket=bucket, Key=log_key)['Body'].read().decode()
            csv_data = existing + csv_line
        except s3.exceptions.NoSuchKey:
            csv_data = csv_line  # Start fresh

        s3.put_object(Bucket=bucket, Key=log_key, Body=csv_data.encode())



    def read_csv_from_s3(key):
        obj = s3.get_object(Bucket=bucket, Key=key)
        return pd.read_csv(obj['Body'])

    def get_processed_keys():
        try:
            obj = s3.get_object(Bucket=bucket, Key=PROCESSED_LOG_KEY)
            return set(obj['Body'].read().decode().splitlines())
        except s3.exceptions.NoSuchKey:
            return set()

    def log_processed_keys(new_keys):
        all_keys = processed_keys.union(new_keys)
        s3.put_object(Bucket=bucket, Key=PROCESSED_LOG_KEY, Body='\n'.join(all_keys))

    def get_new_stream_data():
        new_files = []
        new_keys = set()
        paginator = s3.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=bucket, Prefix='data/streams/'):
            for obj in page.get('Contents', []):
                key = obj['Key']
                if key.endswith('.csv') and key not in processed_keys:
                    df = read_csv_from_s3(key)
                    new_files.append(df)
                    new_keys.add(key)
        return new_files, new_keys

    def compute_genre_kpis(df):
        genre_kpi = df.groupby('genre').agg(
            listen_count=('track_id', 'count'),
            avg_duration=('duration', 'mean'),
            most_popular_track=('title', lambda x: x.value_counts().idxmax())
        ).reset_index()
        genre_kpi['popularity_index'] = genre_kpi['listen_count'] * genre_kpi['avg_duration']
        return genre_kpi

    def compute_hourly_kpis(df):
        df['hour_bucket'] = pd.to_datetime(df['listen_time']).dt.floor('H')
        top_artist = df.groupby(['hour_bucket', 'artist'])['track_id'].count().reset_index()
        top_artist = top_artist.sort_values(['hour_bucket', 'track_id'], ascending=[True, False])
        top_artist = top_artist.drop_duplicates('hour_bucket').rename(columns={'artist': 'top_artist'})

        diversity = df.groupby('hour_bucket').agg(
            total_plays=('track_id', 'count'),
            unique_tracks=('track_id', 'nunique'),
            unique_listeners=('user_id', 'nunique')
        ).reset_index()
        diversity['track_diversity_index'] = diversity['unique_tracks'] / diversity['total_plays']

        hourly_kpi = pd.merge(diversity, top_artist, on='hour_bucket')[
            ['hour_bucket', 'unique_listeners', 'top_artist', 'track_diversity_index']
        ]
        return hourly_kpi

    log_event("INFO", "Pipeline started")

    songs_df = read_csv_from_s3('data/metadata/songs.csv')
    users_df = read_csv_from_s3('data/metadata/users.csv')
    processed_keys = get_processed_keys()
    new_files, new_keys = get_new_stream_data()

    if not new_files:
        log_event("SKIPPED", "No new stream files found.")
        raise AirflowSkipException("No new stream files.")

    stream_df = pd.concat(new_files, ignore_index=True)
    df = stream_df.merge(users_df, on='user_id').merge(songs_df, on='track_id')
    df.rename(columns={
        'track_genre': 'genre',
        'track_name': 'title',
        'artists': 'artist',
        'duration_ms': 'duration'
    }, inplace=True)

    s3.put_object(Bucket=bucket, Key=merged_key, Body=df.to_csv(index=False))
    log_event("SUCCESS", f"Merged data uploaded to {merged_key}")

    genre_kpi = compute_genre_kpis(df)
    hourly_kpi = compute_hourly_kpis(df)

    s3.put_object(Bucket=bucket, Key=genre_kpi_key, Body=genre_kpi.to_csv(index=False))
    log_event("SUCCESS", f"Genre KPIs uploaded to {genre_kpi_key}")

    s3.put_object(Bucket=bucket, Key=hourly_kpi_key, Body=hourly_kpi.to_csv(index=False))
    log_event("SUCCESS", f"Hourly KPIs uploaded to {hourly_kpi_key}")

    log_processed_keys(new_keys)
    log_event("COMPLETE", "Pipeline finished successfully")

# === DAG Tasks ===

transform_task = PythonOperator(
    task_id='transform_and_compute_kpis',
    python_callable=transform_and_kpi,
    provide_context=True,
    dag=dag,
)

load_merged = PostgresOperator(
    task_id='load_merged_to_staging',
    postgres_conn_id=REDSHIFT_CONN_ID,
    sql="""
        COPY staging_streams FROM 's3://rawdata-dag-store/data/processed/{{ ds }}/merged_stream_data.csv'
        IAM_ROLE 'arn:aws:iam::143320676204:role/RedshiftS3CopyRole'
        FORMAT AS CSV IGNOREHEADER 1;
    """,
    trigger_rule=TriggerRule.NONE_FAILED_OR_SKIPPED,
    dag=dag,
)

merge_streams = PostgresOperator(
    task_id='upsert_streams_to_final',
    postgres_conn_id=REDSHIFT_CONN_ID,
    sql="""
        BEGIN;
        DELETE FROM final_streams
        USING staging_streams
        WHERE final_streams.listen_time = staging_streams.listen_time
          AND final_streams.user_id = staging_streams.user_id
          AND final_streams.track_id = staging_streams.track_id;

        INSERT INTO final_streams
        SELECT * FROM staging_streams;
        COMMIT;
    """,
    trigger_rule=TriggerRule.NONE_FAILED_OR_SKIPPED,
    dag=dag,
)

load_genre_kpis = PostgresOperator(
    task_id='load_genre_kpis_to_redshift',
    postgres_conn_id=REDSHIFT_CONN_ID,
    sql="""
        COPY genre_kpis (genre, listen_count, avg_duration, most_popular_track, popularity_index)
        FROM 's3://rawdata-dag-store/data/processed/{{ ds }}/kpis/genre_kpis.csv'
        IAM_ROLE 'arn:aws:iam::143320676204:role/RedshiftS3CopyRole'
        FORMAT AS CSV IGNOREHEADER 1;
    """,
    trigger_rule=TriggerRule.NONE_FAILED_OR_SKIPPED,
    dag=dag,
)

load_hourly_kpis = PostgresOperator(
    task_id='load_hourly_kpis_to_redshift',
    postgres_conn_id=REDSHIFT_CONN_ID,
    sql="""
        COPY hourly_kpis (hour_bucket, unique_listeners, top_artist, track_diversity_index)
        FROM 's3://rawdata-dag-store/data/processed/{{ ds }}/kpis/hourly_kpis.csv'
        IAM_ROLE 'arn:aws:iam::143320676204:role/RedshiftS3CopyRole'
        FORMAT AS CSV IGNOREHEADER 1;
    """,
    trigger_rule=TriggerRule.NONE_FAILED_OR_SKIPPED,
    dag=dag,
)

load_etl_logs = PostgresOperator(
    task_id='load_etl_logs_to_redshift',
    postgres_conn_id=REDSHIFT_CONN_ID,
    sql="""
        COPY etl_logs (log_time, dag_id, task_id, event_type, message)
        FROM 's3://rawdata-dag-store/logs/event_log_{{ ds }}.csv'
        IAM_ROLE 'arn:aws:iam::143320676204:role/RedshiftS3CopyRole'
        FORMAT AS CSV;
    """,
    trigger_rule=TriggerRule.ALL_DONE,
    dag=dag,
)
# Task flow
transform_task >> load_merged >> merge_streams >> [load_genre_kpis, load_hourly_kpis] >> load_etl_logs

