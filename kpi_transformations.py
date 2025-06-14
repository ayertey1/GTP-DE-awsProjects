import pandas as pd
from io import BytesIO
import boto3

def compute_genre_kpis(**kwargs):
    s3 = boto3.client('s3')

    # Read CSVs from S3
    def read_s3_csv(bucket, key):
        obj = s3.get_object(Bucket=bucket, Key=key)
        return pd.read_csv(BytesIO(obj['Body'].read()))

    songs = read_s3_csv('rawdata-dag-store', 'data/songs.csv')
    streams = read_s3_csv('rawdata-dag-store', 'data/streams1.csv')  # expand if needed

    # Merge
    merged = streams.merge(songs, left_on='track_id', right_on='song_id')

    # KPIs
    genre_kpis = merged.groupby('genre').agg(
        listen_count=('track_id', 'count'),
        avg_track_duration=('duration', 'mean')
    ).reset_index()

    # Most popular track per genre
    top_tracks = (
        merged.groupby(['genre', 'title'])
        .size()
        .reset_index(name='play_count')
        .sort_values(['genre', 'play_count'], ascending=[True, False])
        .drop_duplicates('genre')
    )

    genre_kpis = genre_kpis.merge(top_tracks[['genre', 'title']], on='genre')
    genre_kpis.rename(columns={'title': 'most_popular_track'}, inplace=True)

    # Save to CSV and upload to S3
    out_key = 'output/genre_kpis.csv'
    out_buffer = BytesIO()
    genre_kpis.to_csv(out_buffer, index=False)
    s3.put_object(Bucket='rawdata-dag-store', Key=out_key, Body=out_buffer.getvalue())



def compute_hourly_kpis(**kwargs):
    s3 = boto3.client('s3')

    def read_s3_csv(bucket, key):
        obj = s3.get_object(Bucket=bucket, Key=key)
        return pd.read_csv(BytesIO(obj['Body'].read()))

    streams = read_s3_csv('rawdata-dag-store', 'data/streams1.csv')
    songs = read_s3_csv('rawdata-dag-store', 'data/songs.csv')

    df = streams.merge(songs, left_on='track_id', right_on='song_id')
    df['listen_hour'] = pd.to_datetime(df['listen_time']).dt.strftime('%Y-%m-%d %H:00:00')

    hourly_kpis = df.groupby('listen_hour').agg(
        unique_listeners=('user_id', 'nunique'),
        total_plays=('track_id', 'count'),
        unique_tracks=('track_id', 'nunique')
    ).reset_index()

    # Diversity index
    hourly_kpis['track_diversity_index'] = hourly_kpis['unique_tracks'] / hourly_kpis['total_plays']

    # Top artist per hour
    top_artists = (
        df.groupby(['listen_hour', 'artist'])
        .size()
        .reset_index(name='play_count')
        .sort_values(['listen_hour', 'play_count'], ascending=[True, False])
        .drop_duplicates('listen_hour')
    )

    result = hourly_kpis.merge(top_artists[['listen_hour', 'artist']], on='listen_hour')
    result.rename(columns={'artist': 'top_artist'}, inplace=True)

    # Save and upload
    out_key = 'output/hourly_kpis.csv'
    out_buffer = BytesIO()
    result.to_csv(out_buffer, index=False)
    s3.put_object(Bucket='rawdata-dag-store', Key=out_key, Body=out_buffer.getvalue())
