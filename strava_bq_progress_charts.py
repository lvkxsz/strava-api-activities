from google.cloud import bigquery, storage
from pandas import to_datetime
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import seaborn as sns
import os
import tempfile

# get GCF local file dir
tmpdir = tempfile.gettempdir()

# GCP clients setup
bq_client = bigquery.Client()
storage_client = storage.Client()
bucket = storage_client.bucket(os.environ.get('gcs_bucket_name'))

def fetch_bq_df(sql, bq_client):
    
    # Executes sql job on BigQuery and returns the results as DataFrame
    
    sql_job = bq_client.query(sql)
    sql_res = sql_job.result()
    df = sql_res.to_dataframe()
    
    return df

def main(message, context):
        
    # =============================================================================
    # Running training performance over time
    # =============================================================================

    # define sql for run activities aggregated by day, for the below range of dates
    start_date = "'2022-07-01'"
    end_date = 'current_date'

    sql = (" with dates as ("
          "    select start_date"
          "    from unnest("
          "    generate_date_array(" + start_date + ", " + end_date + ", interval 1 day)"
          "    ) as start_date"
          " )"
          " select date(d.start_date) as start_date"
          "    ,sum(a.moving_time) as moving_time"
          "    ,sum(a.distance) as distance"
          "    ,round(avg(a.average_heartrate),0) as average_heartrate"
          " from dates d"
          "    left join strava.activities a"
          "      on date(a.start_date) = d.start_date "
          "      and a.type = 'Run'"
          " group by 1")

    # fetch bigquery results
    df = fetch_bq_df(sql, bq_client)

    # calculated fields
    df['distance_km'] = df['distance']/1000
    df['average_pace_decimal'] = (df['moving_time']/60)/df['distance_km']
    df['average_pace_dt'] = to_datetime(df['average_pace_decimal'], unit='m')

    # rename fields for visual
    df = df.rename(columns={'distance_km': 'Distance (km)',
                            'average_heartrate': 'Average HR (bpm)'})

    # plot
    file_name = 'progress_chart_run.png'
    file_path = tmpdir + '/' + file_name
    sns.set_context("talk",font_scale=1.1)
    plt.figure(figsize=(16,9))

    sc = sns.scatterplot(x = df['start_date'], 
                        y = df['average_pace_dt'],
                        size = df['Distance (km)'],
                        sizes = (50,1500),
                        alpha = 0.8,
                        hue = df['Average HR (bpm)'],
                        palette = "viridis")

    plt.legend(bbox_to_anchor=(1.01,1), borderaxespad=0, labelspacing=1.2)
    plt.xlabel(None)
    plt.ylabel("Average pace (min / km)")
    plt.gca().yaxis.set_major_formatter(mdates.DateFormatter('%M:%S'))
    plt.title('Running training performance over time')
    plt.savefig(file_path, bbox_inches='tight')

    # upload png file to Cloud Storage
    bucket.blob(file_name).upload_from_filename(file_path, content_type='image/png')


    # =============================================================================
    # Road cycling training performance over time
    # =============================================================================

    # define sql for run activities aggregated by day, for the below range of dates
    # note that type=VirtualRide are included
    start_date = "'2022-03-01'"
    end_date = 'current_date'

    sql = (" with dates as ("
          "    select start_date"
          "    from unnest("
          "    generate_date_array(" + start_date + ", " + end_date + ", interval 1 day)"
          "    ) as start_date"
          " )"
          " select date(d.start_date) as start_date"
          "    ,sum(a.moving_time) as moving_time"
          "    ,sum(a.distance) as distance"
          "    ,sum(a.total_elevation_gain) as total_elevation_gain"
          "    ,round(avg(a.average_watts),0) as average_watts"
          " from dates d"
          "    left join strava.activities a"
          "      on date(a.start_date) = d.start_date "
          "      and a.type in ('Ride','VirtualRide')"
          " group by 1")

    # fetch bigquery results
    df = fetch_bq_df(sql, bq_client)

    # calculated fields
    df['distance_km'] = df['distance']/1000
    df['average_speed'] = df['distance_km']/(df['moving_time']/3600)

    # rename fields for visual
    df = df.rename(columns={'distance_km': 'Distance (km)',
                            'total_elevation_gain': 'Elevation (m)',
                            'average_speed': 'Average Speed (km / h)',
                            'average_watts': 'Average Power (W)'})

    # plot with average_speed on y-axis
    file_name = 'progress_chart_ride_speed.png'
    file_path = tmpdir + '/' + file_name
    sns.set_context("talk",font_scale=1.1)
    plt.figure(figsize=(16,9))

    sc = sns.scatterplot(x = df['start_date'], 
                        y = df['Average Speed (km / h)'],
                        size = df['Distance (km)'],
                        sizes = (50,1200),
                        alpha = 0.8,
                        hue = df['Elevation (m)'],
                        palette="flare")

    plt.legend(bbox_to_anchor=(1.01,1), borderaxespad=0, labelspacing=1.2)
    plt.ylabel("Average Speed (km / h)")
    plt.title('Road cycling training performance over time')
    plt.savefig(file_path, bbox_inches='tight')

    # upload png file to Cloud Storage
    bucket.blob(file_name).upload_from_filename(file_path, content_type='image/png')

    # plot with average_power on y-axis
    file_name = 'progress_chart_ride_power.png'
    file_path = tmpdir + '/' + file_name
    sns.set_context("talk",font_scale=1.1)
    plt.figure(figsize=(16,9))

    sc = sns.scatterplot(x = df['start_date'], 
                        y = df['Average Power (W)'],
                        size = df['Distance (km)'],
                        sizes = (50,1200),
                        alpha = 0.8,
                        hue = df['Elevation (m)'],
                        palette="flare")

    plt.legend(bbox_to_anchor=(1.01,1), borderaxespad=0, labelspacing=1.2)
    plt.ylabel("Average Power (W)")
    plt.title('Road cycling training performance over time')
    plt.savefig(file_path, bbox_inches='tight')

    # upload png file to Cloud Storage
    bucket.blob(file_name).upload_from_filename(file_path, content_type='image/png')
