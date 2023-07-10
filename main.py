from google.cloud import storage
import requests
import json
import time
import pandas as pd
import datetime as dt
import os

def refresh_strava_api_tokens(client_id, client_secret_mount_ref, refresh_token_mount_ref):
    
    # Refreshes Strava API tokens using Client Secret and Refresh Token mounted from GCP Secret Manager
    # https://developers.strava.com/docs/authentication/#refreshingexpiredaccesstokens
    # https://cloud.google.com/functions/docs/configuring/secrets

    # Params:   client_id                   API Cliend ID from Strava API App configuration
    #           client_secret_mount_ref     Reference to Strava API Client Secret string, mounted locally
    #           refresh_token_mount_ref     Reference to Strava API Refresh Token string, mounted locally

    # Returns JSON fields: token_type, access_token, expires_at, expires_in, refresh_token
    
    # Retrieve Client Secret and Refresh Token from Secret Manager
    with open(client_secret_mount_ref) as f:
        client_secret = f.readlines()[0]

    with open(refresh_token_mount_ref) as f:
        refresh_token = f.readlines()[0]
    
    # Strava OAuth API request
    payload = {"client_id": client_id,
               "client_secret": client_secret,
               "grant_type": "refresh_token",
               "refresh_token": refresh_token}
    
    res = requests.post(url="https://www.strava.com/api/v3/oauth/token", data = payload)
    
    return res


def get_recent_strava_activities(access_token, created_before, created_after):
    
    # Retrieves Athlete Activities created within the specified time window from Strava API 
    # https://developers.strava.com/docs/reference/#api-Activities

    # Params:    access_token    API Access Token string for Bearer authorization
    #            created_before  End of lookback window (Unix timestamp)
    #            created_after   Beginning of lookback window (Unix timestamp)

    # Returns JSON string containing activity details
    
    # 1st request: list athlete Activity IDs
    # https://developers.strava.com/docs/reference/#api-Activities-getLoggedInAthleteActivities
    headers = {'Authorization': 'Bearer ' + access_token}
    ids = []
    per_page = 200
    page = 1
    
    while page > 0:
        
        url = ("https://www.strava.com/api/v3/athlete/activities"
               "?after=" + str(created_after) + 
               "&before=" + str(created_before) + 
               "&per_page=" + str(per_page) + 
               "&page=" + str(page))
        
        res = requests.get(url=url, headers=headers)
        data = res.json()
        ids.extend(data)
        
        # continue to next page?
        if(len(data) == per_page):
            page = page + 1
        else:
            page = 0
            
    # 2nd request: retrieve Activity details for each Activity ID
    # https://developers.strava.com/docs/reference/#api-Activities-getActivityById
    activities = []
    
    for Value in ids:
        url = "https://www.strava.com/api/v3/activities/" + str(Value['id']) + "?include_all_efforts=true"
        res = requests.get(url = url, headers = headers)
        data = res.json()
        activities.append(data)
        
    return activities


def upload_string_to_gcs(string, content_type, bucket_name, blob_name):
    
    # Saves string as blob object in Google Cloud Storage
    
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    bucket.blob(blob_name).upload_from_string(string, content_type)


def extract_strava_activities(message, context):

    # Use as Entry Point in Cloud Function
    # message & context params are redundant but required for compatibility with Pub/Sub trigger

    # Retrieve environment variables
    client_id = str(os.environ.get('api_client_id'))                                # API Cliend ID from Strava API App configuration
    client_secret_mount_ref = str(os.environ.get('api_client_secret_mount_ref'))    # Reference to Strava API Client Secret string, mounted locally
    refresh_token_mount_ref = str(os.environ.get('api_refresh_token_mount_ref'))    # Reference to Strava API Refresh Token string, mounted locally
    time_window_offset = int(os.environ.get('api_time_window_offset'))              # Number of seconds to subtract from current timestamp for created_before API parameter
    time_window = int(os.environ.get('api_time_window'))                            # Size of the lookback window in seconds, used to calculate created_after API parameter
    bucket_name = str(os.environ.get('gcs_bucket_name'))                            # Target GCS bucket for JSON output
    file_name = str(os.environ.get('gcs_file_name'))                                # Name of output JSON file

    # Fetch Strava API tokens
    api_tokens = refresh_strava_api_tokens(client_id, client_secret_mount_ref, refresh_token_mount_ref)
    access_token = api_tokens.json()['access_token']

    # Request activity details from Strava API
    created_before = int(time.time()) - time_window_offset
    created_after = created_before - time_window
    activities_list = get_recent_strava_activities(access_token, created_before, created_after)

    # Convert JSON output to EOL-delimited format (required by BigQuery load command)
    activities_string = '\n'.join(json.dumps(activity) for activity in activities_list)

    # Output JSON string to GCS
    upload_string_to_gcs(activities_string, 'application/json', bucket_name, file_name)
