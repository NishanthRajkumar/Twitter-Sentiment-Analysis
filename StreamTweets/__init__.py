import logging

import azure.functions as func
from azure.storage.blob import BlobClient

import tweepy
import os
import pandas as pd

def get_tweets(name: str):
    # Environment variables for Twitter API and Azure Storage
    api_key = os.environ['TWITTER_API_KEY']
    api_key_secret = os.environ['TWITTER_API_KEY_SECRET']
    access_token = os.environ['TWITTER_ACCESS_TOKEN']
    access_token_secret = os.environ['TWITTER_ACCESS_TOKEN_SECRET']
    azure_storage_conn = os.environ['AZURE_STORAGE_CONN']
    storage_container_name = os.environ['AZURE_CONTAINER_NAME']

    # Twitter API Authentication
    logging.info('Authenticating Twitter API')
    auth = tweepy.OAuthHandler(api_key, api_key_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = tweepy.API(auth)

    logging.info("Getting user's public tweets")
    public_tweets = api.user_timeline(screen_name = name)
    
    # Create dataframe
    columns = ['Time', 'User', 'Tweet']
    data = []
    for tweet in public_tweets:
        data.append([tweet.created_at, tweet.user.screen_name, tweet.text])
    df = pd.DataFrame(data, columns=columns)

    logging.info('Storing dataframe as csv format buffer data')
    output = df.to_csv(encoding = "utf-8", index=False)

    logging.info(f'Connecting to Azure storage account')
    blob = BlobClient.from_connection_string(
        conn_str=azure_storage_conn,
        container_name=storage_container_name,
        blob_name="tweets.csv"
    )

    logging.info('Attempting to store in blob storage')
    if blob.exists():
        logging.info('Blob already exist! Attempting to delete existing blob in storage')
        blob.delete_blob(delete_snapshots="include")

    logging.info('Uploading tweets.csv to blob storage')
    blob.upload_blob(output)


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Twitter-stream HTTP trigger function processed a request.')
    
    name = req.params.get('name')

    if not name:
        try:
            logging.info('No query. Attempting to read json body')
            req_body = req.get_json()
        except ValueError:
            logging.warn('No name Received!')
            pass
        else:
            name = req_body.get('name')

    if name:
        logging.info('Name received!')
        get_tweets(name)
        return func.HttpResponse("Twitter data of user was streamed and executed successfully.")
    else:
        return func.HttpResponse(
             "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body to get tweets of user with that name",
             status_code=200
        )
