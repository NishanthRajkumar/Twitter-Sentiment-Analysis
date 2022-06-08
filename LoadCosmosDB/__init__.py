import logging

import azure.functions as func

import os
import pymongo
import pandas as pd

cosmos_uri = os.environ['CFP_COSMOS_URI']
azure_blob_url = os.environ['AZURE_SENTIMENTS_BLOB_URL']

def load_cosmos():
    client = pymongo.MongoClient(cosmos_uri)
    list_of_db = client.list_database_names()
    if 'twitter_data' not in list_of_db:
        return "unable to locate 'twitter_data' DB"
    db = client['twitter_data']
    list_of_collections = db.list_collection_names()
    if 'tweet_sentiments' not in list_of_collections:
        db.create_collection('tweet_sentiments')
    tweet_coll = db['tweet_sentiments']

    logging.info(f'Reading sentiments csv file blob')
    df = pd.read_csv(azure_blob_url)
    df_dict = df.to_dict('records')

    logging.info(f'Inserting into CosmosDB')
    tweet_coll.insert_many(df_dict)
    return "Loading to CosmosDB... successfull"

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('LoadCosmosDB function processed a request.')
    result = load_cosmos()
    return func.HttpResponse(result)