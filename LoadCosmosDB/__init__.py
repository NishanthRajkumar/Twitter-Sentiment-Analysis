import logging

import azure.functions as func

import os
import pymongo
import pandas as pd

cosmos_uri = os.environ['CFP_COSMOS_URI']
azure_blob_url = os.environ['AZURE_SENTIMENTS_BLOB_URL']

def load_cosmos():
    client = pymongo.MongoClient(cosmos_uri)
    db = client['twitter_data']
    tweet_coll = db['tweet_sentiments']

    logging.info(f'Reading sentiments csv file blob')
    df = pd.read_csv(azure_blob_url)
    df_dict = df.to_dict('records')

    logging.info(f'Inserting into CosmosDB')
    tweet_coll.insert_many(df_dict)


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('LoadCosmosDB function processed a request.')
    
    load_cosmos()
    return func.HttpResponse("Loading to CosmosDB... successfull")