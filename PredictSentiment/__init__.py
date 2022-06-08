import logging

import azure.functions as func
from azure.storage.blob import BlobClient

import os
import re
import nltk
import pickle
import pandas as pd

path = 'model/logistic_model.pkl'
nltk.download('stopwords')
nltk.download('wordnet')
nltk.download('omw-1.4')
with open(path, 'rb') as file:
    bow_obj = pickle.load(file)
    model = pickle.load(file)
logging.info("Model Imported")

def tokenization(data):
    """
    :param data: It will receive the tweet and perform tokenization and remove the stopwords
    :return: It will return the cleaned data
    """
    stop_words = set(nltk.corpus.stopwords.words('english'))
    stop_words.remove('no')
    stop_words.remove('not')

    tokenizer = nltk.tokenize.TweetTokenizer()

    document = []
    for text in data:
        collection = []
        tokens = tokenizer.tokenize(text)
        for token in tokens:
            if token not in stop_words:
                if '#' in token:
                    collection.append(token)
                else:
                    collection.append(re.sub("@\S+|https?:\S+|http?:\S|[^A-Za-z0-9]+", " ", token))
        document.append(" ".join(collection))
    return document

def lemmatization(data):
    """
    :param data: Receive the tokenized data
    :return: Return the cleaned data
    """
    lemma_function = nltk.stem.wordnet.WordNetLemmatizer()
    sentence = []
    for text in data:
        document = []
        words = text.split(' ')
        for word in words:
            document.append(lemma_function.lemmatize(word))
        sentence.append(" ".join(document))
    return sentence

def get_tweet_sentiment(my_tweet) -> str:
    """
        Here we'll perform predictions on the data given by the tweeter.
    """
    tokenized_data = tokenization([my_tweet])
    lemmatized_data = lemmatization(tokenized_data)
    temp = bow_obj.transform(lemmatized_data)
    pred = model.predict(temp)
    if pred == 0:
        return 'Positive'
    else:
        return 'Negative'

def sentiment_prediction_main():
    azure_storage_conn = os.environ['AZURE_STORAGE_CONN']
    storage_container_name = os.environ['AZURE_CONTAINER_NAME']
    azure_blob_url = os.environ['AZURE_TWEETS_BLOB_URL']

    logging.info(f'Reading tweets csv file blob')

    df = pd.read_csv(azure_blob_url)

    # Predict sentiments of tweets
    pred_sentiments = []
    for tweet in df['Tweet']:
        pred_sentiments.append(get_tweet_sentiment(tweet))
    df['Sentiment'] = pred_sentiments
    
    output = df.to_csv(encoding = "utf-8", index=False)

    logging.info(f'Connecting to Azure storage account, to store tweet sentiment prediction')
    sentiment_blob = BlobClient.from_connection_string(
        conn_str=azure_storage_conn,
        container_name=storage_container_name,
        blob_name="tweet_sentiment.csv"
    )

    logging.info('Uploading tweet_sentiment.csv to blob storage')
    sentiment_blob.upload_blob(output, overwrite=True)
    

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('PredictSentiment function processed a request.')
    
    sentiment_prediction_main()
    return func.HttpResponse("Twitter Sentiment analysis successfull")