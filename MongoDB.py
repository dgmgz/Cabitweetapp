import pymongo
from kafka import KafkaConsumer
import json
import feeling_analysis as fa
import os

cwd = os.getcwd()


class MongoDB:

    def DatabaseCreation(DDBBName):

        """
        MongoDB connection and DataBase creation
        :param DDBBName
        """

        MongoDBConnection = pymongo.MongoClient('mongodb://localhost:27017/')
        MongoDBConnection[DDBBName]


    def CollectionCreation(DDBBName, CollectionName):

        """
        MongoDB collection creation
        :param DDBBName
        :param CollectionName
        """

        MongoDBConnection = pymongo.MongoClient('mongodb://localhost:27017/')
        MongoDBConnection[str(DDBBName)][str(CollectionName)]


    def InsertJSON(DDBBName, CollectionName, JSON):

        """
        MongoDB JSON insertion
        :param DDBBName
        :param CollectionName
        :param JSON
        """

        MongoDBConnection = pymongo.MongoClient('mongodb://localhost:27017/')
        MongoDBConnection[str(DDBBName)][str(CollectionName)].insert_many(JSON)


# Database and Collection Creation
DDBB = MongoDB.DatabaseCreation("twitter_test_parsed")
COL = MongoDB.CollectionCreation(DDBB,"fact_tweets")


# Setup MongoDB connection

MongoDBConnection = pymongo.MongoClient('mongodb://localhost:27017/')
TwitterMongo = MongoDBConnection[str('twitter_test_parsed')][str('fact_tweets')]


# MongoDB Tweets Consumer
consumer = KafkaConsumer(bootstrap_servers=['localhost:9092'],
                         group_id='my-group',
                         enable_auto_commit=True,
                         auto_commit_interval_ms=1000,
                         auto_offset_reset='latest')
consumer.subscribe(['twittertest'])


for tweet in consumer:
    try:
        raw_tweet = fa.tweet_parser(json.loads(tweet.value))

        print(TwitterMongo.insert_one(raw_tweet).inserted_id)

    except Exception:
        continue
    except KeyError:
        continue


