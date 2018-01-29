from pymongo import MongoClient, errors

client = MongoClient('localhost')
db = client['big_data']