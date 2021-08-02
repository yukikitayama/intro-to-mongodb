import pymongo
from pymongo import MongoClient
import pprint

PASSWORD = 'xxx'
DATABASE = 'mflix'
HOST = f'mongodb+srv://analytics:{PASSWORD}@mflix.yfk6m.mongodb.net/{DATABASE}?retryWrites=true&w=majority'

client = MongoClient(HOST)

filter = {'language': 'Korean, English'}

pprint.pprint(list(client.mflix.movies_initial.find(filter)))
