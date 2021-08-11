import pymongo
import pprint
from datetime import datetime
from bson.decimal128 import Decimal128

PASSWORD = 'xxx'
DATABASE = 'mflix'
HOST = f'mongodb+srv://analytics:{PASSWORD}@mflix.yfk6m.mongodb.net/{DATABASE}?retryWrites=true&w=majority'

course_client = pymongo.MongoClient(HOST)
movies = course_client['mflix']['movies']
print(movies)
movie = movies.find_one()
pprint.pprint(movie)
print(movie['_id'])

# Before running the below 1 line, there was no database called test,
# but the below automatically creates a database test and a collection dates.
# dates = course_client['test']['dates']
# dates.insert_one({'dt': datetime.utcnow()})
# pprint.pprint(dates.find_one())

decimals = course_client['test']['decimals']
# decimals.insert_one({'money': Decimal128('99.99')})
pprint.pprint(decimals.find_one())
pprint.pprint(movies.find_one({'year': {'$type': 'int'}}))
