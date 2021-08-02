from pymongo import MongoClient
import pprint

PASSWORD = 'xxx'
DATABASE = 'mflix'
HOST = f'mongodb+srv://analytics:{PASSWORD}@mflix.yfk6m.mongodb.net/{DATABASE}?retryWrites=true&w=majority'

client = MongoClient(HOST)

# pipeline = [
#     {
#         '$group': {
#             '_id': {'language': '$language'},
#             'count': {'$sum': 1}
#         }
#     },
#     {
#         # -1 is descending
#         '$sort': {'count': -1}
#     }
# ]

pipeline = [
    {
        '$sortByCount': '$language'
    }
]

pprint.pprint(list(client.mflix.movies_initial.aggregate(pipeline)))

