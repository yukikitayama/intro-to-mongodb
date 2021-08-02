from pymongo import MongoClient
import pprint

PASSWORD = 'xxx'
DATABASE = 'mflix'
HOST = f'mongodb+srv://analytics:{PASSWORD}@mflix.yfk6m.mongodb.net/{DATABASE}?retryWrites=true&w=majority'

client = MongoClient(HOST)

pipeline = [
    {
        '$sortByCount': '$language'
    },
    {
        '$facet': {
            'top language combinations': [{'$limit': 100}],
            'unusual combinations shared by': [{
                '$skip': 100
            },
            {
                '$bucketAuto': {
                    'groupBy': '$count',
                    'buckets': 5,
                    'output': {
                        'language combinations': {'$sum': 1}
                    }
                }
            }]
        }
    }
]

pprint.pprint(list(client.mflix.movies_initial.aggregate(pipeline)))
