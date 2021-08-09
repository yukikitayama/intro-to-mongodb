from pymongo import MongoClient
import pprint

PASSWORD = 'xxx'
DATABASE = 'mflix'
HOST = f'mongodb+srv://analytics:{PASSWORD}@mflix.yfk6m.mongodb.net/{DATABASE}?retryWrites=true&w=majority'

client = MongoClient(HOST)

pipeline = [
    {
        '$limit': 100
    },
    {
        '$project': {
            'title': 1,
            'year': 1,
            'directors': {'$split': ['$director', ', ']},
            'actors': {'$split': ['$cast', ', ']},
            'writers': {'$split': ['$writer', ', ']},
            'genres': {'$split': ['$genre', ', ']},
            'languages': {'$split': ['$language', ', ']},
            'countries': {'$split': ['$country', ', ']},
            'plot': 1,
            'fullPlot': '$fullplot',
            'rated': '$rating',
            'released': {
                '$cond': {
                    'if': {'$ne': ['$released', '']},
                    'then': {
                        '$dateFromString': {
                            'dateString': '$released'
                        }
                    },
                    'else': ''}},
            'runtime': 1,
            'poster': 1,
            'imdb': {
                'id': '$imdbID',
                'rating': '$imdbRating',
                'votes': '$imdbVotes'
                },
            'metaccritic': 1,
            'awards': 1,
            'type': 1,
            'lastUpdated': '$lastupdated'
        }
    },
    {
        '$out': 'movies_scratch'
    }
]

pprint.pprint(list(client.mflix.movies_initial.aggregate(pipeline)))
