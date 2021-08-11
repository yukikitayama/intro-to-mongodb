from pymongo import MongoClient
import pprint
import re
from datetime import datetime

PASSWORD = 'xxx'
DATABASE = 'mflix'
HOST = f'mongodb+srv://analytics:{PASSWORD}@mflix.yfk6m.mongodb.net/{DATABASE}?retryWrites=true&w=majority'

client = MongoClient(HOST)

# Compile regular expression pattern into regular expression object.
# This us used for matching to return whether match object of True or None.
runtime_pattern = re.compile(r'([0-9]+) min')

for movie in client.mflix.movies_initial.find({}).limit(2):

    fields_to_set = {}
    fields_to_unset = {}

    # Fields to delete from database
    for k, v in movie.copy().items():
        if v == '' or v == ['']:
            del movie[k]
            fields_to_unset[k] = ''

    # Fields to update in database
    if 'director' in movie:
        # Delete director because we want to make a new directors column
        fields_to_unset['director'] = ''
        fields_to_set['directors'] = movie['director'].split(', ')
    if 'cast' in movie:
        fields_to_set['cast'] = movie['cast'].split(', ')
    if 'writer' in movie:
        fields_to_unset['writer'] = ''
        fields_to_set['writers'] = movie['writer'].split(', ')
    if 'genre' in movie:
        fields_to_unset['genre'] = ''
        fields_to_set['genres'] = movie['genre'].split(', ')
    if 'language' in movie:
        fields_to_unset['language'] = ''
        fields_to_set['languages'] = movie['language'].split(', ')
    if 'country' in movie:
        fields_to_unset['country'] = ''
        fields_to_set['countries'] = movie['country'].split(', ')
    if 'fullplot' in movie:
        fields_to_unset['fullplot'] = ''
        fields_to_set['fullPlot'] = movie['fullplot']
    if 'rating' in movie:
        fields_to_unset['rating'] = ''
        fields_to_set['rated'] = movie['rating']

    imdb = {}
    if 'imdbID' in movie:
        fields_to_unset['imdbID'] = ''
        imdb['id'] = movie['imdbID']
    if 'imdbRating' in movie:
        fields_to_unset['imdbRating'] = ''
        imdb['rating'] = movie['imdbRating']
    if 'imdbVotes' in movie:
        fields_to_unset['imdbVotes'] = ''
        imdb['votes'] = movie['imdbVotes']
    if imdb:
        fields_to_set['imdb'] = imdb

    if 'released' in movie:
        fields_to_set['released'] = datetime.strptime(movie['released'],
                                                      '%Y-%m-%d')
    if 'lastUpdated' in movie:
        fields_to_set['lastUpdated'] = datetime.strptime(movie['lastUpdated'][0:19],
                                                         '%Y-%m-%d %H:%M:%S')
    if 'runtime' in movie:
        matchObject = runtime_pattern.match(movie['runtime'])
        if matchObject:
            # use m.group(1) because we need only the first parenthesized subgroup, and group(0) is the entire thing
            fields_to_set['runtime'] = int(matchObject.group(1))

    update_doc = {}
    if fields_to_set:
        update_doc['$set'] = fields_to_set
    if fields_to_unset:
        update_doc['$unset'] = fields_to_unset

    pprint.pprint(movie['_id'])
    print(type(movie['_id']))
    print(str(movie['_id']))
    pprint.pprint(update_doc)

    # collection.update_one({'_id': movie['_id']}, update_doc)
