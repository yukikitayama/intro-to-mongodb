# Before this
# $ mongoimport --drop --type csv --headerline --db mflix --collection movies --host "mflix-shard-00-02.yfk6m.mongodb.net:27017" --authenticationDatabase admin --ssl --username analytics --password xxx --file movies_initial.csv


from pymongo import MongoClient, UpdateOne
import pprint
import re
from datetime import datetime


PASSWORD = 'xxx'
DATABASE = 'mflix'
HOST = f'mongodb+srv://analytics:{PASSWORD}@mflix.yfk6m.mongodb.net/{DATABASE}?retryWrites=true&w=majority'
BATCH_SIZE = 1000


client = MongoClient(HOST)

runtime_pat = re.compile(r'([0-9]+) min')
updates = []
count = 0
iteration = 0
for movie in client.mflix.movies.find({}):

    fields_to_set = {}
    fields_to_unset = {}

    for k, v in movie.copy().items():
        if v == "" or v == [""]:
            del movie[k]
            fields_to_unset[k] = ""

    if 'director' in movie:
        fields_to_unset['director'] = ""
        fields_to_set['directors'] = movie['director'].split(", ")
    if 'cast' in movie:
        fields_to_set['cast'] = movie['cast'].split(", ")
    if 'writer' in movie:
        fields_to_unset['writer'] = ""
        fields_to_set['writers'] = movie['writer'].split(", ")
    if 'genre' in movie:
        fields_to_unset['genre'] = ""
        fields_to_set['genres'] = movie['genre'].split(", ")
    if 'language' in movie:
        fields_to_unset['language'] = ""
        fields_to_set['languages'] = movie['language'].split(", ")
    if 'country' in movie:
        fields_to_unset['country'] = ""
        fields_to_set['countries'] = movie['country'].split(", ")

    if 'fullplot' in movie:
        fields_to_unset['fullplot'] = ""
        fields_to_set['fullPlot'] = movie['fullplot']
    if 'rating' in movie:
        fields_to_unset['rating'] = ""
        fields_to_set['rated'] = movie['rating']

    imdb = {}
    if 'imdbID' in movie:
        fields_to_unset['imdbID'] = ""
        imdb['id'] = movie['imdbID']
    if 'imdbRating' in movie:
        fields_to_unset['imdbRating'] = ""
        imdb['rating'] = movie['imdbRating']
    if 'imdbVotes' in movie:
        fields_to_unset['imdbVotes'] = ""
        imdb['votes'] = movie['imdbVotes']
    if imdb:
        fields_to_set['imdb'] = imdb

    if 'released' in movie:
        fields_to_set['released'] = datetime.strptime(movie['released'],
                                                      "%Y-%m-%d")
    if 'lastUpdated' in movie:
        fields_to_set['lastUpdated'] = datetime.strptime(movie['lastUpdated'][0:19],
                                                         "%Y-%m-%d %H:%M:%S")

    if 'runtime' in movie:
        m = runtime_pat.match(movie['runtime'])
        if m:
            fields_to_set['runtime'] = int(m.group(1))

    update_doc = {}
    if fields_to_set:
        update_doc['$set'] = fields_to_set
    if fields_to_unset:
        update_doc['$unset'] = fields_to_unset

    updates.append(UpdateOne({'_id': movie['_id']}, update_doc))

    count += 1
    if count == BATCH_SIZE:
        client.mflix.movies.bulk_write(updates)
        updates = []
        count = 0
        iteration += 1
        print(f'Iteration: {iteration} updated MongoDB with batch')

# The final chunk of data which may not be up to batch size
if updates:
    client.mflix.movies.bulk_write(updates)
