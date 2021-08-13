from pymongo import MongoClient
import pprint

client = MongoClient('mongodb://analytics-student:analytics-password@cluster0-shard-00-00-jxeqq.mongodb.net:27017,cluster0-shard-00-01-jxeqq.mongodb.net:27017,cluster0-shard-00-02-jxeqq.mongodb.net:27017/?ssl=true&replicaSet=Cluster0-shard-0&authSource=admin')
movies = client.mflix.movies
surveys = client.results.surveys

# pprint.pprint(movies.find_one())
movie_filter_doc = {'cast': 'Harrison Ford'}
pprint.pprint(movies.find(movie_filter_doc).count())
# 167

# pprint.pprint(surveys.find_one())
survey_filter_doc = {'results': {'$elemMatch': {'product': 'abc', 'score': {'$gt': 6}}}}
pprint.pprint(surveys.find(survey_filter_doc).count())
# 295