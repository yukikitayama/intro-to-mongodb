import pymongo
import pprint

PASSWORD = 'xxx'
DATABASE = 'mflix'
HOST = f'mongodb+srv://analytics:{PASSWORD}@mflix.yfk6m.mongodb.net/{DATABASE}?retryWrites=true&w=majority'

free_tier_client = pymongo.MongoClient(HOST)
people = free_tier_client.cleansing['people-raw']

pprint.pprint(people.index_information())


def distilled_explain(explain_output):
    return {
        'executionTimeMillis': explain_output['executionStats']['executionTimeMillis'],
        'totalDocsExamined': explain_output['executionStats']['totalDocsExamined'],
        'nReturned': explain_output['executionStats']['nReturned']
    }


query_1_stats = people.find({
    'address.state': 'Nebraska',
    'last_time': 'Miller'
}).explain()

query_2_stats = people.find({
    'first_name': 'Harry',
    'last_name': 'Reed'
}).explain()

print(distilled_explain(query_1_stats))
print(distilled_explain(query_2_stats))

# people.create_index([
#     ('last_name', pymongo.ASCENDING),
#     ('first_name', pymongo.ASCENDING),
#     ('address.state', pymongo.ASCENDING)
# ])

query_1_stats = people.find({
    'address.state': 'Nebraska',
    'last_name': 'Miller'
}).explain()

query_2_stats = people.find({
    'first_name': 'Harry',
    'last_name': 'Reed'
}).explain()

print(distilled_explain(query_1_stats))
print(distilled_explain(query_2_stats))

pprint.pprint(people.index_information())
