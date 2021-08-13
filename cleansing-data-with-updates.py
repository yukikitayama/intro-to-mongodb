from pymongo import MongoClient, InsertOne, UpdateOne
import dateparser
from bson.json_util import loads
import pprint

PASSWORD = 'xxx'
DATABASE = 'mflix'
HOST = f'mongodb+srv://analytics:{PASSWORD}@mflix.yfk6m.mongodb.net/{DATABASE}?retryWrites=true&w=majority'


def update_collection(people_raw):
    batch_size = 1000
    inserts = []
    count = 0

    with open('./people-raw.json') as dataset:
        for line in dataset:
            inserts.append(InsertOne(loads(line)))
            count += 1
            if count == batch_size:
                people_raw.bulk_write(inserts)
                inserts = []
                count = 0

    if inserts:
        people_raw.bulk_write(inserts)
        count = 0


def update_birthday(people, people_raw):
    batch_size = 1000
    count = 0
    updates = []
    for person in people:
        updates.append(UpdateOne({'_id': person['_id']}, {'$set': {'birthday': dateparser.parse(person['birthday'])}}))
        count += 1

        if count == batch_size:
            people_raw.bulk_write(updates)
            updates = []
            count = 0

    if updates:
        people_raw.bulk_write(updates)
        count = 0


def main() -> None:

    client = MongoClient(HOST)
    people_raw = client.cleansing['people-raw']

    # Upload data to MongoBD
    # update_collection(people_raw)

    # pprint.pprint(people_raw.count())
    query = {'birthday': {'$type': 'string'}}
    print('Number of all the documents')
    pprint.pprint(people_raw.count_documents({}))
    print('Number of the documents where birthday is string')
    pprint.pprint(people_raw.count_documents(query))

    # Update birthday
    people_with_string_birthdays = people_raw.find(query)
    update_birthday(people_with_string_birthdays, people_raw)

    pprint.pprint(people_raw.count_documents(query))


if __name__ == '__main__':
    main()
