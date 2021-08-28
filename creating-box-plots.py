import pymongo
import matplotlib.pyplot as plt
import dateparser


SAVEFIG_01 = 'creating-box-plots.png'
course_cluster_uri = 'mongodb://analytics-student:analytics-password@cluster0-shard-00-00-jxeqq.mongodb.net:27017,cluster0-shard-00-01-jxeqq.mongodb.net:27017,cluster0-shard-00-02-jxeqq.mongodb.net:27017/?ssl=true&replicaSet=Cluster0-shard-0&authSource=admin'


course_client = pymongo.MongoClient(course_cluster_uri)
trips = course_client.citibike.trips
cursor = trips.aggregate([
    {
        '$match': {
            'start time': {
                '$gte': dateparser.parse('1/1/2016'),
                '$lt': dateparser.parse('2/1/2016')
            },
            'tripduration': {
                '$lt': 3600
            }
        }
    },
    {
        '$sort': {
            'bikeid': 1
        }
    },
    {
        '$limit': 2500
    },
    {
        '$addFields': {
            'dayOfWeek': {
                '$dayOfWeek': '$start time'
            }
        }
    },
    {
        '$group': {
            '_id': '$dayOfWeek',
            'trips': {
                '$push': '$$ROOT'
            }
        }
    },
    {
        '$sort': {
            '_id': 1
        }
    }
])
trips_by_day = [doc['trips'] for doc in cursor]
trip_durations_by_day = [[trip['tripduration'] / 60 for trip in trips] for trips in trips_by_day]

print(len(trip_durations_by_day))
print(len(trip_durations_by_day[0]))
print(len(trip_durations_by_day[1]))

plt.boxplot(trip_durations_by_day)
plt.savefig(SAVEFIG_01)
plt.close()

