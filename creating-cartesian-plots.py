import pymongo
import matplotlib.pyplot as plt


SAVEFIG_01 = 'creating-cartesian-plots.png'
course_cluster_uri = 'mongodb://analytics-student:analytics-password@cluster0-shard-00-00-jxeqq.mongodb.net:27017,cluster0-shard-00-01-jxeqq.mongodb.net:27017,cluster0-shard-00-02-jxeqq.mongodb.net:27017/?ssl=true&replicaSet=Cluster0-shard-0&authSource=admin'


course_client = pymongo.MongoClient(course_cluster_uri)
weather_data = course_client['100YWeatherSmall'].data
query = {'pressure.value': {'$lt': 9999}, 'airTemperature.value': {'$lt': 9999}}
data = list(weather_data.find(query).limit(1000))
pressures = [x['pressure']['value'] for x in data]
air_temps = [x['airTemperature']['value'] for x in data]

plt.scatter(pressures, air_temps)
plt.savefig(SAVEFIG_01)
plt.close()

