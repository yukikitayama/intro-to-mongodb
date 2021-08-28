import pymongo
from mpl_toolkits.mplot3d import Axes3D
import matplotlib.pyplot as plt


SAVEFIG_01 = 'creating-3d-plots.png'
course_cluster_uri = 'mongodb://analytics-student:analytics-password@cluster0-shard-00-00-jxeqq.mongodb.net:27017,cluster0-shard-00-01-jxeqq.mongodb.net:27017,cluster0-shard-00-02-jxeqq.mongodb.net:27017/?ssl=true&replicaSet=Cluster0-shard-0&authSource=admin'


course_client = pymongo.MongoClient(course_cluster_uri)
weather_data = course_client['100YWeatherSmall'].data
query = {
    'pressure.value': {'$lt': 9999},
    'airTemperature.value': {'$lt': 9999},
    'wind.speed.rate': {'$lt': 500}
}
data = list(weather_data.find(query).limit(1000))
pressures = [x['pressure']['value'] for x in data]
air_temps = [x['airTemperature']['value'] for x in data]
wind_speeds = [x['wind']['speed']['rate'] for x in data]

fig = plt.figure()
ax = fig.add_subplot(projection='3d')
ax.scatter(pressures, air_temps, wind_speeds)
plt.savefig(SAVEFIG_01)
plt.close()