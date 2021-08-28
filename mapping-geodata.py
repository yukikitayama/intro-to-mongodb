import pymongo
import pprint
import matplotlib.pyplot as plt
from mpl_toolkits.basemap import Basemap


SAVEFIG_01 = 'mapping-geodata.png'
course_cluster_uri = 'mongodb://analytics-student:analytics-password@cluster0-shard-00-00-jxeqq.mongodb.net:27017,cluster0-shard-00-01-jxeqq.mongodb.net:27017,cluster0-shard-00-02-jxeqq.mongodb.net:27017/?ssl=true&replicaSet=Cluster0-shard-0&authSource=admin'


course_client = pymongo.MongoClient(course_cluster_uri)
shipwrecks = course_client.ships.shipwrecks
l = list(shipwrecks.find({}))
pprint.pprint(l[0])

lngs = [data['londec'] for data in l]
lats = [data['latdec'] for data in l]

plt.clf()
plt.figure(figsize=(14, 8))
m = Basemap(lat_0=lats[0], lon_0=lngs[0], projection='cyl')
m.drawcoastlines()
m.drawstates()
x, y = m(lngs, lats)
plt.scatter(x, y)
plt.savefig(SAVEFIG_01)
plt.close()
