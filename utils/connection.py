from models_min import *
from mongoengine import connect
from pymongo import MongoClient

dbname = 'dev-agdb'

server = 'ds161551.mlab.com'
port = 61551
username = 'staging-agridata'
password = 'M5m7xMe2cUNcNn6y$Rz'

connect(dbname, host=server, port=port, username=username, password=password)
c = MongoClient('mongodb://' + username + ':' + password + '@' + server + ':' + str(port) + '/' + dbname)

db = c[dbname]