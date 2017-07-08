from models_min import *
from mongoengine import connect
from pymongo import MongoClient

dbname = 'dev-agdb'

server = 'ds161551.mlab.com'
port = 61551
username = 'agridata'
password = 'agridata'

connect(dbname, host=server, port=port, username=username, password=password)
c = MongoClient('mongodb://' + username + ':' + password + '@' + server + ':' + str(port) + '/' + dbname)

db = c[dbname]