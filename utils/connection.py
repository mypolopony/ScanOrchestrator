from mongoengine import connect
from pymongo import MongoClient
from bson.objectid import ObjectId
import os
import boto3

# MongoDB
print('Connecting to Development Databae')
dbname = 'dev-agdb'
username = 'staging-agridata'
password = 'M5m7xMe2cUNcNn6y$Rz'
server = 'ds036947-a0.mlab.com'
port = 36947
connect(dbname, host=server, port=port, username=username, password=password)
c = MongoClient('mongodb://' + username + ':' + password + '@' + server + ':' + str(port) + '/' + dbname)
db = c[dbname]