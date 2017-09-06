from mongoengine import connect
from pymongo import MongoClient
import os
import boto3
import ConfigParser

# Load config file
config = ConfigParser.ConfigParser()
config_path = os.path.join('utils/poller.conf')
config.read(config_path)

# S3
S3Key = config.get('s3', 'aws_access_key_id')
S3Secret = config.get('s3', 'aws_secret_access_key')
s3 = boto3.client('s3', aws_access_key_id=S3Key, aws_secret_access_key=S3Secret)
s3r = boto3.resource('s3', aws_access_key_id=S3Key, aws_secret_access_key=S3Secret)

# Kombu
rabbit_url = '{}:{}@{}'.format(config.get('rmq', 'username'), config.get('rmq', 'password'), config.get('rmq', 'hostname'))
conn = None

# MongoDB
dbname = 'dev-agdb'
username = 'staging-agridata'
password = 'M5m7xMe2cUNcNn6y$Rz'
server = 'ds036947-a0.mlab.com'
port = 36947
connect(dbname, host=server, port=port, username=username, password=password)
c = MongoClient('mongodb://' + username + ':' + password + '@' + server + ':' + str(port) + '/' + dbname)
db = c[dbname]