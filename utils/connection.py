from models_min import *
from mongoengine import connect
from pymongo import MongoClient

db = 'staging'

if db == 'local':
    
    print('Connecting to Local DB')
    dbname = 'dev-agdb'
    connect(dbname)
    c = MongoClient('mongodb://localhost:27017/' + dbname)
    db=c[dbname]

else:

    if db == 'staging':
        print('Connecting to Development DB')
        dbname = 'dev-agdb'
        username = 'staging-agridata'
        password = 'M5m7xMe2cUNcNn6y$Rz'
        server = ['ds036947-a0.mlab.com:36947', 'ds036947-a1.mlab.com:36942']
        port = 36947

    if db == 'production':
        print('Connecting to Production DB')
        dbname = 'agdb'
        username = 'production-agridata'
        password = '8svcjPZYtx$7F+%p?sBv'
        server = ['ds036937-a0.mlab.com:36937','ds036937-a1.mlab.com:36932']
        port = 36937

    connect(dbname, host=server, port=port, username=username, password=password)
    c = MongoClient('mongodb://' + username + ':' + password + '@' + ','.join(server) + '/' + dbname)
    db = c[dbname]
