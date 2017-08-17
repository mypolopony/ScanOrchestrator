import csv
import subprocess
import random
from datetime import datetime
import time
from dateutil import parser
from pymongo import MongoClient

dbname = 'status'
server = 'boringmachine'
port = 27017

c = MongoClient('mongodb://' + server + ':' + str(port) + '/' + dbname)
db = c[dbname]

# Pseudorandom Color
def get_pseudorandom_color(basis):
    # Initializing enforces all calls resolve to the same pseudorandom set
    random.seed('selwyn')

    # Now we reinitialize based on the basis string
    random.seed(basis)
    return '#%06x' % random.randint(0, 0xFFFFFF)


session_name = 'thpcssn3'
filename = '/tmp/plotly.csv'
results = list(db.orchestrator.find({'session_name': session_name}))

with open(filename, 'wb') as outfile:
    writer = csv.writer(outfile , quoting=csv.QUOTE_ALL)
    writer.writerow(['timestamp','hostname','ip','message','tasktype','color'])
    for item in results:
        item['timestamp'] = int(time.mktime(parser.parse(item['timestamp']).timetuple()))
        item['hostname'] = item['hostname'].replace('-matlab','')
        item['tasktype'] = item['message'].split(' ')[0]
        item['color'] = get_pseudorandom_color(item['tasktype'])
        writer.writerow([item['timestamp'],item['hostname'],item['ip'],item['message'],item['tasktype'],item['color']])