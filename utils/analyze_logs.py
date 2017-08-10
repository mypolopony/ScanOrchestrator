import csv
import subprocess
import seaborn as sns
from datetime import datetime
import time
from dateutil import parser
from pymongo import MongoClient

dbname = 'status'
server = 'boringmachine'
port = 27017

c = MongoClient('mongodb://' + server + ':' + str(port) + '/' + dbname)
db = c[dbname]

session_name = '08.10.13.52'
filename = '/tmp/rhetoric.csv'
results = list(db.orchestrator.find({'session_name': session_name}))

tasktypes = set([msg.split(' ')[0] for msg in [r['message'] for r in results]])
colors = dict(zip(tasktypes, sns.color_palette('deep', len(tasktypes))))

with open(filename, 'wb') as outfile:
    writer = csv.writer(outfile , quoting=csv.QUOTE_ALL)
    writer.writerow(['timestamp','hostname','ip','message','tasktype','color','message'])
    for item in results:
        item['timestamp'] = int(time.mktime(parser.parse(item['timestamp']).timetuple()))
        item['hostname'] = item['hostname'].replace('-matlab','')
        item['tasktype'] = item['message'].split(' ')[0]
        item['color'] = colors[item['tasktype']]
        writer.writerow([item['timestamp'],item['hostname'],item['ip'],item['message'],item['tasktype'],item['color'],item['message']])