import csv
import datetime
import time

# File definition
fin = open('logs/runlog.log','r')
data = fin.readlines()
fin.close()

# To ne saved
rows = list()
t = None

# Ingest a (hopefully well formatted) log file
for line in data:
    if '2017' in line:
        t = line.replace('\n','')
        t = time.mktime(time.strptime(t,'%c'))
    elif line == '\n' or line == '--------\n':
        pass
    elif ':' in line:
        row = dict()
        (row['queue'], row['num']) = [s.replace('\n','') for s in line.split(' ') if s]
        row['timestamp'] = t
        rows.append(row)
    else:
        pass

# Get fieldnames
fields = set()
for row in rows:
    for key in row.keys():
        fields.add(key)

# Add num column
fields.add('num')

# Time to be used as output filename
t = datetime.datetime.strftime(datetime.datetime.now(),'%m%d%H%M')

# Write to file
with open('logs/{}.csv'.format(t),'w') as csvout:
    writer = csv.DictWriter(csvout,fieldnames=list(fields))
    writer.writeheader()

    for row in rows:
        writer.writerow(row)
