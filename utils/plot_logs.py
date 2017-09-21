import re
import pandas as pd
from dateutil import parser
import plotly.plotly as py
import plotly.graph_objs as go

# Larkmead blocks
# acres) = 44.66

logpath = 'logs/larkmead_detection.log'

py.sign_in('agriselwyn', '0nM9F4CYVWKsI8lIwk9X')

with open(logpath,'r') as logfile:
    data_in = logfile.read()
sessions = set([col.replace('detection:','').strip() for col in re.findall('detection:[a-z0-9\-]+',data_in)])

print(sessions)
frames = dict.fromkeys(sessions)
for c in frames.keys():
    frames[c] = pd.DataFrame(columns=['Date', 'Remaining'])

date = None
with open(logpath,'r') as logfile:
    for line in logfile:
        line = line.replace('\t','')
        if '2017' in line:
            date = parser.parse(line)
        elif 'detection' in line:
            (sess, num) = re.search(':([a-z0-9\-]+) (.+)', line).groups()
            frames[sess] = frames[sess].append({'Date':date, 'Remaining':int(num.strip())}, ignore_index=True)

# Remove sessions that cause bad scaling
frames.pop('c7')

series = [go.Scatter(x=data['Date'], y=data['Remaining'], mode='lines', name=sess) for (sess, data) in frames.iteritems()]
fig = go.Figure(data=series)

py.image.save_as(fig, filename=logpath.replace('.log','.png'))