import re
import pandas as pd
from dateutil import parser
import plotly.plotly as py
import plotly.graph_objs as go

# Larkmead blocks
# acres) = 44.66

# TODO: Task a parameter ('detection')
# TODO: Reinstate daytotal

# Input log file
logpath = 'logs/larkmead_detection.log'

# Plotly credentials
py.sign_in('agriselwyn', '0nM9F4CYVWKsI8lIwk9X')

with open(logpath,'r') as logfile:
    data_in = logfile.read()

# Create a dictionary of data frames, along with a total cumulative data frame
sessions = set([col.replace('detection:','').strip() for col in re.findall('detection:[a-z0-9\-]+',data_in)])

# Initialization
frames = dict.fromkeys(sessions)
for c in frames.keys():
    frames[c] = pd.DataFrame(columns=['Date', 'Remaining'])

date = None
#! daytotal = 0

# Read each line and add it to the data frame for that session
with open(logpath,'r') as logfile:
    for line in logfile:
        line = line.replace('\t','')
        if '2017' in line:
            # Save the day total and reset the date
            #! frames['total'] = frames['total'].append({'Date':date, 'Remaining':daytotal}, ignore_index=True)
            #! daytotal = 0
            date = parser.parse(line)
        elif 'detection' in line:
            # Parse
            (sess, num) = re.search(':([a-z0-9\-]+) (.+)', line).groups()

            # Convert to integer
            num = int(num.strip())

            # Add to cumulative total
            #! daytotal += num

            # Add to appropriate data frame
            frames[sess] = frames[sess].append({'Date':date, 'Remaining':num }, ignore_index=True)

# Entirely emove sessions that cause bad scaling
frames.pop('c7')

# Generate the series (based on session name)
series = [go.Scatter(x=data['Date'], y=data['Remaining'], mode='lines', name=sess) for (sess, data) in frames.iteritems()]

# General layout
layout = go.Layout(
    title='Larkmead Detection Tasks (21 Sep 2017)',
    xaxis=dict(
        title='Time',
        titlefont=dict(
            family='Courier New, monospace',
            size=14,
            color='#7f7f7f'
        )
    ),
    yaxis=dict(
        title='Tasks Remaining',
        titlefont=dict(
            family='Courier New, monospace',
            size=14,
            color='#7f7f7f'
        )
    )
)

fig = go.Figure(data=series, layout=layout)
py.image.save_as(fig, filename=logpath.replace('.log','.png'))