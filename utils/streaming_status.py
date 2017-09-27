from __init__ import RedisManager
from datetime import datetime
from pprint import pprint
import boto3
import ConfigParser
import copy
import random
import os
import pickle
import plotly.graph_objs as go
import plotly.plotly as py
import plotly.tools as tls
import re
import time

# Load config file
config = ConfigParser.ConfigParser()
config.read('poller.conf')

# Plotly
tokens = tls.get_credentials_file()['stream_ids']

# Redis connection
redisman = RedisManager(host=config.get('redis','host'), db=config.get('redis', 'db'), port=config.get('redis','port'))

# Delay and Hours
DELAY = 22
SHOW_HOURS = 6

# Set figure layout
layout = go.Layout(
    title='Active Queues',
    yaxis=dict(
        title='Pending items'
    ),
    xaxis=dict(
        title='Time'
    ),
    showlegend=True
)

# All plots and traces
plots = dict()
traces = list()

# Initialize
status = redisman.status()
for idx, queue in enumerate(status.keys()):
    plots[queue] = dict()
    plots[queue]['meta'] = dict()
    plots[queue]['stream'] = dict()

    plots[queue]['meta']['color'] = 'rgb({}, {}, {})'.format(random.randint(0,255), random.randint(0,255), random.randint(0,255))
    plots[queue]['meta']['token'] = tokens[idx]
    plots[queue]['stream'] = dict(
        token=tokens[idx], maxpoints=SHOW_HOURS * 60 * 60 / DELAY)

    # Create stream links
    plots[queue]['meta']['link'] = py.Stream(stream_id=plots[queue]['meta']['token'])
    plots[queue]['meta']['link'].open()

    # Generate trace
    traces.append(go.Scatter(x=[], y=[], stream=plots[queue]['stream'],
                             name=queue, marker=dict(color=plots[queue]['meta']['color'])))


# Initialize plot
fig = go.Figure(data=traces, layout=layout)
plot_url = py.plot(fig, filename='queue-status')


while True:
    '''
    Main loop
    '''
    status = redisman.status()

    # Plot
    for queue in status.keys():
        if queue in plots.keys():       # Until we figure out how to add new traces
            x = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
            print(queue)
            print(dict(x=x, y=status[queue]))
            plots[queue]['meta']['link'].write(dict(x=x, y=status[queue]))


    # Rest
    time.sleep(DELAY)