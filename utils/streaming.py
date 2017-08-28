import plotly.plotly as py  
import plotly.tools as tls   
import plotly.graph_objs as go
import time
import pickle
import os
from kombu import Connection
from datetime import datetime
import re
import copy
import boto3
import ConfigParser
from pprint import pprint

# Global config
config = ConfigParser.ConfigParser()
config.read('poller.conf')

# Delay and Hours
DELAY = 30
SHOW_HOURS = 6

# AWS Resources: S3
S3Key = config.get('s3', 'aws_access_key_id')
S3Secret = config.get('s3', 'aws_secret_access_key')
s3 = boto3.client('s3', aws_access_key_id=S3Key, aws_secret_access_key=S3Secret)
s3r = boto3.resource('s3', aws_access_key_id=S3Key, aws_secret_access_key=S3Secret)

# Target
block = '4D'
farm = 'Quatacker-Burns'
client = '591daa81e1cf4d8cbfbb1bf6'
session_name = '4d3'
s3prefix = '{}/results/farm_{}/block_{}/{}'.format(client, farm, block, session_name)

# Series
roles = ['preprocess-frames','detection','process-frames']
plots = dict.fromkeys(roles)
colors = ['rgb(0, 200, 255)', 'rgb(0, 20, 255)', 'rgb(20, 255, 0)']

# Populate series
for color, role, token in zip(colors, roles, tls.get_credentials_file()['stream_ids']):
    plots[role] = dict()
    plots[role]['meta'] = dict()
    plots[role]['stream'] = dict()
    
    plots[role]['meta']['color'] = color
    plots[role]['meta']['token'] = token
    plots[role]['stream'] = dict(token=token, maxpoints=SHOW_HOURS*60*60/DELAY)


# Generate traces
traces = list()
for role in roles:
    traces.append(go.Scatter(x=[], y=[], stream=plots[role]['stream'], name=role, marker=dict(color=plots[role]['meta']['color'])))


# Set figure layout
layout = go.Layout(
    title='Pipeline Completion ({})'.format(session_name),
    yaxis=dict(
        title='S3 Count'
    ),
    xaxis=dict(
        title='Time'
    ),
    showlegend=True
)


# Initialize plot
fig = go.Figure(data=traces, layout=layout)
plot_url = py.plot(fig, filename='pipeline-update')


# Create stream links
for role in roles:
    plots[role]['meta']['link'] = py.Stream(stream_id=plots[role]['meta']['token'])
    plots[role]['meta']['link'].open()


# Run loop
while True:
    x = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
    for role in roles:
        try:
            count = len(s3.list_objects(Bucket=config.get('s3', 'bucket'), Prefix='{}/results/farm_{}/block_{}/{}/{}'.format(client, farm, block, session_name, role))['Contents'])
        except:
            count = 0
        plots[role]['meta']['link'].write(dict(x=x, y=count))
    time.sleep(DELAY)


# This is meant to gracefully close the stream links
# TODO: This is currently unreachable
for role in roles:
    plots[role]['meta']['link'].close()