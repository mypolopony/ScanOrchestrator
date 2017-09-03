from utils.models_min import Task
from kombu import Connection, Producer, Exchange, Queue
import numpy as np
import ConfigParser
import os
import yaml
import glob
import requests
from pprint import pprint
import binascii
import argparse
from orchestrator import sendtoRabbitMQ

# Load config file
config = ConfigParser.ConfigParser()
config_parent_dir = '.'
config_path = os.path.join(config_parent_dir, 'utils', 'poller.conf')
config.read(config_path)

# Kombu connection
conn = Connection('amqp://{}:{}@{}:5672//'.format(config.get('rmq', 'username'),
                                                  config.get('rmq', 'password'),
                                                  config.get('rmq', 'hostname'))).connect()

chan = conn.channel()


def insert(task):
    print('\nInserting into {}:\n'.format(task['role']))
    pprint(task)

    ex = Exchange(task['role'], type='topic', channel=chan)
    producer = Producer(channel=chan, exchange=ex)
    Queue('_'.join([task['role'], task['session_name']]), exchange=ex,
          channel=chan, routing_key=task['session_name']).declare()
    producer.publish(task, routing_key=task['session_name'])


def create_routing(session_name):
    print('\nEnsuring exchanges / queues')

    for role in ['rvm','preprocess','detection','process']:
        # This is probably unnecessary now, it just defines the exchanges
        ex = Exchange(role, channel=chan, type='topic').declare()

        Queue('_'.join([role, session_name]), exchange=ex, channel=chan).bind_to(exchange=role, routing_key=session_name)


def reset_connections():
    '''
    This is a crude way to reset all incoming connections from this IP to the RabbitMQ server --
    Not great to do all the time, but okay when first starting out.

    These are simple HTTP calls
    '''

    # Obtain the public IP
    # TODO: This only really works when running from a jumpbox
    myip = requests.get('http://ifconfig.co/json').json()['ip']

    # Get the existing connections
    connections = requests.get('http://{}:{}@dash.agridata.ai:15672/api/connections/'.format(config.get('rmq','username'), config.get('rmq','password'))).json()
    print('\nDropping {} connections'.format(len(connections)))
    for connection in connections:
        if connection['peer_host'] == myip:
            print('-- {}'.format(connection['name']))
            requests.delete('http://dash.agridata.ai/orchestrator:15672/api/connections/{}'.format(connection['name']))


if __name__ == '__main__':
    # Task definition
    for taskfile in glob.glob('tasks/*.yaml'):
        print('Reading task file: {}'.format(taskfile))
        task = yaml.load(open(taskfile, 'r'))
        task = Task(client_name=task['client_name'],
                    farm_name=task['farm_name'],
                    block_name=task['block_name'],
                    session_name=task['session_name'],
                    cluster_model=task['detection_params']['cluster_model'],
                    trunk_model=task['detection_params']['trunk_model'],
                    test=task['test'],
                    exclude_scans=task['exclude_scans'],
                    include_scans=task['include_scans'],
                    role=task['role']).to_json()

        # Reset connections
        reset_connections()
        
        # Create exchanges and queues
        # NOTE: The class has dot notation, 
        # NOTE: the json, required for messaging, used in insert() does not
        create_routing(task['session_name'])

        # Insert
        insert(task)

    chan.close()