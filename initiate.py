from utils.models_min import Task
from kombu import Connection, Producer, Exchange, Queue
import numpy as np
import ConfigParser
import os
import yaml
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
    task = task.to_json()
    print('\nInserting into {}:\n'.format(task['role']))
    pprint(task)

    ex = Exchange(task['role'], type='topic', channel=chan)
    producer = Producer(channel=chan, exchange=ex)
    Queue('_'.join([task['role'], task['session_name']]), exchange=ex,
          channel=chan, routing_key=task['session_name']).declare()
    producer.publish(task, routing_key=task['session_name'])


def setup_exchanges():
    print('Ensuring exchanges')

    Exchange('rvm', channel=chan, type='topic').declare()
    Exchange('preprocess', channel=chan, type='topic').declare()
    Exchange('process', channel=chan, type='topic').declare()
    Exchange('detection', channel=chan, type='topic').declare()


if __name__ == '__main__':
    # Create exchanges (indempotent)
    setup_exchanges()

    # Task definition
    task = yaml.load(open('task.yaml', 'r'))
    task = Task(client_name=task['client_name'],
                farm_name=task['farm_name'],
                block_name=task['block_name'],
                session_name=task['session_name'],
                cluster_model=task['detection_params']['cluster_model'],
                trunk_model=task['detection_params']['trunk_model'],
                test=task['test'],
                exclude_scans=task['exclude_scans'],
                include_scans=task['include_scans'],
                role=task['role'])
    insert(task)
