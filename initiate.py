from utils.models_min import Task
from kombu import Connection, Producer, Exchange, Queue
import numpy as np
import ConfigParser
import os
from pprint import pprint
import binascii
import argparse

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

'''
for queue in queues:
    if queue == 'QueueB':
        Queue(name=queue, exchange=symphony, routing_key=queue, channel=chan,consumer_arguments={'x-priority': 10}).declare()
    else:
        Queue(name=queue, exchange=symphony, routing_key=queue, channel=chan).declare()
'''

def insert(items):
    for item in items:
        item = item.__dict__
        pprint(item)
        producer.publish(item['task'], routing_key=item['task']['queue'])


def generateTasks(num):
    return [Task() for i in xrange(num)]


def setup_exchanges():
    print('Ensuring exchanges')

    Exchange('gates_rvm', channel=chan).declare()
    Exchange('gates_preprocess', channel=chan).declare()
    Exchange('gates_process', channel=chan).declare()
    Exchange('torvalds_detection', channel=chan).declare()


if __name__ == '__main__':
    task = yaml.load(open('task.yaml','r'))
    task = Task(client_name=task['client_name'],
                farm_name=task['farm_name'],
                 client_name,
                 farm_name,
                 block_name,
                 session_name = None,
                 cluster_model='s3://deeplearning_data/models/best/cluster_june_15_288000.caffemodel', 
                 trunk_model='s3://deeplearning_data/models/best/trunk_june_10_400000.caffemodel',
                 test=True, 
                 exclude_scans=None, 
                 include_scans=None,
                 role='rvm'):
    args = parse_args()
    set_up_queues()
    main(args)
