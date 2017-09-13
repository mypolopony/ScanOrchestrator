from utils.models_min import Task
from kombu import Connection, Producer, Exchange, Queue
import numpy as np
import ConfigParser
import os
import yaml
import glob
import redis
import sys
import requests
import datetime
import traceback
from pprint import pprint
import binascii
import argparse
from utils import RedisManager
from orchestrator import sendtoRabbitMQ

# Load config file
config = ConfigParser.ConfigParser()
config_parent_dir = '.'
config_path = os.path.join(config_parent_dir, 'utils', 'poller.conf')
config.read(config_path)

# Redis queue
redisman = RedisManager(host=config.get('redis','host'), db=config.get('redis', 'db'), port=config.get('redis','port'), password=config.get('redis','password'))

# Kombu connection
conn = Connection('amqp://{}:{}@{}:5672//'.format(config.get('rmq', 'username'),
                                                  config.get('rmq', 'password'),
                                                  config.get('rmq', 'hostname'))).connect()

chan = conn.channel()


def insert(tasks):
    pprint(tasks)

    # Assume tasks are all destined for the same exchange
    '''
    ex = Exchange(tasks[0]['role'], type='topic', channel=chan)
    producer = Producer(channel=chan, exchange=ex)

    for task in tasks:
        Queue('_'.join([task['role'], task['session_name']]), exchange=ex,
          channel=chan, routing_key=task['session_name']).declare()
        producer.publish(task, routing_key=task['session_name'])
    '''
    for task in tasks:
        redisman.put(task['role'], task['session_name'], task)


def create_routing(session_name):
    print('\nEnsuring exchanges / queues')

    for role in ['rvm','preprocess','detection','process']:
        # This is probably unnecessary now, it just defines the exchanges
        ex = Exchange(role, channel=chan, type='topic').declare()

        Queue('_'.join([role, session_name]), exchange=ex, channel=chan).declare()
        Queue('_'.join([role, session_name]), channel=chan).bind_to(exchange=role, routing_key=session_name)


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

def parse_args():
    '''
    Add other parameters here
    '''
    parser=argparse.ArgumentParser('orchestrator')
    parser.add_argument('-t', '--task', help='taskfile', dest='taskfile', default=None)

    args = parser.parse_args()
    return args


if __name__ == '__main__':
    args = parse_args()

    # Task definition
    if args.taskfile:
        taskfiles = ['tasks/{}'.format(args.taskfile)]
    else:
        taskfiles = glob.glob('tasks/*.yaml')

    for taskfile in taskfiles:
        try:
            print('Reading task file: {}'.format(taskfile))
            task = yaml.load(open(taskfile, 'r'))
            task = Task(client_name=task['client_name'],
                        farm_name=task['farm_name'],
                        block_name=task['block_name'],
                        session_name=task['session_name'],
                        caffemodel_s3_url_cluster=task['detection_params']['caffemodel_s3_url_cluster'],
                        caffemodel_s3_url_trunk=task['detection_params']['caffemodel_s3_url_trunk'],
                        test=task['test'],
                        exclude_scans=task['exclude_scans'],
                        include_scans=task['include_scans'],
                        role=task['role']).to_json()

            # Create exchanges and queues
            # NOTE: The class has dot notation, 
            # NOTE: the json, required for messaging, used in insert() does not
            create_routing(task['session_name'])

            # Add fields to generate process task
            # TODO: Turn responsibility for this over to Task object
            role = task['role']                 # Only one task for RVM
            if role == 'rvm':
                tasks = [task]
            elif role == 'process':
                # Get the list of zips in detection folder
                base_url_path = '{}/results/farm_{}/block_{}/{}'.format(task.clientid, task.farm_name.replace(' ' ,''), task.block_name.replace(' ',''), task.session_name)
                zips = s3.list_objects(Bucket=config.get('s3','bucket'),Prefix='{}/detection'.format(base_url_path))
                zips = [z['Key'] for z in zips['Contents'] if '.zip' in z['Key']]

                # Task template
                task['num_retries'] = 0
                task['detection_params']['base_url_path'] = base_url_path
                task['detection_params']['bucket'] = config.get('s3','bucket')
                task['detection_params']['input_path'] = 'preprocess-frames'
                task['detection_params']['output_path'] = 'detection'
                task['detection_params']['session_name'] = datetime.datetime.now().strftime('%H-%M-%S')

                # Generate the new tasks
                tasks = list()
                for z in zips:
                    newtask = task.copy()
                    newtask['detection_params']['folders'] = [os.path.basename(z)]
                    newtask['detection_params']['result'] = z
                    tasks.append(newtask)

            # Insert
            insert(tasks)

        except Exception as e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            traceback.print_exception(exc_type, exc_value, exc_traceback, file=sys.stdout)

    chan.close()