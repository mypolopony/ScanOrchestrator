from utils.models_min import Task
from kombu import Connection, Producer, Exchange, Queue
import numpy as np
import ConfigParser
import os
from pprint import pprint
import binascii

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
symphony = Exchange('symphony', channel=chan)
producer = Producer(channel=chan, exchange=symphony)
queues = ['QueueA', 'QueueB', 'QueueC']

for queue in queues:
    if queue == 'QueueB':
        Queue(name=queue, exchange=symphony, routing_key=queue, channel=chan,consumer_arguments={'x-priority': 10}).declare()
    else:
        Queue(name=queue, exchange=symphony, routing_key=queue, channel=chan).declare()

def insert(items):
    for item in items:
        item = item.__dict__
        pprint(item)
        producer.publish(item['task'], routing_key=item['task']['queue'])

def generateTasks(num):
    return [Task() for i in xrange(num)]

if __name__ == '__main__':
    insert(generateTasks(100))