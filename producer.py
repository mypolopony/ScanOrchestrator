from utils.models_min import Task
from kombu import Connection, Producer, Exchange, Queue
import numpy as np
import ConfigParser
import os
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
queues = ['QueueA', 'QueueB', 'QueueC']
symphony = Exchange('symphony', channel=chan)
producer = Producer(channel=chan, exchange=symphony)

for queue in queues:
    if queue == 'QueueB':
        Queue(name=queue, exchange=symphony, routing_key=queue, channel=chan,consumer_arguments={'x-priority': 10}).declare()
    else:
        Queue(name=queue, exchange=symphony, routing_key=queue, channel=chan).declare()

def insert(tasks):
    for task in tasks:
        producer.publish(task.message, routing_key=task.queue)

def generateTasks(num):
    return [Task(binascii.b2a_hex(os.urandom(15)), np.random.choice(queues)).__dict__() for i in xrange(num)]

if __name__ == '__main__':
    insert(generateTasks(100))