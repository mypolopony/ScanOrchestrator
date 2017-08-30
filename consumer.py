from utils.models_min import Task
from kombu import Connection, Consumer, Exchange, Queue
from kombu.utils.compat import nested
import numpy as np
import ConfigParser
import os
import socket
import requests
import binascii

# Load config file
config = ConfigParser.ConfigParser()
config_parent_dir = '.'
config_path = os.path.join(config_parent_dir, 'utils', 'poller.conf')
config.read(config_path)

# Kombu connection
rabbit_url = '{}:{}@{}'.format(config.get('rmq', 'username'), config.get('rmq', 'password'), config.get('rmq', 'hostname'))
conn = Connection('amqp://{}:5672//'.format(rabbit_url)).connect()
chan = conn.channel()

print(conn)

def establish_connection():
    revived_connection = Connection('amqp://{}:5672//'.format(rabbit_url)).connect()
    revived_connection.ensure_connection(max_retries=3)
    channel = fresh_connection.channel()
    consumer.revive(channel)
    consumer.consume()
    print('Fresh connection!')
    return fresh_connection


def process_message(body, message):
    print('Message received: {}\n\t{}'.format(message,body))
    message.ack()


def list_bound_queues(exchange):
    '''
    In a canonical AMQP framework, there is no way of listing an Exchange's bindings. Canonically,
    publishers publish messages to an exchange and consumers declare queues and determine routing.

    Since a machine will not have access to routing keys (session_names), we must use the Management
    Plugin and call for a list of the bindings using the HTTP API
    '''
    r = requests.get('http://{}:15672/api/bindings/%2f/'.format(rabbit_url))
    bindings = [binding for binding in r.json() if binding['source'] == exchange]
    return [Queue(b['destination']) for b in bindings]


def single():
    while True:
        try:
            conn.drain_events(timeout=2)
        except socket.timeout:
            print('Waiting')
            pass 
        except conn.connection_errors:
            conn = establish_connection()
            #print('Lost Connection')
            #break



# Must be after process_message
# consumer = Consumer(channel=chan, queues=list_bound_queues('symphony'), callbacks=[process_message], auto_declare=False)
# consumer.consume()

def multiple():
    cha = conn.channel()
    chb = conn.channel()
    with nested(Consumer(cha, Queue(name='QueueA'), callbacks=[process_message], auto_declare=False),
                Consumer(chb, Queue(name='QueueB'), callbacks=[process_message], auto_declare=False)):
        conn.drain_events(timeout=2)


if __name__ == '__main__':
    # single()
    multiple()