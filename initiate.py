
import os
import sys
import ConfigParser
import datetime
import time
import json
import argparse
from pprint import pprint

import pika
connection = pika.BlockingConnection(pika.URLParameters('amqp://agridata:agridata@boringmachine/'))
channel = connection.channel()

from kombu import Connection


# Load config file
config = ConfigParser.ConfigParser()
config_parent_dir = '.'
if os.name == 'nt':
    config_parent_dir = r'C:\AgriData\Projects\ScanOrchestrator'
    import matlab.engine
else:
    assert (os.path.basename(os.getcwd()) == 'ScanOrchestrator')
config_path = os.path.join(config_parent_dir, 'utils', 'poller.conf')
assert (os.path.isfile(config_path))
config.read(config_path)


def sendtoKombu(queue, task):
    with Connection('amqp://{}:{}@{}:5672//'.format(config.get('rmq', 'username'),
                                                    config.get('rmq', 'password'),
                                                    config.get('rmq', 'hostname'))) as kbu:
        q = kbu.SimpleQueue(queue)
        q.put(task)
        q.close()

def main(args):
    # Task definition - Start with RVM
    task = {
       'clientid'     : '5953469d1fb359d2a7a66287',
       'farmname'     : 'Quintessa',
       'scanids'      : ['2017-07-11_09-57', '2017-07-11_13-59', '2017-07-11_09-57', '2017-07-11_13-59', '2017-07-12_08-19', '2017-07-12_09-04'],
       'blockname'    : 'Dragons Terrace',
       'role'         : 'rvm',
       'session_name' : args.session_name,
       'test'         : True
    }

    # To start at detection, uncomment and modify this code
    '''
    task['role'] = 'detection'
    task['detection_params'] =  dict(
        bucket='agridatadepot',
        base_url_path='{}/results/farm_{}/block_{}/temp'.format(task['clientid'],task['farmname'].replace(' ',''), task['blockname'].replace(' ','')),
        input_path='preprocess-frames',
        output_path='detection',
        caffemodel_s3_url_cluster='s3://deeplearning_data/models/best/post-bloom_july_13_2017_224000.caffemodel',
        caffemodel_s3_url_trunk='s3://deeplearning_data/models/best/trunk_june_10_400000.caffemodel',
        s3_aws_access_key_id='AKIAJC7XVEAQELBKAANQ',
        s3_aws_secret_access_key='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',
        session_name= datetime.datetime.now().strftime('%m-%d-%H-%M-%S'),
        folders=[ '2017-07-12_09-04_22179677_09_16-preprocess-row142-dir2.zip' ]
        )
    '''

    sendtoKombu('%s_%s' % (task['role'], task['session_name']), task)

def parse_args():
    parser=argparse.ArgumentParser('initiate')
    parser.add_argument('-s', '--session_name', help='session_name', dest='session_name',required=True)
    args = parser.parse_args()
    return args


#nifty code to delete a queue
#channel.queue_delete(queue='detection_sessionkg3')
#connection.close()

# Send the task
#channel.basic_publish(exchange='', routing_key=task['role'], body=json.dumps(task))




if __name__ == '__main__':
    args=parse_args()
    main(args)
