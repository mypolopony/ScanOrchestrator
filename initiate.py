import datetime
import time
import json
from pprint import pprint

import pika
connection = pika.BlockingConnection(pika.URLParameters('amqp://agridata:agridata@boringmachine/'))
channel = connection.channel()

from kombu import Connection

def sendtoKombu(queue, message):
    with Connection('amqp://agridata:agridata@boringmachine:5672//') as kbu:
        q = kbu.SimpleQueue(queue)
        q.put(task)
        q.close()

# Task definition - Start with RVM
task = {
   'clientid'     : '5953469d1fb359d2a7a66287',
   'farmname'     : 'Quintessa',
   'scanids'      : ['2017-07-11_09-57', '2017-07-11_13-59', '2017-07-11_09-57', '2017-07-11_13-59', '2017-07-12_08-19', '2017-07-12_09-04'],
   'blockname'    : 'Dragons Terrace',
   'role'         : 'rvm',
   'test'         : True
}

# To start at detection, uncomment and modify this code
'''
task['role'] = 'detection'
task['detection_params'] =  dict(
    bucket='agridatadepot',
    base_url_path='{}/results/farm_{}/block_{}/temp'.format(task['clientid'],task['farmname'].replace(' ',''), task['blockname']),
    input_path='preprocess-frames',
    output_path='detection',
    caffemodel_s3_url_cluster='s3://deeplearning_data/models/best/post-bloom_july_13_2017_224000.caffemodel',
    caffemodel_s3_url_trunk='s3://deeplearning_data/models/best/trunk_june_10_400000.caffemodel',
    s3_aws_access_key_id='AKIAJC7XVEAQELBKAANQ',
    s3_aws_secret_access_key='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',
    session_name= datetime.datetime.now().strftime('%m-%d-%H-%M-%S'),
    folders=[ '2017-06-30_10-01_22179677_10_28-preprocess-row39-dir1.zip' ]
    )
'''

# Send the task
#channel.basic_publish(exchange='', routing_key=task['role'], body=json.dumps(task))
sendtoKombu(task['role'], task)