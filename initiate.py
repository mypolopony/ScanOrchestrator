import datetime
import time
from pprint import pprint
from azure.servicebus import ServiceBusService, Message, Queue
service_bus = ServiceBusService(service_namespace='agridataqueues',
                                shared_access_key_name='sharedaccess',
                                shared_access_key_value='cWonhEE3LIQ2cqf49mAL2uIZPV/Ig85YnyBtdb1z+xo=')

# Task definition
task = {
   'clientid'     : '5953469d1fb359d2a7a66287',
   'farmname'     : 'UpperRange',
   'scanids'      : ['2017-06-30_10-01'],
   'blockname'    : 'G2',
   'role'         : 'rvm',
}

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

for i in range(1,100):
    service_bus.send_queue_message(task['role'], Message(task))