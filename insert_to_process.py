import json
import os
import ConfigParser
import boto3

from azure.servicebus import ServiceBusService, Message, Queue
service_bus = ServiceBusService(service_namespace='agridataqueues',
                                shared_access_key_name='sharedaccess',
                                shared_access_key_value='cWonhEE3LIQ2cqf49mAL2uIZPV/Ig85YnyBtdb1z+xo=')

config = ConfigParser.ConfigParser()
config.read('utils/poller.conf')

S3Key = config.get('s3', 'aws_access_key_id')
S3Secret = config.get('s3', 'aws_secret_access_key')
s3 = boto3.client('s3', aws_access_key_id=S3Key, aws_secret_access_key=S3Secret)
s3r = boto3.resource('s3', aws_access_key_id=S3Key, aws_secret_access_key=S3Secret)

# Clear the queue
print('Clearing Queue')
while service_bus.get_queue('process').message_count > 0:
    msg = service_bus.receive_queue_message('process', peek_lock=False).body

detected = [obj['Key'] for obj in s3.list_objects(Bucket='agridatadepot', Prefix='5953469d1fb359d2a7a66287/results/farm_Quintessa/block_DragonsTerrace/temp/detection')['Contents']]


base = '''{"rvm_uri":"s3://agridatadepot/5953469d1fb359d2a7a66287/results/farm_Quintessa/block_DragonsTerrace/temp/rvm.csv","num_retries":0,"farmid":"59643b601fb3595ed5426cba","blockname":"Dragons Terrace","blockid":"59643b611fb3595ed5426cbb","clientid":"5953469d1fb359d2a7a66287","role":"rvm","detection_params":{"folders":[],"s3_aws_secret_access_key":"YlPBiE9s9LV5+ruhKqQ0wsZgj3ZFp6psF6p5OBpZ","input_path":"preprocess-frames","s3_aws_access_key_id":"AKIAJC7XVEAQELBKAANQ","base_url_path":"5953469d1fb359d2a7a66287/results/farm_Quintessa/block_DragonsTerrace/temp","caffemodel_s3_url_trunk":"s3://deeplearning_data/models/best/trunk_june_10_400000.caffemodel","bucket":"agridatadepot","caffemodel_s3_url_cluster":"s3://deeplearning_data/models/best/post-bloom_july_29_2017_390000.caffemodel","output_path":"detection","result":"5953469d1fb359d2a7a66287/results/farm_Quintessa/block_DragonsTerrace/temp/detection/2017-07-11_09-57_22179677_10_10-preprocess-row8-dir1-5of10.zip","session_name":"08-03-18-58-28-180000"},"scanids":["2017-07-11_09-57","2017-07-11_13-59","2017-07-11_09-57","2017-07-11_13-59","2017-07-12_08-19","2017-07-12_09-04"],"tarfiles":["2017-07-11_09-57_22179677_10_09.tar.gz","2017-07-11_09-57_22179677_10_10.tar.gz","2017-07-11_09-57_22179677_10_11.tar.gz"],"farmname":"Quintessa", "test":true}'''
base = json.loads(base)

print('Inserting into queue')

'''
# Insert all
for idx, zipfile in enumerate(detected):
    print('{}/{}'.format(idx,len(detected)))
    base['detection_params']['folders'] = [os.path.basename(zipfile)]
    service_bus.send_queue_message('process', Message(json.dumps(base)))
'''

# Insert n
n = 3
for i in range(0,n):
	base['detection_params']['folders'] = [os.path.basename(detected[n])]
	service_bus.send_queue_message('process', Message(json.dumps(base)))