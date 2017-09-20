from connection import *
from models_min import *
import boto3
from bson.objectid import ObjectId
from pprint import pprint
import json

# AWS Resources: S3
S3Key = 'AKIAIQYWKQQF5NKCCPGA'
S3Secret = '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'
s3 = boto3.client('s3', aws_access_key_id=S3Key, aws_secret_access_key=S3Secret)
s3r = boto3.resource('s3', aws_access_key_id=S3Key, aws_secret_access_key=S3Secret)
s3bucket = 'agridatadepot'

broken = Scan.objects(blocks__exists=False,id__gt=ObjectId("599f33afb3ba610368511634"))
for scan in broken:
    print('{}\t{}'.format(Client.objects.get(id=ObjectId(scan.client)).name, scan.scanid))
    # pprint(json.loads(scan.to_json()))
    print('\t{}'.format(scan.notes))
    files = s3.list_objects(Bucket=s3bucket, Prefix='{}/{}/'.format(str(scan.client), scan.scanid))['Contents']
    print('{} minutes'.format((len(files) - 2)/2))