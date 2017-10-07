import argparse
from models_min import *
from connection import *
import glob
import datetime
import boto3
import yaml
import os
import csv
import ConfigParser

import datetime
from __init__ import RedisManager
import ConfigParser
import time

def get_top_dir_keys(s3client, bucket_to_search, common_prefix):
    """
    This function takes in the name of an s3 bucket and returns a dictionary
    containing the top level dirs as keys and total filesize and value.
    :param bucket_to_search: a String containing the name of the bucket
    """
    # Setup the output dictionary for running totals
    dirsizedict = {}
    # Create 1 entry for '.' to represent the root folder instead of the default.
    dirsizedict['.'] = 0
    ret_val=[]
    # ------------
    # Setup the AWS Res. and Clients
    #s3 = boto3.resource('s3')
    #s3client = boto3.client('s3')

    # This is a check to ensure a bad bucket name wasn't passed in.   I'm sure there is a better
    # way to check this.   If you have a better method, please comment on the article.
    try:
        response = s3client.head_bucket(Bucket=bucket_to_search)
    except:
        print('Bucket ' + bucket_to_search + ' does not exist or is unavailable. - Exiting')
        quit()

    # since buckets could have more than 1000 items, have to use paginator to iterate 1000 at a time
    paginator = s3client.get_paginator('list_objects')
    pageresponse = paginator.paginate(Bucket=bucket_to_search, Prefix=common_prefix )
    common_prefix_cmpnts=common_prefix.split('/')
    # iterate through each object in the bucket through the paginator.
    for pageobject in pageresponse:

        # Check to see of a buckets has contents, without this an empty bucket would throw an error.
        if 'Contents' in pageobject.keys():

            # if there are contents, then iterate through each 'file'.
            for file in pageobject['Contents']:
                #itemtocheck = s3.ObjectSummary(bucket_to_search, file['Key'])

                # Get Top level directory from the file by splitting the key.
                keylist = file['Key'].split('/')
                assert(len(common_prefix_cmpnts) <= len(keylist))
                ret_val.append(keylist[len(common_prefix_cmpnts)-1])

    return list(set(ret_val))

if __name__ == '__main__':
    pass