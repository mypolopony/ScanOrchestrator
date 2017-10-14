import os
import datetime
from __init__ import RedisManager
import ConfigParser
import time
from pprint import pprint

# Load config file
config = ConfigParser.ConfigParser()
config_parent_dir = '.'
config_path = os.path.join(config_parent_dir, 'utils', 'poller.conf')
config.read(config_path)

redisman = RedisManager(host=config.get('redis','host'), db=os.getenv('REDIS_DB'), port=config.get('redis','port'))

def generator():
    return redisman.status()

if __name__ == '__main__':
    while True:
        print('-----------')
        print(datetime.datetime.strftime(datetime.datetime.now(),'%c'))
        pprint(generator())
        time.sleep(10)
