import utils
import ConfigParser
import os


# Load config file
config = ConfigParser.ConfigParser()
config_path = os.path.join('utils', 'poller.conf')
config.read(config_path)

redisman = RedisManager(host=config.get('redis','host'), db=config.get('redis', 'db'), port=config.get('redis','port'))

dlqueues = redisman.list_queues('dlq')

for q in dlqueues:
    print('Reviving {}'.format(q))
    redisman.revive(q,q.replace('dlq','detection'))