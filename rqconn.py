from kombu import Connection, Exchange, Queue, Consumer
import ConfigParser
import os

# Config
config = ConfigParser.ConfigParser()
config_path = os.path.join(os.path.curdir(), 'utils', 'poller.conf')
config.read(config_path)

class rqconn:
    '''
    For RabbitMQ Connections
    '''


    def __init__(self, id, role, topic):
        self.conn = Connection('amqp://{}:{}@{}:{}//'.format(config.get('rmq', 'username'),
                                                             config.get('rmq', 'password'),
                                                             config.get('rmq', 'hostname'),
                                                             config.get('rmq', 'port')))


        self.id = id
        self.exchange= Exchange(self.id, type='topic')
        self.queue = Queue(name='queueidentifier', exchange=self.exchange, routing_key=topic)


    def change_role(self,newrole):
        pass


    def receive(num=1):
        