import redis as _redis

class RedisManager(object):
    '''
    Simple Queue with Redis Backend
    '''


    def __init__(self, **redis_kwargs):
        '''
        The default connection parameters are: host='localhost', port=6379, db=0
        '''

        self.db = _redis.Redis(**redis_kwargs)


    def qsize(self, queue):
        '''
        Return the approximate size of the queue.
        '''

        return self.db.llen(queue)


    def empty(self, queue):
        '''
        Return True if the queue is empty, False otherwise.
        '''

        return self.qsize(queue) == 0


    def put(self, queue, item):
        '''
        Put item into the queue.
        '''

        self.db.rpush(queue, item)


    def get(self, queue):
        '''
        Remove and return an item from the queue. 

        '''

        message = self.db.lpop(queue)
        # The string returned must be edited to form a proper JSON:
        # - change from unicode
        message = message.replace("u'", "'")
        #     - single quotes-->double quotes
        message = message.replace("'", '"')
        #     - boolean values to 0/1
        message = message.replace('False', '0').replace('True', '1')

        return json.loads(message)


    def list_queues(self, namespace):
        '''
        List all queues by namespace
        '''
        return self.db.keys('*{}*'.format(namespace))