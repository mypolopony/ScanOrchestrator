import redis

class RedisManager(object):
    '''
    Simple Queue with Redis Backend'''


    def __init__(self, **redis_kwargs):
        '''
        The default connection parameters are: host='localhost', port=6379, db=0
        '''

        self.db = redis.Redis(**redis_kwargs)

2
    def nq(self, namespace, queue):
        '''
        Simple wrapper function to return the constructed name of the queue
        '''
        return '{}:{}'.format(namespace,queue)


    def qsize(self, namespace, queue):
        '''
        Return the approximate size of the queue.
        '''

        return self.db.llen(nq(namespace,queue))


    def empty(self, namespace, queue):
        '''
        Return True if the queue is empty, False otherwise.
        '''

        return self.qsize(nq(namespace,queue)) == 0


    def put(self, namespace, queue, item):
        '''
        Put item into the queue.
        '''

        self.db.rpush(namespace, queue, item)


    def get(self, namespace, queue):
        '''
        Remove and return an item from the queue. 

        If optional args block is true and timeout is None (the default), block
        if necessary until an item is available.
        '''
        
        item = self.db.lpop(self.key)

        if item:
            item = item[1]
        return item


    def list_queues(self, namespace=namespace):
        '''
        List all queues by namespace
        '''
        return r.keys('*{}*'.format(namespace))