class RemoteQueue(object):
    
    def factory(type, params=None):
        if type == 'AzureServiceBus': return ServiceBusQueue(params)
        if type == 'RabbitMQ': return RabbitQueue(params)
        assert(0, 'Could not create type: {}'.format(type))


class ServiceBusQueue:
    '''
    Interface for Azure's Service Bus
    '''

    from azure.servicebus import ServiceBusService, Message, Queue

    def __init__(self, params):
        '''
        Required keys in params: 'service_namespace', 'shared_access_key_name', 'shared_access_key_value'
        '''
        self.service_bus = ServiceBusService(params['service_namespace'],
                                             params['shared_access_key_name'],
                                             params['shared_access_key_value'])

    def send(self, queue, message):
        '''
        Send a message (should be dict)
        '''
        message['num_retries'] = 0              # Reset number of retries
        service_bus.send_queue_message(queue, Message(json.dumps(message)))


    def receive(self, queue, lock=False):
        '''
        Receive a message
        '''
        incoming = None
        while not incoming:
            incoming = service_bus.receive_queue_message(queue, peek_lock=lock).body

        # Here, we can accept either properly formatted JSON strings or, if necessary, convert the
        # quotes to make them compliant.
        try:
            msg = json.loads(incoming)
        except:
            msg = json.loads(incoming.replace("'",'"').replace('u"','"'))

        return msg


class RabbitQueue:
    '''
    Interface for RabbitMQ broker
    '''

    from kombu import Connection

    def __init__(self, params):
        '''
        Required keys in params: 'usename', 'password', 'hostname'
        '''
        self.username = params['username']
        self.password = params['password']
        self.hostname = params['hostname']


    def send(self, queue, message):
        '''
        Send a message
        '''
        message['num_retries'] = 0                  # Reset number of retries
        with Connection('amqp://{}:{}@{}:5672//'.format(self.username, self.password, self.hostname)) as kbu:
            q = kbu.SimpleQueue(queue)
            q.put(message)
            q.close()


    def receive(self, queue, num=1):
        '''
        Receive message(s)
        '''
        msgs = list()
        with Connection('amqp://{}:{}@{}:5672//'.format(self.username, self.password, self.hostname)) as kbu:
            q = kbu.SimpleQueue(queue)
            while(len(msgs) < num and q.qsize > 0):
                message = q.get(block=True)
                message.ack()
                msgs.append(message.payload)
            q.close()

            # If only one task is requested, release it from the list
            if num == 1:
                return msgs[0]
            # Otherwise, return a list of tasks
            else:
                return msgs