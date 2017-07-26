from azure.servicebus import ServiceBusService, Message, Queue
bus_service = ServiceBusService(service_namespace='agridataqueues',
                                shared_access_key_name='sharedaccess',
                                shared_access_key_value='cWonhEE3LIQ2cqf49mAL2uIZPV/Ig85YnyBtdb1z+xo=')

# Task definition
task = {
   'clientid'     : '5953469d1fb359d2a7a66287',
   'scanids'      : ['2017-06-30_10-01'],
   'role'         : 'rvm',
}

# Send to RVM Queue
bus_service = bus_service.send_queue_message(task['role'], Message(task))