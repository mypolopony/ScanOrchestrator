import redis
import random
from RedisQueue import RedisQueue

id = str(random.random())[-3:]

q = RedisQueue('a5a',namespace='preprocess', host='52.191.141.93', db=0, port=6379, password='0jCX70CmceFgN1XhEoPopAajWN8CtXSjEmt5DgEUaV8=')
q.put({'hseon':'yeah'})