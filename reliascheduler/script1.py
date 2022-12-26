import os
import redis
import uuid
from datetime import datetime

class Keys:
    def __init__(self, base_key):
        self.base_key = base_key
   
    @property
    def tasks(self):
        return f"{self.base_key}:relia:scheduler:tasks"

    class Task:
        def __init__(self, base_key, task_identifier):
             self.base_key = base_key
             self.task_identifier = task_identifier

        @property
        def identifiers(self):
             return f"{self.base_key}:relia:scheduler:tasks:{self.task_identifier}"

    class queuePriority:
        def __init__(self, base_key, queue_priority):
             self.base_key = base_key
             self.queue_priority = queue_priority

        @property
        def queuePriority(self):
             return f"{self.base_key}:relia:scheduler:tasks:{self.queue_priority}"

# Access file name and contents for transmitter and receiver
transmitterURL = '/user/transactions/m4fxkAuTr9hnw_xnN1aE4UgRAvMAYBghDfzqsUYRr5g/transmitter/basic_histogram.grc'
receiverURL = '/user/transactions/m4fxkAuTr9hnw_xnN1aE4UgRAvMAYBghDfzqsUYRr5g/receiver/basic_wave_time_const.grc'
strip_character = '/'
transmitterFile = open('../../../Documents/relia-web/reliaweb/views/uploads/' + strip_character.join(transmitterURL.split('/')[3:]), 'r')
transmitterContents = transmitterFile.read()
transmitterFile.close()
receiverFile = open('../../../Documents/relia-web/reliaweb/views/uploads/' + strip_character.join(receiverURL.split('/')[3:]), 'r')
receiverContents = receiverFile.read()
receiverFile.close()
transmitterName = os.path.basename(transmitterURL)
receiverName = os.path.basename(receiverURL)

# Load into Redis
r = redis.Redis()
k = Keys('uw-depl1')
t = Keys.Task(k, id)
q = Keys.queuePriority(k, '1')
id = uuid.uuid1()

p = r.pipeline()
p.sadd(k.tasks, str(id))
p.hset(t.identifiers, 'transmitterFilename', transmitterName)
p.hset(t.identifiers, 'receiverFilename', receiverName)
p.hset(t.identifiers, 'transmitterFile', transmitterContents)
p.hset(t.identifiers, 'receiverFile', receiverContents)
p.hset(t.identifiers, 'author', "m4fxkAuTr9hnw_xnN1aE4UgRAvMAYBghDfzqsUYRr5g")
p.hset(t.identifiers, 'startedTime', datetime.now().strftime("%H:%M:%S"))
p.hset(t.identifiers, 'deviceAssigned', "null")
p.lpush(q.queuePriority, str(id))
result = p.execute()

# Sanity check
for item in r.lrange(q.queuePriority, 0, -1):
    print(item)
