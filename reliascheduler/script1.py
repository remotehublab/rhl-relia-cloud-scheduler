import os
import redis
import uuid
import time
from datetime import datetime

BASE_KEY = 'uw-depl1'

class Keys:
    def __init__(self, base_key):
        self.base_key = base_key
   
    def tasks(self):
        return f"{self.base_key}:relia:scheduler:tasks"

class Task:
    def __init__(self, base_key, task_identifier):
        self.base_key = base_key
        self.task_identifier = task_identifier

    def identifiers(self):
        return f"{self.base_key}:relia:scheduler:tasks:{self.task_identifier}"

class Device:
    def __init__(self, base_key, device_id):
        self.base_key = base_key
        self.device_id = device_id

    def devices(self):
        return f"{self.base_key}:relia:scheduler:devices:{self.device_id}"

# Access file name and contents for transmitter and receiver
transmitterURL = '/user/transactions/m4fxkAuTr9hnw_xnN1aE4UgRAvMAYBghDfzqsUYRr5g/transmitter/basic_wave_const.grc'
receiverURL = '/user/transactions/m4fxkAuTr9hnw_xnN1aE4UgRAvMAYBghDfzqsUYRr5g/receiver/dynamic_freq.grc'
strip_character = '/'
transmitterFile = open('../../../Documents/relia-web/uploads/' + strip_character.join(transmitterURL.split('/')[3:]), 'r')
transmitterContents = transmitterFile.read()
transmitterFile.close()
receiverFile = open('../../../Documents/relia-web/uploads/' + strip_character.join(receiverURL.split('/')[3:]), 'r')
receiverContents = receiverFile.read()
receiverFile.close()
transmitterName = os.path.basename(transmitterURL)
receiverName = os.path.basename(receiverURL)

# Load into Redis
# NOTE: UUID cannot be used, as the ID generator must be strictly lexicographically increasing to satisfy sorted set requirements in Redis.
r = redis.Redis()

id = "0000000000"
if r.get(BASE_KEY + ":relia:scheduler:id"):
     print(r.get(BASE_KEY + ":relia:scheduler:id"))
     id = str(int(r.get(BASE_KEY + ":relia:scheduler:id").decode()) + 1).zfill(10)
     r.set(BASE_KEY + ":relia:scheduler:id", id)
else:
     r.set(BASE_KEY + ":relia:scheduler:id", "0000000000")

k = Keys(BASE_KEY)
t = Task(BASE_KEY, id)

p = r.pipeline()
p.hset(t.identifiers(), 'id', id)
p.hset(t.identifiers(), 'transmitterFilename', transmitterName)
p.hset(t.identifiers(), 'receiverFilename', receiverName)
p.hset(t.identifiers(), 'transmitterFile', transmitterContents)
p.hset(t.identifiers(), 'receiverFile', receiverContents)
p.hset(t.identifiers(), 'author', "m4fxkAuTr9hnw_xnN1aE4UgRAvMAYBghDfzqsUYRr5g")
p.hset(t.identifiers(), 'startedTime', datetime.now().strftime("%H:%M:%S"))
p.hset(t.identifiers(), 'transmitterAssigned', "null")
p.hset(t.identifiers(), 'receiverAssigned', "null")
p.hset(t.identifiers(), 'status', "queued")
# p.zadd(k.tasks(), {t.identifiers(): 2}, nx=True)
p.execute_command('ZADD', k.tasks(), 'NX', 2, t.identifiers())
result = p.execute()

print("Sanity Check 1: Access tasks from given author (could be easily modified to ID or to all tasks)")
print("ID of task most recently added: " + id)
print("There are " + str(r.zcard(k.tasks())) + " items in the queue by the given author")
for item in r.zrange(k.tasks(), 0, -1):
     if str(r.hget(item, 'author'), 'UTF-8') == "m4fxkAuTr9hnw_xnN1aE4UgRAvMAYBghDfzqsUYRr5g":
          print(str(r.hget(item, 'status'), 'UTF-8') + ", Priority: " + str(r.zscore(k.tasks(), item)))
print("============================================")

#print("Sanity check 2: Remove task given ID (or some other parameter)")
#for item in r.zrange(k.tasks(), 0, -1):
#    if str(r.hget(item, 'id'), 'UTF-8') == id:
#         if str(r.hget(item, 'author'), 'UTF-8') == "m4fxkAuTr9hnw_xnN1aE4UgRAvMAYBghDfzqsUYRr5g":
#              r.zrem(k.tasks(), item)
#              break
#print("============================================")

print("Sanity check 3: Set and check device credentials, with expiration")
d = Device(BASE_KEY, "uw-s1i1r")
r.hset(d.devices(), 'credential', "SOME_PASSWORD")
r.expire(d.devices(), 2)
if r.hget(d.devices(), 'credential'):
     if str(r.hget(d.devices(), 'credential'), 'UTF-8') == "SOME_PASSWORD":
          print("Correctly verified device")
     if str(r.hget(d.devices(), 'credential'), 'UTF-8') == "SOME_OTHER_PASSWORD":
          print("Incorrectly verified device")
else:
     print("Device timed out incorrectly")
print("Waiting 2 seconds...")
time.sleep(2)
if r.hget(d.devices(), 'credential'):
     if str(r.hget(d.devices(), 'credential'), 'UTF-8') == "SOME_PASSWORD":
          print("Incorrectly verified device")
else:
     print("Device timed out correctly")
print("============================================")

print("Sanity check 4: Update status and deviceAssigned, moving queue")
r.hset(d.devices(), 'credential', "SOME_PASSWORD")
r.expire(d.devices(), 120)
r.hset(d.devices(), 'name', "uw-s1i1r")
r.hset(d.devices(), 'type', 'receiver')

d2 = Device(BASE_KEY, "uw-s1i1t")
r.hset(d2.devices(), 'credential', "SOME_PASSWORD")
r.expire(d2.devices(), 120)
r.hset(d2.devices(), 'name', "uw-s1i1t")
r.hset(d2.devices(), 'type', 'transmitter')

r.hset(d.devices(), 'pair', d2.devices())
r.hset(d2.devices(), 'pair', d.devices())

if r.hget(d.devices(), 'credential'):
     if str(r.hget(d.devices(), 'type'), 'UTF-8') == 'receiver':
          if r.hget(r.hget(d.devices(), 'pair'), 'credential'):
               count = -1
               while count >= -1 * r.zcard(k.tasks()):
                    assignment = r.zrange(k.tasks(), count, count)
                    if str(r.hget(assignment[0], 'status'), 'UTF-8') != "processing" and str(r.hget(assignment[0], 'status'), 'UTF-8') != "processing2":
                         r.hset(assignment[0], 'receiverAssigned', str(r.hget(d.devices(), 'name'), 'UTF-8'))
                         r.hset(assignment[0], 'status', "processing")
                         print("Receiver correctly assigned")
                         break
                    else:
                         count -= 1
          else:
               print("Other device is not active")
     else:
          count = -1
          while count >= -1 * r.zcard(k.tasks()):
               assignment = r.zrange(k.tasks(), count, count)
               if str(r.hget(assignment[0], 'receiverAssigned'), 'UTF-8') == str(r.hget(r.hget(d.devices(), 'pair'), 'name'), 'UTF-8'):
                    r.hset(assignment[0], 'transmitterAssigned', str(r.hget(d.devices(), 'name'), 'UTF-8'))
                    print("Transmitter correctly assigned")
                    break
               else:
                    count -= 1

if r.hget(d2.devices(), 'credential'):
     if str(r.hget(d2.devices(), 'type'), 'UTF-8') == 'receiver':
          if r.hget(r.hget(d2.devices(), 'pair'), 'credential'):
               count = -1
               while count >= -1 * r.zcard(k.tasks()):
                    assignment = r.zrange(k.tasks(), count, count)
                    if str(r.hget(assignment[0], 'status'), 'UTF-8') != "processing" and str(r.hget(assignment[0], 'status'), 'UTF-8') != "processing2":
                         r.hset(assignment[0], 'receiverAssigned', str(r.hget(d2.devices(), 'name'), 'UTF-8'))
                         r.hset(assignment[0], 'status', "processing")
                         print("Receiver correctly assigned")
                         break
                    else:
                         count -= 1
          else:
               print("Other device is not active")
     else:
          count = -1
          while count >= -1 * r.zcard(k.tasks()):
               assignment = r.zrange(k.tasks(), count, count)
               if str(r.hget(assignment[0], 'receiverAssigned'), 'UTF-8') == str(r.hget(r.hget(d2.devices(), 'pair'), 'name'), 'UTF-8'):
                    r.hset(assignment[0], 'transmitterAssigned', str(r.hget(d2.devices(), 'name'), 'UTF-8'))
                    print("Transmitter correctly assigned")
                    break
               else:
                    count -= 1

print("Simulating completion of assignment...")
count = -1
while count >= -1 * r.zcard(k.tasks()):
     assignment = r.zrange(k.tasks(), count, count)
     deviceType = str(r.hget(d.devices(), 'type'), 'UTF-8')
     if str(r.hget(assignment[0], deviceType + 'Assigned'), 'UTF-8') == str(r.hget(d.devices(), 'name'), 'UTF-8'):
          if str(r.hget(assignment[0], 'status'), 'UTF-8') == "processing":
               r.hset(assignment[0], 'status', "processing2")
               break
          elif str(r.hget(assignment[0], 'status'), 'UTF-8') == "processing2":
               r.hset(assignment[0], 'status', "completed")
               r.zincrby(k.tasks(), -1, assignment[0])
               break
     else:
          count -= 1

count = -1
while count >= -1 * r.zcard(k.tasks()):
     assignment = r.zrange(k.tasks(), count, count)
     deviceType = str(r.hget(d2.devices(), 'type'), 'UTF-8')
     if str(r.hget(assignment[0], deviceType + 'Assigned'), 'UTF-8') == str(r.hget(d2.devices(), 'name'), 'UTF-8'):
          if str(r.hget(assignment[0], 'status'), 'UTF-8') == "processing":
               r.hset(assignment[0], 'status', "processing2")
               break
          elif str(r.hget(assignment[0], 'status'), 'UTF-8') == "processing2":
               r.hset(assignment[0], 'status', "completed")
               r.zincrby(k.tasks(), -1, assignment[0])
               break
     else:
          count -= 1
print("============================================")