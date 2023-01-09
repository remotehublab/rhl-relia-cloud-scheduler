import json
import requests

receiver_grc = open("receiver.grc").read()
transmitter_grc = open("transmitter.grc").read()

print("Sending a task with priority 5")
result1 = requests.post("http://localhost:6002/scheduler/user/tasks", headers={'relia-secret': 'password'}, json={
    "grc_files": {
         "receiver": {
               "filename": "receiver.grc",
               "content": receiver_grc,
         },
         "transmitter": {
               "filename": "transmitter.grc",
               "content": transmitter_grc,
         },
    },
    "priority": 5,
    "session_id": "session_id5", # will be different in the real world
}).json()
print("Result:")
print(json.dumps(result1, indent=4))

print("Sending a task with priority 1")
result2 = requests.post("http://localhost:6002/scheduler/user/tasks", headers={'relia-secret': 'password'}, json={
    "grc_files": {
         "receiver": {
               "filename": "receiver.grc",
               "content": receiver_grc,
         },
         "transmitter": {
               "filename": "transmitter.grc",
               "content": transmitter_grc,
         },
    },
    "priority": 1, # Higher priority
    "session_id": "session_id1", # will be different in the real world
}).json()
print("Result:")
print(json.dumps(result2, indent=4))


print("As a device, get all the primary tasks, one by one:")


while True:
    print("Requesting one receiver data (it might take some time)...")
    device_data = requests.get("http://localhost:6002/scheduler/devices/tasks/receiver?max_seconds=5", headers={'relia-device': 'uw-s1i1r', 'relia-password': 'password'}).json()
    print(json.dumps(device_data, indent=4))
    if not device_data.get('success') or not device_data.get('taskIdentifier'):
        break

