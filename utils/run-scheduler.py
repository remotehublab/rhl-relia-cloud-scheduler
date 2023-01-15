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
    device_data = requests.get("http://localhost:6002/scheduler/devices/tasks/receiver?max_seconds=5", headers={'relia-device': 'uw-s1i1:r', 'relia-password': 'password'}).json()
    print(json.dumps(device_data, indent=4))
    if not device_data.get('success') or not device_data.get('taskIdentifier'):
        break
    elif device_data.get('taskIdentifier'):
        taskID1 = device_data.get('taskIdentifier')
        print(taskID1)
    else:
        print("TaskID1 not set")

    print("Requesting as transmitter")
    device_data = requests.get("http://localhost:6002/scheduler/devices/tasks/transmitter?max_seconds=5", headers={'relia-device': 'uw-s1i1:t', 'relia-password': 'password'}).json()
    print(json.dumps(device_data, indent=4))
    if not device_data.get('success') or not device_data.get('taskIdentifier'):
        break
    elif device_data.get('taskIdentifier'):
        taskID2 = device_data.get('taskIdentifier')
        print(taskID2)
    else:
        print("TaskID2 not set")

    print("Transmitter completing task")
    device_data = requests.post("http://localhost:6002/scheduler/devices/tasks/transmitter/" + taskID2, headers={'relia-device': 'uw-s1i1:t', 'relia-password': 'password'}).json()
    if not device_data.get('success'):
        print("Transmitter completion failed")
        break
    else:
        print("Transmitter completion successful")

    print("Receiver completing task")
    device_data = requests.post("http://localhost:6002/scheduler/devices/tasks/receiver/" + taskID1, headers={'relia-device': 'uw-s1i1:r', 'relia-password': 'password'}).json()
    if not device_data.get('success'):
        print("Transmitter completion failed")
        break
    else:
        print("Transmitter completion successful")

