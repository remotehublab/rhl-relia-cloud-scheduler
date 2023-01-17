import json
import requests
import sys

receiver_grc = open("receiver.grc").read()
transmitter_grc = open("transmitter.grc").read()

most_recent_task_id = ""
receiver_1_assign = ""
receiver_2_assign = ""
transmitter_1_assign = ""
transmitter_2_assign = ""

for i in range(len(sys.argv) - 1):
    match sys.argv[i]:
        case "A2":
            print("Sending a task with priority 2")
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
                "priority": 2,
                "session_id": "session_id1",
            }).json()
            print(result1['message'])
            if result1['success']:
                most_recent_task_id = result1['taskIdentifier']
        case "A5":
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
                "session_id": "session_id1",
            }).json()
            print(result1['message'])
            if result1['success']:
                most_recent_task_id = result1['taskIdentifier']
        case "A8":
            print("Sending a task with priority 8")
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
                "priority": 8,
                "session_id": "session_id1",
            }).json()
            print(result1['message'])
            if result1['success']:
                most_recent_task_id = result1['taskIdentifier']
        case "D":
            print("Deleting most recent task added")
            if most_recent_task_id == "":
                print("No task added")
            else:
                result1 = requests.post("http://localhost:6002/scheduler/user/tasks/" + most_recent_task_id, headers={'relia-secret': 'password'}, json={'action': 'delete'}).json()
                print(result1['message'])
        case "R1A":
            print("Receiver 1 requesting assignment (it might take some time)...")
            device_data = requests.get("http://localhost:6002/scheduler/devices/tasks/receiver?max_seconds=5", headers={'relia-device': 'uw-s1i1:r', 'relia-password': 'password'}).json()
            if device_data.get('taskIdentifier'):
                receiver_1_assign = device_data.get('taskIdentifier')
            print(device_data.get('message'))
        case "R2A":
            print("Receiver 2 requesting assignment (it might take some time)...")
            device_data = requests.get("http://localhost:6002/scheduler/devices/tasks/receiver?max_seconds=5", headers={'relia-device': 'uw-s1i2:r', 'relia-password': 'password'}).json()
            if device_data.get('taskIdentifier'):
                receiver_2_assign = device_data.get('taskIdentifier')
            print(device_data.get('message'))
        case "T1A":
            print("Transmitter 1 requesting assignment")
            device_data = requests.get("http://localhost:6002/scheduler/devices/tasks/transmitter?max_seconds=5", headers={'relia-device': 'uw-s1i1:t', 'relia-password': 'password'}).json()
            if device_data.get('taskIdentifier'):
                transmitter_1_assign = device_data.get('taskIdentifier')
            print(device_data.get('message'))
        case "T2A":
            print("Transmitter 2 requesting assignment")
            device_data = requests.get("http://localhost:6002/scheduler/devices/tasks/transmitter?max_seconds=5", headers={'relia-device': 'uw-s1i2:t', 'relia-password': 'password'}).json()
            if device_data.get('taskIdentifier'):
                transmitter_2_assign = device_data.get('taskIdentifier')
            print(device_data.get('message'))
        case "R1C":
            print("Receiver 1 completing task")
            device_data = requests.post("http://localhost:6002/scheduler/devices/tasks/receiver/" + receiver_1_assign, headers={'relia-device': 'uw-s1i1:r', 'relia-password': 'password'}).json()
            print(device_data.get('status'))
        case "R2C":
            print("Receiver 2 completing task")
            device_data = requests.post("http://localhost:6002/scheduler/devices/tasks/receiver/" + receiver_2_assign, headers={'relia-device': 'uw-s1i2:r', 'relia-password': 'password'}).json()
            print(device_data.get('status'))
        case "T1C":
            print("Transmitter 1 completing task")
            device_data = requests.post("http://localhost:6002/scheduler/devices/tasks/transmitter/" + transmitter_1_assign, headers={'relia-device': 'uw-s1i1:t', 'relia-password': 'password'}).json()
            print(device_data.get('status'))
        case "T2C":
            print("Transmitter 2 completing task")
            device_data = requests.post("http://localhost:6002/scheduler/devices/tasks/transmitter/" + transmitter_2_assign, headers={'relia-device': 'uw-s1i2:t', 'relia-password': 'password'}).json()
            print(device_data.get('status'))
