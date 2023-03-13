import os
import glob
import time
import secrets
import logging
from datetime import datetime

from typing import Optional

from urllib.request import urlopen

import yaml
import numpy as np

from werkzeug.utils import secure_filename
from flask import Blueprint, jsonify, current_app, request, make_response, send_file

from reliascheduler import redis_store
from reliascheduler.auth import check_backend_credentials, check_device_credentials
from reliascheduler.keys import ErrorKeys, TaskKeys, DeviceKeys

logger = logging.getLogger(__name__)

scheduler_blueprint = Blueprint('scheduler', __name__)

@scheduler_blueprint.route('/user/tasks/<task_identifier>/<user_id>', methods=['GET'])
def get_one_task(task_identifier, user_id):
    t = TaskKeys.identifier(task_identifier)
    author = redis_store.hget(t, TaskKeys.author)
    if author == None:
        pipeline = redis_store.pipeline()
        pipeline.sadd(ErrorKeys.errors(), task_identifier)
        pipeline.hset(ErrorKeys.identifier(task_identifier), ErrorKeys.uniqueIdentifier, task_identifier)
        pipeline.hset(ErrorKeys.identifier(task_identifier), ErrorKeys.author, user_id)
        pipeline.hset(ErrorKeys.identifier(task_identifier), ErrorKeys.errorMessage, "Task identifier does not exist")
        pipeline.hset(ErrorKeys.identifier(task_identifier), ErrorKeys.errorTime, datetime.now().isoformat())
        results = pipeline.execute()
        return jsonify(success=False, status=None, receiver=None, transmitter=None, session_id=None, message="Task identifier does not exist")
    if author != user_id:
        pipeline = redis_store.pipeline()
        pipeline.sadd(ErrorKeys.errors(), task_identifier)
        pipeline.hset(ErrorKeys.identifier(task_identifier), ErrorKeys.uniqueIdentifier, task_identifier)
        pipeline.hset(ErrorKeys.identifier(task_identifier), ErrorKeys.author, user_id)
        pipeline.hset(ErrorKeys.identifier(task_identifier), ErrorKeys.errorMessage, "You do not have permission to access the status of the task")
        pipeline.hset(ErrorKeys.identifier(task_identifier), ErrorKeys.errorTime, datetime.now().isoformat())
        results = pipeline.execute()
        return jsonify(success=False, status=None, receiver=None, transmitter=None, message="You do not have permission to access the status of the task")    
    return jsonify(success=True, status=redis_store.hget(t, TaskKeys.status), receiver=redis_store.hget(t, TaskKeys.receiverAssigned), transmitter=redis_store.hget(t, TaskKeys.transmitterAssigned), session_id=redis_store.hget(t, TaskKeys.sessionId), message="Success")

@scheduler_blueprint.route('/user/all-tasks/<user_id>', methods=['GET'])
def get_all_tasks(user_id):
    task_id = []
    task_status = []
    task_receiver = []
    task_transmitter = []
    for t in redis_store.smembers(TaskKeys.tasks()):
        if redis_store.hget(TaskKeys.identifier(t), TaskKeys.author) == user_id:
            task_id.append(redis_store.hget(TaskKeys.identifier(t), TaskKeys.uniqueIdentifier))
            task_status.append(redis_store.hget(TaskKeys.identifier(t), TaskKeys.status))
            task_receiver.append(redis_store.hget(TaskKeys.identifier(t), TaskKeys.receiverAssigned))
            task_transmitter.append(redis_store.hget(TaskKeys.identifier(t), TaskKeys.transmitterAssigned))
    return jsonify(success=True, ids=task_id, statuses=task_status, receivers=task_receiver, transmitters=task_transmitter, method="Success")

@scheduler_blueprint.route('/user/error-messages/<user_id>', methods=['GET'])
def get_errors(user_id):
#    authenticated = check_backend_credentials()
#    if not authenticated:
#        return jsonify(success=False, message="Invalid secret"), 401

    task_id = []
    error_messages = []
    error_times = []

    for t in redis_store.smembers(TaskKeys.tasks()):
        if redis_store.hget(TaskKeys.identifier(t), TaskKeys.author) == user_id and redis_store.hget(TaskKeys.identifier(t), TaskKeys.errorMessage) != "null":
            task_id.append(redis_store.hget(TaskKeys.identifier(t), TaskKeys.uniqueIdentifier))
            error_messages.append(redis_store.hget(TaskKeys.identifier(t), TaskKeys.errorMessage))
            error_times.append(redis_store.hget(TaskKeys.identifier(t), TaskKeys.errorTime))
    for v in redis_store.smembers(ErrorKeys.errors()):
        if redis_store.hget(ErrorKeys.identifier(v), ErrorKeys.author) == user_id and redis_store.hget(ErrorKeys.identifier(v), ErrorKeys.errorMessage) != "null":
            task_id.append(redis_store.hget(ErrorKeys.identifier(v), ErrorKeys.uniqueIdentifier))
            error_messages.append(redis_store.hget(ErrorKeys.identifier(v), ErrorKeys.errorMessage))
            error_times.append(redis_store.hget(ErrorKeys.identifier(v), ErrorKeys.errorTime))       

    sorted_times = np.argsort([datetime.strptime(x, '%Y-%m-%dT%H:%M:%S.%f') for x in error_times])
    most_recent = sorted(range(len(sorted_times)), key=lambda i: sorted_times[i])[-5:]

    new_task_id = []
    new_error_messages = []
    for i in most_recent:
            new_task_id.append(task_id[i])
            new_error_messages.append(error_messages[i])
    return jsonify(success=True, ids=new_task_id, errors=new_error_messages)

@scheduler_blueprint.route('/user/tasks/poll/<task_id>', methods=['GET', 'POST'])
def poll(task_id):

#    authenticated = check_backend_credentials()
#    if not authenticated:
#        return jsonify(success=False, message="Invalid secret"), 401

    redis_store.setex(f"{TaskKeys.base_key()}:relia:data:tasks:{task_id}:user-active", 15, "1")
    return jsonify(success=True)

@scheduler_blueprint.route('/user/tasks/<user_id>', methods=['POST'])
def load_task(user_id):

    authenticated = check_backend_credentials()
    if not authenticated:
        return jsonify(success=False, message="Invalid secret"), 401

    request_data = request.get_json(silent=True, force=True)
    # It should be something like:
    # {
    #     "grc_files": {
    #         "receiver": {
    #               "filename": "myreceiver-file.grc",
    #               "content": "( the content of the grc in a string; maybe base64 )",
    #         },
    #         "transmitter": {
    #               "filename": "mytransmitter-file.grc",
    #               "content": "( the content of the grc in a string; maybe base64 )",
    #         },
    #    },
    #    "priority": 10 (number between 0 and 15)
    #    "session_id": "asdfadfasfdaf"
    # }

    default_priority = 10
    try:
        priority = int(request_data.get('priority', default_priority) or default_priority)
    except Exception as err:
        logger.warning(f"Invalid priority {priority}, outside range 0-{current_app.config['MAX_PRIORITY_QUEUE']}. Using {default_priority}", exc_info=True)
        priority = default_priority

    if priority < 0 or priority > current_app.config['MAX_PRIORITY_QUEUE']:
        logger.warning(f"Invalid priority {priority}, outside range 0-{current_app.config['MAX_PRIORITY_QUEUE']}. Using {default_priority}")
        priority = default_priority

    session_id = request_data.get('session_id')

    grc_files = request_data.get('grc_files')
    if not grc_files:
        task_identifier = secrets.token_urlsafe()
        while redis_store.sadd(ErrorKeys.errors(), task_identifier) == 0:
            task_identifier = secrets.token_urlsafe()
        pipeline = redis_store.pipeline()
        pipeline.hset(ErrorKeys.identifier(task_identifier), ErrorKeys.uniqueIdentifier, task_identifier)
        pipeline.hset(ErrorKeys.identifier(task_identifier), ErrorKeys.author, user_id)
        pipeline.hset(ErrorKeys.identifier(task_identifier), ErrorKeys.errorMessage, "No grc_files provided")
        pipeline.hset(ErrorKeys.identifier(task_identifier), ErrorKeys.errorTime, datetime.now().isoformat())
        result = pipeline.execute()
        return jsonify(success=False, message="No grc_files provided")

    for grc_file_type in ('receiver', 'transmitter'):
        grc_file_data = grc_files.get(grc_file_type)
        if not grc_file_data:
            task_identifier = secrets.token_urlsafe()
            while redis_store.sadd(ErrorKeys.errors(), task_identifier) == 0:
                task_identifier = secrets.token_urlsafe()
            pipeline = redis_store.pipeline()
            pipeline.hset(ErrorKeys.identifier(task_identifier), ErrorKeys.uniqueIdentifier, task_identifier)
            pipeline.hset(ErrorKeys.identifier(task_identifier), ErrorKeys.author, user_id)
            pipeline.hset(ErrorKeys.identifier(task_identifier), ErrorKeys.errorMessage, f"No {grc_file_type} found in grc_files")
            pipeline.hset(ErrorKeys.identifier(task_identifier), ErrorKeys.errorTime, datetime.now().isoformat())
            result = pipeline.execute()
            return jsonify(success=False, message=f"No {grc_file_type} found in grc_files")

        filename = grc_file_data.get('filename')
        if not filename:
            task_identifier = secrets.token_urlsafe()
            while redis_store.sadd(ErrorKeys.errors(), task_identifier) == 0:
                task_identifier = secrets.token_urlsafe()
            pipeline = redis_store.pipeline()
            pipeline.sadd(ErrorKeys.errors(), task_identifier)
            pipeline.hset(ErrorKeys.identifier(task_identifier), ErrorKeys.uniqueIdentifier, task_identifier)
            pipeline.hset(ErrorKeys.identifier(task_identifier), ErrorKeys.author, user_id)
            pipeline.hset(ErrorKeys.identifier(task_identifier), ErrorKeys.errorMessage, f"No filename found in {grc_file_type} in grc_files")
            pipeline.hset(ErrorKeys.identifier(task_identifier), ErrorKeys.errorTime, datetime.now().isoformat())
            result = pipeline.execute()
            return jsonify(success=False, message=f"No filename found in {grc_file_type} in grc_files")
        content = grc_file_data.get('content')
        if not content:
            task_identifier = secrets.token_urlsafe()
            while redis_store.sadd(ErrorKeys.errors(), task_identifier) == 0:
                task_identifier = secrets.token_urlsafe()
            pipeline = redis_store.pipeline()
            pipeline.hset(ErrorKeys.identifier(task_identifier), ErrorKeys.uniqueIdentifier, task_identifier)
            pipeline.hset(ErrorKeys.identifier(task_identifier), ErrorKeys.author, user_id)
            pipeline.hset(ErrorKeys.identifier(task_identifier), ErrorKeys.errorMessage, f"No content found in {grc_file_type} in grc_files")
            pipeline.hset(ErrorKeys.identifier(task_identifier), ErrorKeys.errorTime, datetime.now().isoformat())
            result = pipeline.execute()
            return jsonify(success=False, message=f"No content found in {grc_file_type} in grc_files")

        try:
            yaml.safe_load(content)
        except Exception as err:
            task_identifier = secrets.token_urlsafe()
            while redis_store.sadd(ErrorKeys.errors(), task_identifier) == 0:
                task_identifier = secrets.token_urlsafe()
            pipeline = redis_store.pipeline()
            pipeline.hset(ErrorKeys.identifier(task_identifier), ErrorKeys.uniqueIdentifier, task_identifier)
            pipeline.hset(ErrorKeys.identifier(task_identifier), ErrorKeys.author, user_id)
            pipeline.hset(ErrorKeys.identifier(task_identifier), ErrorKeys.errorMessage, f"Invalid content (not yaml) for provided {grc_file_type}")
            pipeline.hset(ErrorKeys.identifier(task_identifier), ErrorKeys.errorTime, datetime.now().isoformat())
            result = pipeline.execute()
            return jsonify(success=False, message=f"Invalid content (not yaml) for provided {grc_file_type}")
        # in the future we might check more things about the .grc files

    # We have checked the data, so we can now do:
    transmitter_filename = grc_files['transmitter']['filename']
    transmitter_file = grc_files['transmitter']['content']
    receiver_filename = grc_files['receiver']['filename']
    receiver_file = grc_files['receiver']['content']

    # Load into Redis
    # We rely on a set to know which keys have been added and are unique.
    # The key means that there has been an attempt to create the key, it does not
    # mean that the key is currently active (and might need to be cleaned)
    task_identifier = secrets.token_urlsafe()
    while redis_store.sadd(TaskKeys.tasks(), task_identifier) == 0:
        task_identifier = secrets.token_urlsafe()

    pipeline = redis_store.pipeline()
    pipeline.hset(TaskKeys.identifier(task_identifier), TaskKeys.uniqueIdentifier, task_identifier)
    pipeline.hset(TaskKeys.identifier(task_identifier), TaskKeys.author, user_id)
    pipeline.hset(TaskKeys.identifier(task_identifier), TaskKeys.transmitterFilename, transmitter_filename)
    pipeline.hset(TaskKeys.identifier(task_identifier), TaskKeys.transmitterFile, transmitter_file)
    pipeline.hset(TaskKeys.identifier(task_identifier), TaskKeys.receiverFilename, receiver_filename)
    pipeline.hset(TaskKeys.identifier(task_identifier), TaskKeys.receiverFile, receiver_file)
    pipeline.hset(TaskKeys.identifier(task_identifier), TaskKeys.sessionId, session_id)
    pipeline.hset(TaskKeys.identifier(task_identifier), TaskKeys.startedTime, datetime.now().isoformat())
    pipeline.hset(TaskKeys.identifier(task_identifier), TaskKeys.priority, str(priority))
    pipeline.hset(TaskKeys.identifier(task_identifier), TaskKeys.transmitterAssigned, "null")
    pipeline.hset(TaskKeys.identifier(task_identifier), TaskKeys.receiverAssigned, "null")
    pipeline.hset(TaskKeys.identifier(task_identifier), TaskKeys.transmitterProcessingStart, "null")
    pipeline.hset(TaskKeys.identifier(task_identifier), TaskKeys.receiverProcessingStart, "null")
    pipeline.hset(TaskKeys.identifier(task_identifier), TaskKeys.status, "queued")
    pipeline.hset(TaskKeys.identifier(task_identifier), TaskKeys.errorMessage, "null")
    pipeline.hset(TaskKeys.identifier(task_identifier), TaskKeys.errorTime, "null")
    pipeline.hset(TaskKeys.identifier(task_identifier), TaskKeys.localTimeRemaining, "60")

    # Add to the corresponding bucket queue the task identifier
    pipeline.lpush(TaskKeys.priority_queue(priority), task_identifier)
    pipeline.zadd(TaskKeys.priorities(), { str(priority): priority })

    result = pipeline.execute()
     
    return jsonify(success=True, taskIdentifier=task_identifier, status='queued', message="Loading successful")

@scheduler_blueprint.route('/user/get-task-time/<task_identifier>/<user_id>', methods=['GET', 'POST'])
def get_local_time(task_identifier, user_id):
#    authenticated = check_backend_credentials()
#    if not authenticated:
#        return jsonify(success=False, timeRemaining="0"), 401

    t = TaskKeys.identifier(task_identifier)
    author = redis_store.hget(t, TaskKeys.author)    
    if author == user_id:
        return jsonify(success=True, timeRemaining=redis_store.hget(t, TaskKeys.localTimeRemaining))
    else:
        return jsonify(success=False, timeRemaining="0")

@scheduler_blueprint.route('/user/set-task-time/<task_identifier>/<user_id>/<time_remaining>', methods=['GET', 'POST'])
def update_local_time(task_identifier, user_id, time_remaining):
#    authenticated = check_backend_credentials()
#    if not authenticated:
#        return jsonify(success=False), 401

    t = TaskKeys.identifier(task_identifier)
    author = redis_store.hget(t, TaskKeys.author)    
    if author == user_id:
        redis_store.hset(t, TaskKeys.localTimeRemaining, time_remaining)
        return jsonify(success=True)
    else:
        return jsonify(success=False)

@scheduler_blueprint.route('/user/tasks/<task_identifier>/<user_id>', methods=['POST'])
def delete_task(task_identifier, user_id):
#    authenticated = check_backend_credentials()
#    if not authenticated:
#        return jsonify(success=False, message="Invalid secret"), 401

    request_data = request.get_json(silent=True, force=True)
    if request_data.get('action') == "delete":
        t = TaskKeys.identifier(task_identifier)
        priority = redis_store.hget(t, TaskKeys.priority)
        if priority == None:
            pipeline = redis_store.pipeline()
            pipeline.sadd(ErrorKeys.errors(), task_identifier)
            pipeline.hset(ErrorKeys.identifier(task_identifier), ErrorKeys.uniqueIdentifier, task_identifier)
            pipeline.hset(ErrorKeys.identifier(task_identifier), ErrorKeys.author, user_id)
            pipeline.hset(ErrorKeys.identifier(task_identifier), ErrorKeys.errorMessage, "Task identifier does not exist")
            pipeline.hset(ErrorKeys.identifier(task_identifier), ErrorKeys.errorTime, datetime.now().isoformat())
            results = pipeline.execute()
            return jsonify(success=False, message="Invalid task identifier")
        if redis_store.hget(t, TaskKeys.author) != user_id:
            pipeline = redis_store.pipeline()
            pipeline.sadd(ErrorKeys.errors(), task_identifier)
            pipeline.hset(ErrorKeys.identifier(task_identifier), ErrorKeys.uniqueIdentifier, task_identifier)
            pipeline.hset(ErrorKeys.identifier(task_identifier), ErrorKeys.author, user_id)
            pipeline.hset(ErrorKeys.identifier(task_identifier), ErrorKeys.errorMessage, "You do not have permission to delete the task")
            pipeline.hset(ErrorKeys.identifier(task_identifier), ErrorKeys.errorTime, datetime.now().isoformat())
            results = pipeline.execute()
            return jsonify(success=False, message="Task not authored by user")
  
        redis_store.hset(t, TaskKeys.status, "deleted")
        if redis_store.hget(t, TaskKeys.status) == "queued":
            task_identifier = redis_store.rpop(TaskKeys.priority_queue(redis_store.hget(t, TaskKeys.priority)))
        if redis_store.hget(t, TaskKeys.receiverAssigned) != "null":
            device_base = redis_store.hget(t, TaskKeys.receiverAssigned).split(':')[0]
            redis_store.set(DeviceKeys.device_assignment(device_base), "null")
        redis_store.lrem(TaskKeys.priority_queue(int(priority)), 1, task_identifier)      
        redis_store.srem(TaskKeys.tasks(), task_identifier)
  
    return jsonify(success=True, message="Successfully deleted")

@scheduler_blueprint.route('/user/complete-tasks/<task_identifier>/<user_id>', methods=['GET', 'POST'])
def complete_user_task(task_identifier, user_id):
    t = TaskKeys.identifier(task_identifier)
    author = redis_store.hget(t, TaskKeys.author)
    if author == None:
        pipeline = redis_store.pipeline()
        pipeline.sadd(ErrorKeys.errors(), task_identifier)
        pipeline.hset(ErrorKeys.identifier(task_identifier), ErrorKeys.uniqueIdentifier, task_identifier)
        pipeline.hset(ErrorKeys.identifier(task_identifier), ErrorKeys.author, user_id)
        pipeline.hset(ErrorKeys.identifier(task_identifier), ErrorKeys.errorMessage, "Task identifier does not exist")
        pipeline.hset(ErrorKeys.identifier(task_identifier), ErrorKeys.errorTime, datetime.now().isoformat())
        results = pipeline.execute()
        return jsonify(success=False, status=None, message="Task identifier does not exist")
    if author != user_id:
        pipeline = redis_store.pipeline()
        pipeline.sadd(ErrorKeys.errors(), task_identifier)
        pipeline.hset(ErrorKeys.identifier(task_identifier), ErrorKeys.uniqueIdentifier, task_identifier)
        pipeline.hset(ErrorKeys.identifier(task_identifier), ErrorKeys.author, user_id)
        pipeline.hset(ErrorKeys.identifier(task_identifier), ErrorKeys.errorMessage, "You do not have permission to access the task")
        pipeline.hset(ErrorKeys.identifier(task_identifier), ErrorKeys.errorTime, datetime.now().isoformat())
        results = pipeline.execute()
        return jsonify(success=False, status=None, message="You do not have permission to access the task")  
    if redis_store.hget(t, TaskKeys.status) == "queued":  
        pipeline = redis_store.pipeline()
        pipeline.sadd(ErrorKeys.errors(), task_identifier)
        pipeline.hset(ErrorKeys.identifier(task_identifier), ErrorKeys.uniqueIdentifier, task_identifier)
        pipeline.hset(ErrorKeys.identifier(task_identifier), ErrorKeys.author, user_id)
        pipeline.hset(ErrorKeys.identifier(task_identifier), ErrorKeys.errorMessage, "You are accessing an invalid page")
        pipeline.hset(ErrorKeys.identifier(task_identifier), ErrorKeys.errorTime, datetime.now().isoformat())
        results = pipeline.execute()
        return jsonify(success=False, status=None, message="You are accessing an invalid page") 

    redis_store.hset(t, TaskKeys.status, "completed")
    if redis_store.hget(t, TaskKeys.receiverAssigned) != "null":
        device_base = redis_store.hget(t, TaskKeys.receiverAssigned).split(':')[0]
        redis_store.set(DeviceKeys.device_assignment(device_base), "null")
    return jsonify(success=True, status="completed", message="Completed")

@scheduler_blueprint.route('/devices/tasks/<type>/<task_identifier>', methods=['POST'])
def complete_device_task(type, task_identifier):
    device = check_device_credentials()
    if device is None:
        return jsonify(success=False, status=None, message="Invalid device credentials"), 401
    
    device_base = device.split(':')[0]
    t = TaskKeys.identifier(task_identifier)
    status_msg = "Error"
    if type == "receiver":
        if redis_store.hget(t, TaskKeys.status) == "receiver assigned" or redis_store.hget(t, TaskKeys.status) == "receiver still processing":
            status_msg = "completed"
        elif redis_store.hget(t, TaskKeys.status) == "fully assigned":
            status_msg = "transmitter still processing"
        pipeline = redis_store.pipeline()
        if status_msg != "Error":
            pipeline.hset(t, TaskKeys.status, status_msg)
        pipeline.set(DeviceKeys.device_assignment(device_base), "null")
        results = pipeline.execute()
    if type == "transmitter":
        if redis_store.hget(t, TaskKeys.status) == "fully assigned":
            redis_store.hset(t, TaskKeys.status, "receiver still processing")
            status_msg = "receiver still processing"
        elif redis_store.hget(t, TaskKeys.status) == "transmitter still processing":
            redis_store.hset(t, TaskKeys.status, "completed")
            status_msg = "completed"
    return jsonify(success=True, status=status_msg, message="Completed")

@scheduler_blueprint.route('/devices/task-status/<task_identifier>', methods=['GET', 'POST'])
def get_task_status(task_identifier):
    t = TaskKeys.identifier(task_identifier)
    author = redis_store.hget(t, TaskKeys.author)
    if author == None:
        pipeline = redis_store.pipeline()
        pipeline.sadd(ErrorKeys.errors(), task_identifier)
        pipeline.hset(ErrorKeys.identifier(task_identifier), ErrorKeys.uniqueIdentifier, task_identifier)
        pipeline.hset(ErrorKeys.identifier(task_identifier), ErrorKeys.author, user_id)
        pipeline.hset(ErrorKeys.identifier(task_identifier), ErrorKeys.errorMessage, "Task identifier does not exist")
        pipeline.hset(ErrorKeys.identifier(task_identifier), ErrorKeys.errorTime, datetime.now().isoformat())
        results = pipeline.execute()
        return jsonify(success=False, status=None, receiver=None, transmitter=None, session_id=None, message="Task identifier does not exist")   
    x = is_task_active(task_identifier)
    return jsonify(success=True, status=redis_store.hget(t, TaskKeys.status), receiver=redis_store.hget(t, TaskKeys.receiverAssigned), transmitter=redis_store.hget(t, TaskKeys.transmitterAssigned), session_id=redis_store.hget(t, TaskKeys.sessionId), message="Success")

@scheduler_blueprint.route('/devices/tasks/poll/<task_id>', methods=['GET', 'POST'])
def is_task_active(task_id):
    current_app.logger.info(redis_store.get(f"{TaskKeys.base_key()}:relia:data:tasks:{task_id}:user-active"))
    if redis_store.get(f"{TaskKeys.base_key()}:relia:data:tasks:{task_id}:user-active") not in ("1", b"1"):
        complete_device_task("receiver", task_id)
        complete_device_task("transmitter", task_id)
        return False
    return True

@scheduler_blueprint.route('/devices/tasks/receiver')
def assign_task_primary():
    device = check_device_credentials()
    if device is None:
        return jsonify(success=False, grcFile=None, grcFileContent=None, taskIdentifier=None, sessionIdentifier=None, message="Invalid device credentials"), 401

    device_base = device.split(':')[0]
    max_time_running = current_app.config['MAX_TIME_RUNNING']
    task_identifier = redis_store.get(DeviceKeys.device_assignment(device_base))
    if task_identifier and task_identifier != "null":
        user_id = redis_store.hget(TaskKeys.identifier(task_identifier), TaskKeys.author)
        if (datetime.now() - datetime.fromisoformat(redis_store.hget(TaskKeys.identifier(task_identifier), TaskKeys.receiverProcessingStart))).total_seconds() < max_time_running:
            return jsonify(success=False, grcFile=None, grcFileContent=None, taskIdentifier=None, sessionIdentifier=None, message="Device in use")
        else:
            pipeline = redis_store.pipeline()
            pipeline.hset(TaskKeys.identifier(task_identifier), TaskKeys.status, "completed")
            pipeline.set(DeviceKeys.device_assignment(device_base), "null")
            pipeline.sadd(ErrorKeys.errors(), task_identifier)
            pipeline.hset(ErrorKeys.identifier(task_identifier), ErrorKeys.uniqueIdentifier, task_identifier)
            pipeline.hset(ErrorKeys.identifier(task_identifier), ErrorKeys.author, user_id)
            pipeline.hset(ErrorKeys.identifier(task_identifier), ErrorKeys.errorMessage, "Receiver side: task timed out")
            pipeline.hset(ErrorKeys.identifier(task_identifier), ErrorKeys.errorTime, datetime.now().isoformat())
            results = pipeline.execute()
    try:
        max_seconds_waiting = int(request.args.get('max_seconds') or '25')
    except:
        max_seconds_waiting = 25

    max_seconds_waiting = max(min(max_seconds_waiting, 25), 1)

    task_identifier = None
    maximum_time = time.time() + max_seconds_waiting

    # Enter in a loop for 25 seconds trying to get a task. If there is a task,
    # return it immediately. If there is no task, keep trying until there is
    # a task or 25 seconds have elapsed
    while task_identifier is None and time.time() <= maximum_time:

        # Check the active priority queues, one by one, in priority, and if we
        # find a task, then assign it
        for priority in redis_store.zrange(TaskKeys.priorities(), 0, -1):
            task_identifier = redis_store.rpop(TaskKeys.priority_queue(priority))
            if not is_task_active(task_identifier):
                task_identifier = None
            if task_identifier is not None:
                break

        if task_identifier is None:
            time.sleep(0.1)

    if task_identifier is None:
        return jsonify(success=False, grcFile=None, grcFileContent=None, taskIdentifier=None, sessionIdentifier=None, message="No tasks in queue")
        
    # at this point, there is a task, which was the next task taking into account
    # priority and FIFO.
    pipeline = redis_store.pipeline()
    t = TaskKeys.identifier(task_identifier)
    pipeline.hget(t, TaskKeys.receiverFilename)
    pipeline.hget(t, TaskKeys.receiverFile)
    pipeline.hget(t, TaskKeys.sessionId)
    pipeline.hset(t, TaskKeys.receiverAssigned, device)
    pipeline.hset(t, TaskKeys.status, "receiver assigned")
    pipeline.set(DeviceKeys.device_assignment(device_base), task_identifier)
    results = pipeline.execute()

    redis_store.hset(t, TaskKeys.receiverProcessingStart, datetime.now().isoformat())
    session_key = f'relia:data-uploader:sessions:{redis_store.hget(t, TaskKeys.sessionId)}:devices'
    redis_store.sadd(session_key, device)
    return jsonify(success=True, grcFile=results[0], grcFileContent=results[1], sessionIdentifier=results[2], taskIdentifier=task_identifier, maxTime=max_time_running, message="Successfully assigned")

@scheduler_blueprint.route('/devices/tasks/transmitter')
def assign_task_secondary():
    device = check_device_credentials()
    if device is None:
        return jsonify(success=False, grcFile=None, grcFileContent=None, taskIdentifier=None, sessionIdentifier=None, message="Invalid device credentials"), 401

    device_base = device.split(':')[0]
    max_time_running = current_app.config['MAX_TIME_RUNNING']
    try:
        max_seconds_waiting = int(request.args.get('max_seconds') or '25')
    except:
        max_seconds_waiting = 25

    max_seconds_waiting = max(min(max_seconds_waiting, 25), 1)

    task_identifier = None
    maximum_time = time.time() + max_seconds_waiting

    # Enter in a loop for 25 seconds trying to get a task. If there is a task,

    # return it immediately. If there is no task, keep trying until there is
    # a task or 25 seconds have elapsed
    while task_identifier is None and time.time() <= maximum_time:

        task_identifier = redis_store.get(DeviceKeys.device_assignment(device_base))
        if task_identifier is not None:
            t = TaskKeys.identifier(task_identifier)
            if redis_store.hget(t, TaskKeys.status) != "receiver assigned":
                task_identifier = None
            if not is_task_active(task_identifier):
                task_identifier = None

        if task_identifier is None:
            time.sleep(0.1)

    if task_identifier is None:
        return jsonify(success=False, grcFile=None, grcFileContent=None, taskIdentifier=None, sessionIdentifier=None, message="No tasks in queue")  

    # Store in Redis that the transmitter has been assigned and return the task information to the user
    pipeline = redis_store.pipeline()
    t = TaskKeys.identifier(task_identifier)
    pipeline.hget(t, TaskKeys.transmitterFilename)
    pipeline.hget(t, TaskKeys.transmitterFile)
    pipeline.hget(t, TaskKeys.sessionId)
    pipeline.hset(t, TaskKeys.transmitterAssigned, device)
    pipeline.hset(t, TaskKeys.status, "fully assigned")
    results = pipeline.execute()

    redis_store.hset(t, TaskKeys.transmitterProcessingStart, datetime.now().isoformat())
    session_key = f'relia:data-uploader:sessions:{redis_store.hget(t, TaskKeys.sessionId)}:devices'
    redis_store.sadd(session_key, device)
    return jsonify(success=True, grcFile=results[0], grcFileContent=results[1], sessionIdentifier=results[2], taskIdentifier=task_identifier, maxTime=max_time_running, message="Successfully assigned")

@scheduler_blueprint.route('/devices/tasks/error_message/<task_identifier>', methods=['POST'])
def assign_error_message(task_identifier):
    device = check_device_credentials()
    if device is None:
        return jsonify(success=False, message="Invalid device credentials"), 401

    request_data = request.get_json(silent=True, force=True)

    t = TaskKeys.identifier(task_identifier)
    redis_store.hset(t, TaskKeys.errorMessage, request_data.get('errorMessage'))
    redis_store.hset(t, TaskKeys.errorTime, request_data.get('errorTime'))
    return jsonify(success=True)

def _corsify_actual_response(response):
    response.headers['Access-Control-Allow-Origin'] = '*';
    response.headers['Access-Control-Allow-Credentials'] = 'true';
    response.headers['Access-Control-Allow-Methods'] = 'OPTIONS, GET, POST';
    response.headers['Access-Control-Allow-Headers'] = 'Content-Type, Depth, User-Agent, X-File-Size, X-Requested-With, If-Modified-Since, X-File-Name, Cache-Control';
    return response
