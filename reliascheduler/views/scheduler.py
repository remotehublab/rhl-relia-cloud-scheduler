from collections import OrderedDict
import json
import os
import glob
import time
import secrets
import logging
from datetime import datetime
from typing import Dict

import yaml
import numpy as np

from flask import Blueprint, jsonify, current_app, request

from reliascheduler import redis_store
from reliascheduler.auth import check_backend_credentials, check_device_credentials
from reliascheduler.keys import ErrorKeys, TaskKeys, DeviceKeys

logger = logging.getLogger(__name__)

scheduler_blueprint = Blueprint('scheduler', __name__)

@scheduler_blueprint.route('/user/tasks/<task_identifier>', methods=['GET'])
def user_get_task(task_identifier):
    t = TaskKeys.identifier(task_identifier)
    author = redis_store.hget(t, TaskKeys.author)
    if author == None:
        pipeline = redis_store.pipeline()
        pipeline.sadd(ErrorKeys.errors(), task_identifier)
        pipeline.hset(ErrorKeys.identifier(task_identifier), ErrorKeys.uniqueIdentifier, task_identifier)
        pipeline.hset(ErrorKeys.identifier(task_identifier), ErrorKeys.author, "No author")
        pipeline.hset(ErrorKeys.identifier(task_identifier), ErrorKeys.errorMessage, "Task identifier does not exist")
        pipeline.hset(ErrorKeys.identifier(task_identifier), ErrorKeys.errorTime, datetime.now().isoformat())
        results = pipeline.execute()
        return jsonify(success=False, status=None, receiver=None, transmitter=None, session_id=None, message="Task identifier does not exist")
    
    mark_as_poll(task_identifier)

    pipeline = redis_store.pipeline()
    pipeline.hget(t, TaskKeys.status)
    pipeline.hget(t, TaskKeys.receiverAssigned)
    pipeline.hget(t, TaskKeys.transmitterAssigned)
    pipeline.hget(t, TaskKeys.deviceAssigned)
    pipeline.hget(t, TaskKeys.receiverFilename)
    pipeline.hget(t, TaskKeys.transmitterFilename)
    status, receiver, transmitter, device, receiver_filename, transmitter_filename  = pipeline.execute()

    metadata_filename = current_app.config['DEVICE_METADATA_FILENAME']

    if device and os.path.exists(metadata_filename):
        devices_metadata = yaml.safe_load(open(metadata_filename).read())
        camera_url = devices_metadata.get(device, {}).get('camera')
    else:
        camera_url = None

    return jsonify(
        success=True, 
        status=status, 
        assignedInstance=device,
        cameraUrl=camera_url,
        receiver=receiver, 
        transmitter=transmitter, 
        receiverFilename=receiver_filename,
        transmitterFilename=transmitter_filename,
        message="Success"
    )

@scheduler_blueprint.route('/user/error-messages/<user_id>', methods=['GET'])
def user_get_errors(user_id):
    authenticated = check_backend_credentials()
    if not authenticated:
        return jsonify(success=False, ids=None, errors=None), 401

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

def mark_as_poll(task_id):
    if redis_store.hget(TaskKeys.identifier(task_id), TaskKeys.uniqueIdentifier) is not None:
        pipeline = redis_store.pipeline()
        pipeline.hset(TaskKeys.identifier(task_id), TaskKeys.inactiveSince, str(time.time()))
        pipeline.execute()

@scheduler_blueprint.route('/user/tasks/', methods=['POST'])
def user_create_task():
    """
    This method is called by the backend, and the purpose is to create a new task.
    Typically, the user calls the backend every time they want to submit some code, and then
    the backend calls the scheduler using this method.
    """
    authenticated = check_backend_credentials()
    if not authenticated:
        return jsonify(success=False, taskIdentifier=None, status=None, message="Invalid secret"), 401

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

    task_identifier = secrets.token_urlsafe()
    while redis_store.sadd(TaskKeys.tasks(), task_identifier) == 0:
        task_identifier = secrets.token_urlsafe()

    grc_files = request_data.get('grc_files')
    user_id = request_data.get('user_id')
    if not grc_files:
        pipeline = redis_store.pipeline()
        pipeline.hset(ErrorKeys.identifier(task_identifier), ErrorKeys.uniqueIdentifier, task_identifier)
        pipeline.hset(ErrorKeys.identifier(task_identifier), ErrorKeys.author, user_id)
        pipeline.hset(ErrorKeys.identifier(task_identifier), ErrorKeys.errorMessage, "No grc_files provided")
        pipeline.hset(ErrorKeys.identifier(task_identifier), ErrorKeys.errorTime, datetime.now().isoformat())
        result = pipeline.execute()
        return jsonify(success=False, taskIdentifier=None, status=None, message="No grc_files provided")

    for grc_file_type in ('receiver', 'transmitter'):
        grc_file_data = grc_files.get(grc_file_type)
        if not grc_file_data:
            pipeline = redis_store.pipeline()
            pipeline.hset(ErrorKeys.identifier(task_identifier), ErrorKeys.uniqueIdentifier, task_identifier)
            pipeline.hset(ErrorKeys.identifier(task_identifier), ErrorKeys.author, user_id)
            pipeline.hset(ErrorKeys.identifier(task_identifier), ErrorKeys.errorMessage, f"No {grc_file_type} found in grc_files")
            pipeline.hset(ErrorKeys.identifier(task_identifier), ErrorKeys.errorTime, datetime.now().isoformat())
            result = pipeline.execute()
            return jsonify(success=False, taskIdentifier=None, status=None, message=f"No {grc_file_type} found in grc_files")

        filename = grc_file_data.get('filename')
        if not filename:
            pipeline = redis_store.pipeline()
            pipeline.sadd(ErrorKeys.errors(), task_identifier)
            pipeline.hset(ErrorKeys.identifier(task_identifier), ErrorKeys.uniqueIdentifier, task_identifier)
            pipeline.hset(ErrorKeys.identifier(task_identifier), ErrorKeys.author, user_id)
            pipeline.hset(ErrorKeys.identifier(task_identifier), ErrorKeys.errorMessage, f"No filename found in {grc_file_type} in grc_files")
            pipeline.hset(ErrorKeys.identifier(task_identifier), ErrorKeys.errorTime, datetime.now().isoformat())
            result = pipeline.execute()
            return jsonify(success=False, taskIdentifier=None, status=None, message=f"No filename found in {grc_file_type} in grc_files")
        content = grc_file_data.get('content')
        if not content:
            pipeline = redis_store.pipeline()
            pipeline.hset(ErrorKeys.identifier(task_identifier), ErrorKeys.uniqueIdentifier, task_identifier)
            pipeline.hset(ErrorKeys.identifier(task_identifier), ErrorKeys.author, user_id)
            pipeline.hset(ErrorKeys.identifier(task_identifier), ErrorKeys.errorMessage, f"No content found in {grc_file_type} in grc_files")
            pipeline.hset(ErrorKeys.identifier(task_identifier), ErrorKeys.errorTime, datetime.now().isoformat())
            result = pipeline.execute()
            return jsonify(success=False, taskIdentifier=None, status=None, message=f"No content found in {grc_file_type} in grc_files")

        file_type = grc_file_data.get('type')
        if file_type == 'grc':
            try:
                yaml.safe_load(content)
            except Exception as err:
                pipeline = redis_store.pipeline()
                pipeline.hset(ErrorKeys.identifier(task_identifier), ErrorKeys.uniqueIdentifier, task_identifier)
                pipeline.hset(ErrorKeys.identifier(task_identifier), ErrorKeys.author, user_id)
                pipeline.hset(ErrorKeys.identifier(task_identifier), ErrorKeys.errorMessage, f"Invalid content (not yaml) for provided {grc_file_type}")
                pipeline.hset(ErrorKeys.identifier(task_identifier), ErrorKeys.errorTime, datetime.now().isoformat())
                result = pipeline.execute()
                return jsonify(success=False, taskIdentifier=None, status=None, message=f"Invalid content (not yaml) for provided {grc_file_type}")
            # in the future we might check more things about the .grc files

    # We have checked the data, so we can now do:
    transmitter_filename = grc_files['transmitter']['filename']
    transmitter_file = grc_files['transmitter']['content']
    transmitter_file_type = grc_files['transmitter']['type']
    receiver_filename = grc_files['receiver']['filename']
    receiver_file = grc_files['receiver']['content']
    receiver_file_type = grc_files['receiver']['type']

    # Load into Redis
    # We rely on a set to know which keys have been added and are unique.
    # The key means that there has been an attempt to create the key, it does not
    # mean that the key is currently active (and might need to be cleaned)

    t = TaskKeys.identifier(task_identifier)
    pipeline = redis_store.pipeline()
    pipeline.hset(t, TaskKeys.uniqueIdentifier, task_identifier)
    pipeline.hset(t, TaskKeys.author, user_id)
    pipeline.hset(t, TaskKeys.transmitterFilename, transmitter_filename)
    pipeline.hset(t, TaskKeys.transmitterFile, transmitter_file)
    pipeline.hset(t, TaskKeys.receiverFilename, receiver_filename)
    pipeline.hset(t, TaskKeys.receiverFile, receiver_file)
    pipeline.hset(t, TaskKeys.sessionId, session_id)
    pipeline.hset(t, TaskKeys.startedTime, datetime.now().isoformat())
    pipeline.hset(t, TaskKeys.priority, str(priority))
    pipeline.hset(t, TaskKeys.transmitterAssigned, "null")
    pipeline.hset(t, TaskKeys.receiverAssigned, "null")
    pipeline.hset(t, TaskKeys.transmitterProcessingStart, "null")
    pipeline.hset(t, TaskKeys.receiverProcessingStart, "null")
    pipeline.hset(t, TaskKeys.status, TaskKeys.Status.queued)
    pipeline.hset(t, TaskKeys.errorMessage, "null")
    pipeline.hset(t, TaskKeys.errorTime, "null")
    pipeline.hset(t, TaskKeys.localTimeRemaining, "0")
    pipeline.hset(t, TaskKeys.inactiveSince, str(time.time()))
    pipeline.hset(t, TaskKeys.transmitterFiletype, transmitter_file_type)
    pipeline.hset(t, TaskKeys.receiverFiletype, receiver_file_type)

    # Add to the corresponding bucket queue the task identifier
    pipeline.lpush(TaskKeys.priority_queue(priority), task_identifier)
    pipeline.zadd(TaskKeys.priorities(), { str(priority): priority })

    result = pipeline.execute()

    logger.warning(f"Task {task_identifier} queued with priority {priority} created for user {user_id}")
    logger.warning("Last time we saw each device:")
    for device_name, device_data in _available_devices_last_check().items():
        logger.warning(f" - {device_name}: {device_data}.")
        
    logger.warning("-")
     
    return jsonify(success=True, taskIdentifier=task_identifier, status='queued', message="Loading successful")

@scheduler_blueprint.route('/user/tasks/<task_identifier>', methods=['POST'])
def user_delete_task(task_identifier):
    request_data = request.get_json(silent=True, force=True)
    if request_data.get('action') == "delete":
        t = TaskKeys.identifier(task_identifier)
        priority = redis_store.hget(t, TaskKeys.priority)
        if priority == None:
            pipeline = redis_store.pipeline()
            pipeline.sadd(ErrorKeys.errors(), task_identifier)
            pipeline.hset(ErrorKeys.identifier(task_identifier), ErrorKeys.uniqueIdentifier, task_identifier)
            pipeline.hset(ErrorKeys.identifier(task_identifier), ErrorKeys.author, "unknown")
            pipeline.hset(ErrorKeys.identifier(task_identifier), ErrorKeys.errorMessage, "Task identifier does not exist")
            pipeline.hset(ErrorKeys.identifier(task_identifier), ErrorKeys.errorTime, datetime.now().isoformat())
            results = pipeline.execute()
            return jsonify(success=False, message="Invalid task identifier")

        redis_store.hset(t, TaskKeys.status, TaskKeys.Status.deleted)
        if redis_store.hget(t, TaskKeys.status) == TaskKeys.Status.queued:
            task_identifier = redis_store.rpop(TaskKeys.priority_queue(redis_store.hget(t, TaskKeys.priority)))
        if redis_store.hget(t, TaskKeys.receiverAssigned) != "null":
            device_base = redis_store.hget(t, TaskKeys.receiverAssigned).split(':')[0]
            redis_store.set(DeviceKeys.device_assignment(device_base), "null")
        redis_store.lrem(TaskKeys.priority_queue(int(priority)), 1, task_identifier)      
        redis_store.srem(TaskKeys.tasks(), task_identifier)
  
    return jsonify(success=True, message="Successfully deleted")


@scheduler_blueprint.route('/devices/tasks/<type>/<task_identifier>', methods=['POST'])
def devices_complete_task(type, task_identifier):
    device = check_device_credentials()
    if device is None:
        return jsonify(success=False, status=None, message="Invalid device credentials"), 401

    device_base = device.split(':')[0]
    response = _complete_device_task_impl(device_base, type, task_identifier)
    return jsonify(**response)

@scheduler_blueprint.route('/devices/tasks/<type>/<task_identifier>', methods=['GET'])
def devices_get_task_status(type, task_identifier):
    """
    Get task (device perspective)
    """
    device = check_device_credentials()
    if device is None:
        return jsonify(success=False, status=None, receiver=None, transmitter=None, session_id=None, message="Invalid device credentials"), 401

    device_base: str = device.split(':')[0]

    t = TaskKeys.identifier(task_identifier)
    author = redis_store.hget(t, TaskKeys.author)
    if author is None:
        pipeline = redis_store.pipeline()
        pipeline.sadd(ErrorKeys.errors(), task_identifier)
        pipeline.hset(ErrorKeys.identifier(task_identifier), ErrorKeys.uniqueIdentifier, task_identifier)
        pipeline.hset(ErrorKeys.identifier(task_identifier), ErrorKeys.author, "None")
        pipeline.hset(ErrorKeys.identifier(task_identifier), ErrorKeys.errorMessage, "Task identifier does not exist")
        pipeline.hset(ErrorKeys.identifier(task_identifier), ErrorKeys.errorTime, datetime.now().isoformat())
        results = pipeline.execute()
        return jsonify(success=False, status=None, receiver=None, transmitter=None, session_id=None, message="Task identifier does not exist")

    _stop_task_if_inactive(device_base, task_identifier)
    return jsonify(
        success=True, 
        status=redis_store.hget(t, TaskKeys.status), 
        receiver=redis_store.hget(t, TaskKeys.receiverAssigned), 
        transmitter=redis_store.hget(t, TaskKeys.transmitterAssigned), 
        session_id=redis_store.hget(t, TaskKeys.sessionId), 
        message="Success"
    )


def _complete_device_task_impl(device_base: str, type: str, task_identifier: str) -> dict:
    t = TaskKeys.identifier(task_identifier)
    status_msg = TaskKeys.Status.error
    if type == "receiver":
        if redis_store.hget(t, TaskKeys.status) == TaskKeys.Status.receiver_assigned or redis_store.hget(t, TaskKeys.status) == TaskKeys.Status.receiver_still_processing:
            status_msg = TaskKeys.Status.completed
        elif redis_store.hget(t, TaskKeys.status) == TaskKeys.Status.fully_assigned:
            status_msg = TaskKeys.Status.transmitter_still_processing
        pipeline = redis_store.pipeline()
        if status_msg != TaskKeys.Status.error:
            pipeline.hset(t, TaskKeys.status, status_msg)
        pipeline.set(DeviceKeys.device_assignment(device_base), "null")
        results = pipeline.execute()
    if type == "transmitter":
        if redis_store.hget(t, TaskKeys.status) == TaskKeys.Status.fully_assigned:
            redis_store.hset(t, TaskKeys.status, TaskKeys.Status.receiver_still_processing)
            status_msg = TaskKeys.Status.receiver_still_processing
        elif redis_store.hget(t, TaskKeys.status) == TaskKeys.Status.transmitter_still_processing:
            redis_store.hset(t, TaskKeys.status, TaskKeys.Status.completed)
            status_msg = TaskKeys.Status.completed
    return {
        'success': True, 
        'status': status_msg, 
        'message': "Completed",
    }

def _stop_task_if_inactive(device_base: str, task_id: str) -> bool:
    """
    Check if the user has not polled in a while, and then remove the task from the receiver and transmitter if that's the case.

    We return if the task is stopped.
    """
    inactive_since = redis_store.hget(TaskKeys.identifier(task_id), TaskKeys.inactiveSince)
    if inactive_since is not None: # i.e., the task still exists
        current_app.logger.info(inactive_since)
        inactive_since_timestamp = float(inactive_since)
        elapsed = time.time() - inactive_since_timestamp
        # in 10 seconds without any poll from the student, we delete the session to allow someone else to use the lab
        if elapsed > current_app.config['MAX_TIME_WITHOUT_POLLING']:
            _complete_device_task_impl(device_base, "receiver", task_id)
            _complete_device_task_impl(device_base, "transmitter", task_id)
            return True
    return False

def _available_devices_last_check() -> Dict[str, Dict[str, str]]:
    """
    Return the device names and the last time they were seen
    """
    now = datetime.utcnow()
    device_keys = sorted(list(redis_store.keys(DeviceKeys("*").last_check())))
    device_names = [ ':'.join(device_key.split(':')[-3:-1]) for device_key in device_keys ]
    pipeline = redis_store.pipeline()
    for device in device_keys:
        pipeline.get(device)
    
    device_data = OrderedDict()
    for device_name, last_check in zip(device_names, pipeline.execute()):
        if last_check:
            last_check_human = f"{(now - datetime.fromisoformat(last_check)).total_seconds()} seconds ago"
        else:
            last_check_human = "Unknown number of seconds ago"

        device_data[device_name] = {
            "last_check": last_check_human,
            "assignment": redis_store.get(DeviceKeys.device_assignment(device_name.split(':')[0])),
        }
    return device_data

@scheduler_blueprint.route('/devices/available')
def available_devices():    
    device_data = _available_devices_last_check()
    return json.dumps(dict(success=True, device_data=device_data), indent=4), 200, {"Content-Type": "application/json"}


@scheduler_blueprint.route('/devices/tasks/receiver')
def devices_assign_task_primary():
    """
    Assign a task to the receiver. The receiver is the primary device: only once the receiver has received a taks, the transmitter
    gets the task.
    """
    device = check_device_credentials()
    if device is None:
        return jsonify(success=False, file=None, fileContent=None, taskIdentifier=None, sessionIdentifier=None, message="Invalid device credentials"), 401
    
    redis_store.setex(DeviceKeys(device).last_check(), 24 * 3600, datetime.utcnow().isoformat())

    device_base = device.split(':')[0]
    max_time_running = current_app.config['MAX_TIME_RUNNING']
    task_identifier = redis_store.get(DeviceKeys.device_assignment(device_base))
    if task_identifier and task_identifier != "null":
        user_id = redis_store.hget(TaskKeys.identifier(task_identifier), TaskKeys.author)
        receiver_processing_start = redis_store.hget(TaskKeys.identifier(task_identifier), TaskKeys.receiverProcessingStart)
        time_elapsed_since_procsesing_start = (datetime.now() - datetime.fromisoformat(receiver_processing_start)).total_seconds()
        if time_elapsed_since_procsesing_start < max_time_running:
            return jsonify(success=False, file=None, fileContent=None, taskIdentifier=None, sessionIdentifier=None, message="Device in use")
        else:
            pipeline = redis_store.pipeline()
            pipeline.hset(TaskKeys.identifier(task_identifier), TaskKeys.status, TaskKeys.Status.completed)
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
            if _stop_task_if_inactive(device_base, task_identifier):
                task_identifier = None
            if task_identifier is not None:
                break

        if task_identifier is None:
            time.sleep(0.1)

    if task_identifier is None:
        return jsonify(success=True, file=None, fileContent=None, taskIdentifier=None, sessionIdentifier=None, message="No tasks in queue")
        
    # at this point, there is a task, which was the next task taking into account
    # priority and FIFO.
    pipeline = redis_store.pipeline()
    t = TaskKeys.identifier(task_identifier)
    pipeline.hget(t, TaskKeys.receiverFilename)
    pipeline.hget(t, TaskKeys.receiverFile)
    pipeline.hget(t, TaskKeys.sessionId)
    pipeline.hget(t, TaskKeys.receiverFiletype)
    pipeline.hset(t, TaskKeys.receiverAssigned, device)
    pipeline.hset(t, TaskKeys.deviceAssigned, device_base)
    pipeline.hset(t, TaskKeys.status, TaskKeys.Status.receiver_assigned)
    pipeline.set(DeviceKeys.device_assignment(device_base), task_identifier)
    results = pipeline.execute()

    redis_store.hset(t, TaskKeys.receiverProcessingStart, datetime.now().isoformat())
    session_key = f'relia:data-uploader:sessions:{redis_store.hget(t, TaskKeys.sessionId)}:devices'
    redis_store.sadd(session_key, device)

    logger.warning(f"Task {task_identifier} assigned to setup {device_base}")

    return jsonify(success=True, file=results[0], fileContent=results[1], sessionIdentifier=results[2], filetype=results[3], taskIdentifier=task_identifier, maxTime=max_time_running, message="Successfully assigned")

@scheduler_blueprint.route('/devices/tasks/transmitter')
def devices_assign_task_secondary():
    """
    Assign a task to the transmitter. The transmitter is the secondary device: it waits until the receiver
    is assigned a task to be assigned a task.
    """
    device = check_device_credentials()
    if device is None:
        return jsonify(success=False, file=None, fileContent=None, taskIdentifier=None, sessionIdentifier=None, message="Invalid device credentials"), 401

    redis_store.setex(DeviceKeys(device).last_check(), 24 * 3600, datetime.utcnow().isoformat())

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
            if redis_store.hget(t, TaskKeys.status) != TaskKeys.Status.receiver_assigned:
                task_identifier = None
            if task_identifier is not None:
                if _stop_task_if_inactive(device_base, task_identifier):
                    task_identifier = None

        if task_identifier is None:
            time.sleep(0.1)

    if task_identifier is None:
        return jsonify(success=True, file=None, fileContent=None, taskIdentifier=None, sessionIdentifier=None, message="No tasks in queue")  

    # Store in Redis that the transmitter has been assigned and return the task information to the user
    pipeline = redis_store.pipeline()
    t = TaskKeys.identifier(task_identifier)
    pipeline.hget(t, TaskKeys.transmitterFilename)
    pipeline.hget(t, TaskKeys.transmitterFile)
    pipeline.hget(t, TaskKeys.sessionId)
    pipeline.hget(t, TaskKeys.transmitterFiletype)
    pipeline.hset(t, TaskKeys.transmitterAssigned, device)
    pipeline.hset(t, TaskKeys.status, TaskKeys.Status.fully_assigned)
    results = pipeline.execute()

    redis_store.hset(t, TaskKeys.transmitterProcessingStart, datetime.now().isoformat())
    session_key = f'relia:data-uploader:sessions:{redis_store.hget(t, TaskKeys.sessionId)}:devices'
    redis_store.sadd(session_key, device)
    return jsonify(success=True, file=results[0], fileContent=results[1], sessionIdentifier=results[2], filetype=results[3], taskIdentifier=task_identifier, maxTime=max_time_running, message="Successfully assigned")

@scheduler_blueprint.route('/devices/tasks/error_message/<task_identifier>', methods=['POST'])
def devices_assign_error_message(task_identifier):
    device = check_device_credentials()
    if device is None:
        return jsonify(success=False, message="Invalid device credentials"), 401

    request_data = request.get_json(silent=True, force=True)

    t = TaskKeys.identifier(task_identifier)
    redis_store.hset(t, TaskKeys.errorMessage, request_data.get('errorMessage'))
    redis_store.hset(t, TaskKeys.errorTime, request_data.get('errorTime'))
    return jsonify(success=True, message="Success")
