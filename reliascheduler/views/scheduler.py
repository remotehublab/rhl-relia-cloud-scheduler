import os
import glob
import time
import secrets
import logging
import datetime

from typing import Optional

from urllib.request import urlopen

import yaml

from werkzeug.utils import secure_filename
from flask import Blueprint, jsonify, current_app, request, make_response, send_file

from reliascheduler import redis_store
from reliascheduler.auth import check_backend_credentials, check_device_credentials
from reliascheduler.keys import TaskKeys, DeviceKeys

logger = logging.getLogger(__name__)

scheduler_blueprint = Blueprint('scheduler', __name__)

@scheduler_blueprint.route('/user/tasks/<task_identifier>', methods=['GET'])
def get_one_task(task_identifier):
    #authenticated = check_backend_credentials()
    #if not authenticated:
    #    return jsonify(success=False, message="Invalid secret"), 401

    t = TaskKeys.identifier(task_identifier)
    return jsonify(success=True, status=redis_store.hget(t, TaskKeys.status), receiver=redis_store.hget(t, TaskKeys.receiverAssigned), transmitter=redis_store.hget(t, TaskKeys.transmitterAssigned))

@scheduler_blueprint.route('/user/all-tasks', methods=['GET'])
def get_all_tasks():
    #authenticated = check_backend_credentials()
    #if not authenticated:
    #    return jsonify(success=False, message="Invalid secret"), 401

    task_id = []
    task_status = []
    task_receiver = []
    task_transmitter = []
    for t in redis_store.smembers(TaskKeys.tasks()):
        task_id.append(redis_store.hget(TaskKeys.identifier(t), TaskKeys.uniqueIdentifier))
        task_status.append(redis_store.hget(TaskKeys.identifier(t), TaskKeys.status))
        task_receiver.append(redis_store.hget(TaskKeys.identifier(t), TaskKeys.receiverAssigned))
        task_transmitter.append(redis_store.hget(TaskKeys.identifier(t), TaskKeys.transmitterAssigned))
    return jsonify(success=True, ids=task_id, statuses=task_status, receivers=task_receiver, transmitters=task_transmitter)

@scheduler_blueprint.route('/user/tasks', methods=['POST'])
def load_task():

    #authenticated = check_backend_credentials()
    #if not authenticated:
    #    return jsonify(success=False, message="Invalid secret"), 401

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
        return jsonify(success=False, message="No grc_files provided")

    for grc_file_type in ('receiver', 'transmitter'):
        grc_file_data = grc_files.get(grc_file_type)
        if not grc_file_data:
            return jsonify(success=False, message=f"No {grc_file_type} found in grc_files")

        filename = grc_file_data.get('filename')
        if not filename:
            return jsonify(success=False, message=f"No filename found in {grc_file_type} in grc_files")
        content = grc_file_data.get('content')
        if not content:
            return jsonify(success=False, message=f"No content found in {grc_file_type} in grc_files")

        try:
            yaml.safe_load(content)
        except Exception as err:
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
    pipeline.hset(TaskKeys.identifier(task_identifier), TaskKeys.transmitterFilename, transmitter_filename)
    pipeline.hset(TaskKeys.identifier(task_identifier), TaskKeys.transmitterFile, transmitter_file)
    pipeline.hset(TaskKeys.identifier(task_identifier), TaskKeys.receiverFilename, receiver_filename)
    pipeline.hset(TaskKeys.identifier(task_identifier), TaskKeys.receiverFile, receiver_file)
    pipeline.hset(TaskKeys.identifier(task_identifier), TaskKeys.sessionId, session_id)
    pipeline.hset(TaskKeys.identifier(task_identifier), TaskKeys.startedTime, datetime.datetime.now().isoformat())
    pipeline.hset(TaskKeys.identifier(task_identifier), TaskKeys.priority, str(priority))
    pipeline.hset(TaskKeys.identifier(task_identifier), TaskKeys.transmitterAssigned, "null")
    pipeline.hset(TaskKeys.identifier(task_identifier), TaskKeys.receiverAssigned, "null")
    pipeline.hset(TaskKeys.identifier(task_identifier), TaskKeys.status, "queued")

    # Add to the corresponding bucket queue the task identifier
    pipeline.lpush(TaskKeys.priority_queue(priority), task_identifier)
    pipeline.zadd(TaskKeys.priorities(), { str(priority): priority })

    result = pipeline.execute()
     
    return jsonify(success=True, taskIdentifier=task_identifier, status='queued')


@scheduler_blueprint.route('/user/tasks/<task_identifier>', methods=['POST'])
def delete_task(task_identifier):
    #authenticated = check_backend_credentials()
    #if not authenticated:
    #    return jsonify(success=False, message="Invalid secret"), 401

    request_data = request.get_json(silent=True, force=True)
    if request_data.get('action') == "delete":
        priority = redis_store.hget(TaskKeys.identifier(task_identifier), TaskKeys.priority)
        redis_store.lrem(TaskKeys.priority_queue(int(priority)), 1, task_identifier)
        redis_store.srem(TaskKeys.tasks(), task_identifier)
  
    return jsonify(success=True)

@scheduler_blueprint.route('/devices/tasks/complete', methods=['POST'])
def complete_device_task():
    device = check_device_credentials()
    if device is None:
        return jsonify(success=False, message="Invalid device credentials"), 401

    for t in redis_store.srange(TaskKeys.tasks(), 0, -1):
        if redis_store.hget(t, TaskKeys.transmitterAssigned) == device or redis_store.hget(t, TaskKeys.receiverAssigned) == device:
            if redis_store.hget(t, TaskKeys.status) == "assigned":
                 redis_store.hset(t, TaskKeys.status, "processing")
                 break
            elif redis_store.hget(t, TaskKeys.status) == "processing":
                 redis_store.hset(t, TaskKeys.status, "completed")
                 break
    return jsonify(success=True)

@scheduler_blueprint.route('/devices/tasks/receiver')
def assign_task_primary():
    device = check_device_credentials()
    if device is None:
        return jsonify(success=False, message="Invalid device credentials"), 401

    device_base = device.split(':')[0]
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
            if task_identifier is not None:
                break

        if task_identifier is None:
            time.sleep(0.1)

    if task_identifier is None:
        return jsonify(success=True, taskIdentifier=None, sessionIdentifier=None)

    # at this point, there is a task, which was the next task taking into account
    # priority and FIFO.
    pipeline = redis_store.pipeline()
    pipeline.hget(TaskKeys.identifier(task_identifier), TaskKeys.receiverFilename)
    pipeline.hget(TaskKeys.identifier(task_identifier), TaskKeys.receiverFile)
    pipeline.hget(TaskKeys.identifier(task_identifier), TaskKeys.sessionId)
    pipeline.hset(TaskKeys.identifier(task_identifier), TaskKeys.receiverAssigned, device)
    pipeline.hset(TaskKeys.identifier(task_identifier), TaskKeys.status, "assigned")
    pipeline.set(DeviceKeys.device_assignment(device_base), task_identifier)
    results = pipeline.execute()
    
    return jsonify(success=True, grcReceiverFile=results[0], grcReceiverFileContent=results[1], sessionIdentifier=results[2], taskIdentifier=task_identifier)

@scheduler_blueprint.route('/devices/tasks/transmitter')
def assign_task_secondary():
    device = check_device_credentials()
    if device is None:
        return jsonify(success=False, message="Invalid device credentials"), 401

    device_base = device.split(':')[0]

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

        if task_identifier is None:
            time.sleep(0.1)

    if task_identifier is None:
        return jsonify(success=True, taskIdentifier=None, sessionIdentifier=None)

    # Store in Redis that the transmitter has been assigned and return the task information to the user
    pipeline = redis_store.pipeline()
    pipeline.hget(TaskKeys.identifier(task_identifier), TaskKeys.transmitterFilename)
    pipeline.hget(TaskKeys.identifier(task_identifier), TaskKeys.transmitterFile)
    pipeline.hget(TaskKeys.identifier(task_identifier), TaskKeys.sessionId)
    pipeline.hset(TaskKeys.identifier(task_identifier), TaskKeys.transmitterAssigned, device)
    results = pipeline.execute()
    
    return jsonify(success=True, grcTransmitterFile=results[0], grcTransmitterFileContent=results[1], sessionIdentifier=results[2], taskIdentifier=task_identifier)

def _corsify_actual_response(response):
    response.headers['Access-Control-Allow-Origin'] = '*';
    response.headers['Access-Control-Allow-Credentials'] = 'true';
    response.headers['Access-Control-Allow-Methods'] = 'OPTIONS, GET, POST';
    response.headers['Access-Control-Allow-Headers'] = 'Content-Type, Depth, User-Agent, X-File-Size, X-Requested-With, If-Modified-Since, X-File-Name, Cache-Control';
    return response
