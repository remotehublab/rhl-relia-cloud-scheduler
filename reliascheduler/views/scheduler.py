import os
import glob
import logging
import redis
import uuid
from datetime import datetime

from werkzeug.utils import secure_filename
from flask import Blueprint, jsonify, current_app, request, make_response, send_file
from urllib.request import urlopen

from reliascheduler.auth import get_current_user
from reliascheduler import weblab

scheduler_blueprint = Blueprint('scheduler', __name__)

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

@weblab.initial_url
def initial_url():
    return current_app['CDN_URL']

@scheduler_blueprint.route('/user/get_tasks')
def get_task():
    current_user = get_current_user()
    if current_user['anonymous']:
        return _corsify_actual_response(jsonify(success=False))

    r = redis.Redis()
    k = Keys(BASE_KEY)
    counter = 0
    task_id = []
    task_status = []
    task_priority = []
    for item in r.zrevrange(k.tasks(), 0, -1):
         if str(r.hget(item, 'author'), 'UTF-8') == current_user['username_unique'] and counter < 5:
              task_id.append(str(r.hget(item, 'id'), 'UTF-8'))
              task_status.append(str(r.hget(item, 'status'), 'UTF-8'))
              task_priority.append(str(r.zscore(k.tasks(), item)))
              counter += 1
    return _corsify_actual_response(jsonify(success=True, ids=task_id, statuses=task_status, priorities=task_priority, counter=counter))

@scheduler_blueprint.route('/user/add_tasks', methods=['POST'])
def load_task():
    current_user = get_current_user()
    if current_user['anonymous']:
    	return _corsify_actual_response(jsonify(success=False))
    
    # Load into Redis
    # NOTE: UUID cannot be used, as the ID generator must be strictly lexicographically increasing to satisfy sorted set requirements in Redis.
    r = redis.Redis()

    id = "0000000000"
    if r.get(BASE_KEY + ":relia:scheduler:id"):
        id = str(int(r.get(BASE_KEY + ":relia:scheduler:id").decode()) + 1).zfill(10)
        r.set(BASE_KEY + ":relia:scheduler:id", id)
    else:
        r.set(BASE_KEY + ":relia:scheduler:id", "0000000000")

    k = Keys(BASE_KEY)
    t = Task(BASE_KEY, id)

    # Access file name and contents for transmitter and receiver
    transmitterURL = request.form['transmitterName']
    receiverURL = request.form['receiverName']
    strip_character = '/'
    transmitterFile = open('../../Documents/relia-web/uploads/' + strip_character.join(transmitterURL.split('/')[3:]), 'r')
    transmitterContents = transmitterFile.read()
    transmitterFile.close()
    receiverFile = open('../../Documents/relia-web/uploads/' + strip_character.join(receiverURL.split('/')[3:]), 'r')
    receiverContents = receiverFile.read()
    receiverFile.close()
    transmitterName = os.path.basename(transmitterURL)
    receiverName = os.path.basename(receiverURL)

    p = r.pipeline()
    p.hset(t.identifiers(), 'id', id)
    p.hset(t.identifiers(), 'transmitterFilename', transmitterName)
    p.hset(t.identifiers(), 'receiverFilename', receiverName)
    p.hset(t.identifiers(), 'transmitterFile', transmitterContents)
    p.hset(t.identifiers(), 'receiverFile', receiverContents)
    p.hset(t.identifiers(), 'author', current_user['username_unique'])
    p.hset(t.identifiers(), 'startedTime', datetime.now().strftime("%H:%M:%S"))
    p.hset(t.identifiers(), 'transmitterAssigned', "null")
    p.hset(t.identifiers(), 'receiverAssigned', "null")
    p.hset(t.identifiers(), 'status', "queued")
    p.execute_command('ZADD', k.tasks(), 'NX', 2, t.identifiers())
    result = p.execute()
     
    return _corsify_actual_response(jsonify(success=True))

@scheduler_blueprint.route('/user/delete_tasks', methods=['POST'])
def delete_task():
    current_user = get_current_user()
    if current_user['anonymous']:
        return _corsify_actual_response(jsonify(success=False))

    r = redis.Redis()
    k = Keys(BASE_KEY)
    for item in r.zrange(k.tasks(), 0, -1):
        if str(r.hget(item, 'id'), 'UTF-8') == request.form['taskToCancel']:
            if str(r.hget(item, 'author'), 'UTF-8') == current_user['username_unique']:
                r.zrem(k.tasks(), item)
                break
    return _corsify_actual_response(jsonify(success=True))

@scheduler_blueprint.route('/device/init_device', methods=['POST'])
def init_device():
    r = redis.Redis()
    d = Device(BASE_KEY, request.form['deviceName'])
    r.hset(d.devices(), 'credential', request.form['password'])
    r.hset(d.devices(), 'name', request.form['deviceName'])
    r.hset(d.devices(), 'type', request.form['type'])
    r.hset(d.devices(), 'status', "0")
    return _corsify_actual_response(jsonify(success=True))

@scheduler_blueprint.route('/device/get_task', methods=['POST'])
def get_device_task():
    r = redis.Redis()
    d = Device(BASE_KEY, request.form['deviceName'])
    if str(r.hget(d.devices(), 'credential'), 'UTF-8') == request.form['password']:
         r.expire(d.devices(), 120)
         if str(r.hget(d.devices(), 'type'), 'UTF-8') == 'receiver':
              if r.hget(r.hget(d.devices(), 'pair'), 'credential'):
                   count = -1
                   while count >= -1 * r.zcard(k.tasks()):
                        assignment = r.zrange(k.tasks(), count, count)
                        if str(r.hget(assignment[0], 'status'), 'UTF-8') != "processing" and str(r.hget(assignment[0], 'status'), 'UTF-8') != "processing2":
                             r.hset(assignment[0], 'receiverAssigned', str(r.hget(d.devices(), 'name'), 'UTF-8'))
                             r.hset(assignment[0], 'status', "processing")
                             r.hset(d.devices(), 'status', "1")
                             break
                        else:
                             count -= 1
              else:
                   return _corsify_actual_response(jsonify(success=False))
         else:
              count = -1
              while count >= -1 * r.zcard(k.tasks()):
                   assignment = r.zrange(k.tasks(), count, count)
                   if str(r.hget(assignment[0], 'receiverAssigned'), 'UTF-8') == str(r.hget(r.hget(d.devices(), 'pair'), 'name'), 'UTF-8'):
                        r.hset(assignment[0], 'transmitterAssigned', str(r.hget(d.devices(), 'name'), 'UTF-8'))
                        r.hset(d.devices(), 'status', "1")
                        break
                   else:
                        count -= 1
         return _corsify_actual_response(jsonify(success=True))
    else:
         return _corsify_actual_response(jsonify(success=False))

@scheduler_blueprint.route('/device/complete_task', methods=['POST'])
def complete_device_task():
    r = redis.Redis()
    d = Device(BASE_KEY, request.form['deviceName'])
    if str(r.hget(d.devices(), 'credential'), 'UTF-8') == request.form['password']:
         r.expire(d.devices(), 120)
         count = -1
         while count >= -1 * r.zcard(k.tasks()):
              assignment = r.zrange(k.tasks(), count, count)
              deviceType = str(r.hget(d.devices(), 'type'), 'UTF-8')
              if str(r.hget(assignment[0], deviceType + 'Assigned'), 'UTF-8') == str(r.hget(d.devices(), 'name'), 'UTF-8'):
                   if str(r.hget(assignment[0], 'status'), 'UTF-8') == "processing":
                        r.hset(assignment[0], 'status', "processing2")
                        r.hset(d.devices(), 'status', "0")
                        break
                   elif str(r.hget(assignment[0], 'status'), 'UTF-8') == "processing2":
                        r.hset(assignment[0], 'status', "completed")
                        r.zincrby(k.tasks(), -1, assignment[0])
                        r.hset(d.devices(), 'status', "0")
                        break
              else:
                   count -= 1
         return _corsify_actual_response(jsonify(success=True))
    else:
         return _corsify_actual_response(jsonify(success=False))

def _corsify_actual_response(response):
    response.headers['Access-Control-Allow-Origin'] = '*';
    response.headers['Access-Control-Allow-Credentials'] = 'true';
    response.headers['Access-Control-Allow-Methods'] = 'OPTIONS, GET, POST';
    response.headers['Access-Control-Allow-Headers'] = 'Content-Type, Depth, User-Agent, X-File-Size, X-Requested-With, If-Modified-Since, X-File-Name, Cache-Control';
    return response