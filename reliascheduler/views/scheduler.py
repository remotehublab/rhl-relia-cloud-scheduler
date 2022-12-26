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

@weblab.initial_url
def initial_url():
    return current_app['CDN_URL']

@scheduler_blueprint.route('/user/tasks', methods=['POST'])
def load_task():
    current_user = get_current_user()
    if current_user['anonymous']:
    	return _corsify_actual_response(jsonify(success=False))
    r = redis.Redis()
    k = Keys('uw-depl1')
    t = Keys.Task(k, id)
    q = Keys.queuePriority(k, '1')
    id = uuid.uuid1()

    transmitterURL = request.files['transmitterName']
    receiverURL = request.files['receiverName']
    strip_character = '/'
    transmitterFile = open('../../../../Documents/relia-web/reliaweb/views/uploads/' + strip_character.join(transmitterURL.split('/')[3:]), 'r')
    transmitterContents = transmitterFile.read()
    transmitterFile.close()
    receiverFile = open('../../../../Documents/relia-web/reliaweb/views/uploads/' + strip_character.join(receiverURL.split('/')[3:]), 'r')
    receiverContents = receiverFile.read()
    receiverFile.close()
    transmitterName = os.path.basename(transmitterURL)
    receiverName = os.path.basename(receiverURL)

    p = r.pipeline()
    p.sadd(k.tasks, str(id))
    p.hset(t.identifiers, 'transmitterFilename', transmitterName)
    p.hset(t.identifiers, 'receiverFilename', receiverName)
    p.hset(t.identifiers, 'transmitterFile', transmitterContents)
    p.hset(t.identifiers, 'receiverFile', receiverContents)
    p.hset(t.identifiers, 'author', current_user['username_unique'])
    p.hset(t.identifiers, 'startedTime', datetime.now().strftime("%H:%M:%S"))
    p.hset(t.identifiers, 'deviceAssigned', "null")
    p.lpush(q.queuePriority, str(id))
    result = p.execute()
     
    return _corsify_actual_response(jsonify(success=True))

def _corsify_actual_response(response):
    response.headers['Access-Control-Allow-Origin'] = '*';
    response.headers['Access-Control-Allow-Credentials'] = 'true';
    response.headers['Access-Control-Allow-Methods'] = 'OPTIONS, GET, POST';
    response.headers['Access-Control-Allow-Headers'] = 'Content-Type, Depth, User-Agent, X-File-Size, X-Requested-With, If-Modified-Since, X-File-Name, Cache-Control';
    return response