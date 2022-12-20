import os
import glob
import logging

from werkzeug.utils import secure_filename
from flask import Blueprint, jsonify, current_app, request, make_response, send_file

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
    print('Hello', flush=True)
    return _corsify_actual_response(jsonify(success=True))

def _corsify_actual_response(response):
    response.headers['Access-Control-Allow-Origin'] = '*';
    response.headers['Access-Control-Allow-Credentials'] = 'true';
    response.headers['Access-Control-Allow-Methods'] = 'OPTIONS, GET, POST';
    response.headers['Access-Control-Allow-Headers'] = 'Content-Type, Depth, User-Agent, X-File-Size, X-Requested-With, If-Modified-Since, X-File-Name, Cache-Control';
    return response