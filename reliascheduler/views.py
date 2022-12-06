from flask import Blueprint

from datauploader import redis_store


main_blueprint = Blueprint('main', __name__)

@main_blueprint.route('/')
def index():
    return "Welcome to the scheduler"

api_blueprint = Blueprint('api', __name__)

@api_blueprint.route('/upload')
def upload():
    result = redis_store.get('result')
    return f"Uploading. btw, in redis I found: {result}"
