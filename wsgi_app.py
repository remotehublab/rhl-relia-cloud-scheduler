import os
import sys

project_dir = os.path.abspath(os.path.dirname(__file__))

sys.path.insert(0, project_dir)
os.chdir(project_dir)

if not os.path.exists('logs'):
    os.mkdir('logs')

sys.stdout = open('logs/stdout.txt', 'a')
sys.stderr = open('logs/stderr.txt', 'a')

from reliabackend import create_app
application = create_app(os.environ['FLASK_CONFIG'])

import logging

file_handler = logging.FileHandler(filename='logs/errors.log')
file_handler.setLevel(logging.INFO)
application.logger.addHandler(file_handler)
