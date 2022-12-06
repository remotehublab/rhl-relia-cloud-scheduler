import os
from reliascheduler import create_app
application = create_app(os.environ.get('FLASK_CONFIG') or 'default')
