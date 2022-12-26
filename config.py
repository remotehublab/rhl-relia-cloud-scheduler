import os

class Keys:
    def __init__(self, base_key):
        self.base_key = base_key
   
    @property
    def tasks(self):
        return f"{self.base_key}:relia:scheduler:tasks"

    class Task:
        def __init__(self, base_key, task_identifier):
             self.base_key = base_key
             self.task_identifier = task_identifier

        @property
        def identifiers(self):
             return f"{self.base_key}:relia:scheduler:tasks:{self.task_identifier}"

    class queuePriority:
        def __init__(self, base_key, queue_priority):
             self.base_key = base_key
             self.queue_priority = queue_priority

        @property
        def queuePriority(self):
             return f"{self.base_key}:relia:scheduler:tasks:{self.queue_priority}"

class Config:
    SECRET_KEY = os.environ.get('SECRET_KEY')
    REDIS_URL = os.environ.get('REDIS_URL') or "redis://localhost/0"
    WEBLAB_USERNAME = os.environ.get('WEBLAB_USERNAME')
    WEBLAB_PASSWORD = os.environ.get('WEBLAB_PASSWORD')
    UPLOAD_FOLDER = os.environ.get('UPLOAD_FOLDER')
    USE_FAKE_USERS = False
    CDN_URL = os.environ.get('CDN_URL')

class DevelopmentConfig(Config):
    DEBUG = True
    SECRET_KEY = 'secret'
    WEBLAB_USERNAME = os.environ.get('WEBLAB_USERNAME') or 'weblab'
    WEBLAB_PASSWORD = os.environ.get('WEBLAB_PASSWORD') or 'password'
    USE_FAKE_USERS = os.environ.get('USE_FAKE_USERS', '1') in ('1', 'true', 'True')
    CDN_URL = os.environ.get('CDN_URL') or 'http://localhost:3000/'

class StagingConfig(Config):
    DEBUG = False

class ProductionConfig(Config):
    DEBUG = False

configurations = {
    'default': DevelopmentConfig,
    'development': DevelopmentConfig,
    'staging': StagingConfig,
    'production': ProductionConfig,
}
