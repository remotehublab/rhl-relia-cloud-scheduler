import os

class Config:
    SECRET_KEY = os.environ.get('SECRET_KEY')
    REDIS_URL = os.environ.get('REDIS_URL') or "redis://localhost/0"
    UPLOAD_FOLDER = os.environ.get('UPLOAD_FOLDER')
    USE_FAKE_USERS = False
    CDN_URL = os.environ.get('CDN_URL')
    BASE_KEY = os.environ.get('BASE_KEY')
    MAX_PRIORITY_QUEUE = int(os.environ.get('MAX_PRIORITY_QUEUE') or '15')
    DEVICE_CREDENTIALS_FILENAME = os.environ.get('DEVICE_CREDENTIALS_FILENAME') or 'device-credentials.json'
    RELIA_BACKEND_TOKEN = os.environ.get('RELIA_BACKEND_TOKEN')
    MAX_TIME_RUNNING = float(os.environ.get('MAX_TIME_RUNNING') or '10')
    

class DevelopmentConfig(Config):
    DEBUG = True
    SECRET_KEY = 'secret'
    USE_FAKE_USERS = os.environ.get('USE_FAKE_USERS', '1') in ('1', 'true', 'True')
    CDN_URL = os.environ.get('CDN_URL') or 'http://localhost:3000/'
    BASE_KEY = os.environ.get('BASE_KEY') or 'uw-depl1'
    RELIA_BACKEND_TOKEN = os.environ.get('RELIA_BACKEND_TOKEN') or 'password'


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
