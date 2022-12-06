import os

class Config:
    SECRET_KEY = os.environ.get('SECRET_KEY')
    REDIS_URL = os.environ.get('REDIS_URL') or "redis://localhost/0"

class DevelopmentConfig(Config):
    DEBUG = True
    SECRET_KEY = 'secret'

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
