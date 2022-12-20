import sys
from flask import Flask
from flask_redis import FlaskRedis

from weblablib import WebLab

from config import configurations

# Plugins
redis_store = FlaskRedis()
weblab = WebLab()

def create_app(config_name: str = 'default'):

    # Based on Flasky https://github.com/miguelgrinberg/flasky
    app = Flask(__name__)
    app.config.from_object(configurations[config_name])

    if config_name in ['development', 'default'] and '--with-threads' not in sys.argv and 'run' in sys.argv:
        print("***********************************************************")
        print("*                                                         *")
        print("*               I M P O R T A N T                         *")
        print("*                                                         *")
        print("* You must pass --with-threads when testing data-uploader *")
        print("*                                                         *")
        print("***********************************************************")

    # Initialize plugins
    redis_store.init_app(app)
    weblab.init_app(app)

    # Register views
    from .views.main import main_blueprint
    from .views.scheduler import scheduler_blueprint

    app.register_blueprint(main_blueprint)
    app.register_blueprint(scheduler_blueprint, url_prefix='/scheduler')

    return app
