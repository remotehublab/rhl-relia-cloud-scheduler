from flask import Flask
from flask_redis import FlaskRedis

from config import configurations

# Plugins
redis_store = FlaskRedis()


def create_app(config_name: str = 'default'):

    # Based on Flasky https://github.com/miguelgrinberg/flasky
    app = Flask(__name__)
    app.config.from_object(configurations[config_name])

    # Initialize plugins
    redis_store.init_app(app)


    # Register views
    from .views import main_blueprint, api_blueprint

    app.register_blueprint(main_blueprint)
    app.register_blueprint(api_blueprint, url_prefix='/api')

    return app
