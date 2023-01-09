import os
import sys
import json
import random
import string
import getpass
import hashlib

from flask import Flask
from flask_redis import FlaskRedis

from config import configurations

# Plugins
redis_store = FlaskRedis(decode_responses=True)

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

    # Register views
    from .views.main import main_blueprint
    from .views.scheduler import scheduler_blueprint

    app.register_blueprint(main_blueprint)
    app.register_blueprint(scheduler_blueprint, url_prefix='/scheduler')

    @app.cli.group()
    def device_credentials():
        "Manage device credentials"

    @device_credentials.command("add")
    def add_device_credentials():
        """
        Add credentials for a new device
        """
        credentials_filename = app.config['DEVICE_CREDENTIALS_FILENAME']
        if not os.path.exists(credentials_filename):
            existing_credentials = {}
        else:
            existing_credentials = json.load(open(credentials_filename))

        while True:
            device_identifier = input("Device identifier (e.g., uw-s1i1r): ").strip()
            password = getpass.getpass("Password: ").strip()
            if len(device_identifier) >= 3 or len(password) >= 8:
                break
            print(f"Too short device identifier ({len(device_identifier)}, min 3) or password ({len(password)}, min 8). Try again")
        
        # 6 letters for the salt
        salt = ''.join(random.sample(string.ascii_letters, 6))
        hashed = hashlib.sha512((salt + password).encode()).hexdigest()

        if device_identifier in existing_credentials:
            updating = True
        else:
            updating = False

        existing_credentials[device_identifier] = salt + '$' + hashed

        open(credentials_filename, 'w').write(json.dumps(existing_credentials, indent=4))
        if updating:
            print(f"File {credentials_filename} updated with an updated password for {device_identifier}")
        else:
            print(f"File {credentials_filename} updated with an new account for {device_identifier}")
        _push_device_credentials_to_redis()

    def _push_device_credentials_to_redis():
        credentials_filename = app.config['DEVICE_CREDENTIALS_FILENAME']
        if not os.path.exists(credentials_filename):
            print("Error: {credentials_filename} not found")
            return -1

        from reliascheduler.keys import DeviceKeys

        existing_credentials = json.load(open(credentials_filename))

        # In a single transaction, delete all the existing credentials and add all of them
        pipeline = redis_store.pipeline()
        pipeline.delete(DeviceKeys.credentials())
        for device_identifier, salted_password in existing_credentials.items():
            pipeline.hset(DeviceKeys.credentials(), device_identifier, salted_password)
        pipeline.execute()
        print(f"{len(existing_credentials)} credentials pushed to the Redis Server")

    @device_credentials.command("push")
    def push_to_redis():
        """
        Push the credentials from the file to Redis
        """
        _push_device_credentials_to_redis()

    return app
