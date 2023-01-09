import hashlib

from flask import current_app, request

from reliascheduler import redis_store
from reliascheduler.keys import DeviceKeys

def check_device_credentials():
    """
    Check if it is a request from an authenticated device.

    Return the device identifier or None if it is not authenticated
    """
    device = request.headers.get('relia-device')
    password = request.headers.get('relia-password')
    if not device or not password:
        return None

    salt_and_salted_password = redis_store.hget(DeviceKeys.credentials(), device)
    if not salt_and_salted_password:
        return None

    salt, salted_password = salt_and_salted_password.split('$')
    if hashlib.sha512((salt + password).encode()).hexdigest() == salted_password:
        return device
    return None

def check_backend_credentials():
    """
    Check if a request comes from the backend, by checking a secret in a header

    Return True if authenticated, False if not.
    """
    secret = request.headers.get('relia-secret')
    if not secret:
        return False

    return secret == current_app.config.get('RELIA_BACKEND_TOKEN')

