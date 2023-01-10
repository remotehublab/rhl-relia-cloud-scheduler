import hashlib

from typing import Optional

from flask import current_app, request

from reliascheduler import redis_store
from reliascheduler.keys import DeviceKeys

def check_device_credentials() -> Optional[str]:
    """
    Check if it is a request from an authenticated device.

    Device pairs are identified as:
      uw-s1i1:r
      uw-s1i1:t

    where uw-s1i1 represents the credentials and 'r' or 't' must be
    the receiver or transmitter.

    Return the device identifier or None if it is not authenticated
    """
    device = request.headers.get('relia-device')
    password = request.headers.get('relia-password')
    if not device or not password:
        return None

    if device.count(':') != 1:
        return None
    
    device_base, device_type = device.split(':')
    if device_type not in ('r', 't'):
        return None

    salt_and_salted_password = redis_store.hget(DeviceKeys.credentials(), device_base)
    if not salt_and_salted_password:
        return None

    salt, salted_password = salt_and_salted_password.split('$')
    if hashlib.sha512((salt + password).encode()).hexdigest() == salted_password:
        return device
    return None

def check_backend_credentials() -> Optional[bool]:
    """
    Check if a request comes from the backend, by checking a secret in a header

    Return True if authenticated, False if not.
    """
    secret = request.headers.get('relia-secret')
    if not secret:
        return False

    return secret == current_app.config.get('RELIA_BACKEND_TOKEN')

