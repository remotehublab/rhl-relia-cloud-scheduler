from flask import current_app

from weblablib import weblab_user

def get_current_user():
    """
    get_current_user() returns either a real user taken from weblablib or
    alternatively, only if we are in development and USE_FAKE_USERS is true, then
    we create a fake user.
    """
    if current_app.debug and current_app.config['USE_FAKE_USERS']:
        return {
            'username_unique': "m4fxkAuTr9hnw_xnN1aE4UgRAvMAYBghDfzqsUYRr5g",
            'session_id': "my-session-id",
            'anonymous': False,
            'time_left': 600, # seconds (10 minutes)
            'locale': 'en', # English
        }

    if weblab_user.is_anonymous or not weblab_user.active:
        return {
            'username_unique': None,
            'session_id': None,
            'anonymous': weblab_user.is_anonymous,
            'time_left': 0,
            'locale': 'en',
        }

    return {
        'username_unique': weblab_user.username_unique,
        'anonymous': weblab_user.is_anonymous,
        'time_left': weblab_user.time_left,
        'locale': weblab_user.locale,
        'session_id': weblab_user.session_id,
    }
