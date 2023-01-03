from flask import current_app

from weblablib import weblab_user

def get_current_user():
    return {
        'username_unique': "m4fxkAuTr9hnw_xnN1aE4UgRAvMAYBghDfzqsUYRr5g",
        'session_id': "my-session-id",
        'anonymous': False,
        'time_left': 600, # seconds (10 minutes)
        'locale': 'en', # English
    }