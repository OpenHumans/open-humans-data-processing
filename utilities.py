import os

import requests

from flask import Flask

OPEN_HUMANS_TOKEN_URL = os.getenv(
    'OPEN_HUMANS_TOKEN_URL',
    'https://www.openhumans.org/api/processing/refresh-token/')

PRE_SHARED_KEY = os.getenv('PRE_SHARED_KEY')


def init_db():
    from models import db

    app = Flask(__name__)

    app.config.update(
        SQLALCHEMY_DATABASE_URI=os.environ.get('DATABASE_URL'),
        SQLALCHEMY_TRACK_MODIFICATIONS=False)

    db.app = app
    db.init_app(app)

    return db


def get_fresh_token(user_id, provider):
    """
    Get a fresh token from Open Humans for the given user ID.
    """
    response = requests.post(
        '{}?key={}'.format(OPEN_HUMANS_TOKEN_URL, PRE_SHARED_KEY),
        data={'user_id': user_id, 'provider': provider})

    result = response.json()

    return result['access_token']
