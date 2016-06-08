import os

from flask import Flask


def init_db():
    from models import db

    app = Flask(__name__)

    app.config.update(
        SQLALCHEMY_DATABASE_URI=os.environ.get('DATABASE_URL'),
        SQLALCHEMY_TRACK_MODIFICATIONS=False)

    db.app = app
    db.init_app(app)

    return db
