from datetime import datetime

from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.dialects.postgresql import JSON

db = SQLAlchemy()


class CacheItem(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    key = db.Column(db.String(length=1024))
    response = db.Column(JSON)
    request_time = db.Column(db.DateTime)

    def __init__(self, key, response):
        self.key = key
        self.response = response
        self.request_time = datetime.now()

    def __repr__(self):
        return "<CacheItem(url='{}')>".format(self.url)
