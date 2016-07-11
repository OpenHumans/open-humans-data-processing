#!/usr/bin/env python

from models import CacheItem
from utilities import init_db

db = init_db()

deleted_rows = CacheItem.query.delete()

db.session.commit()

print 'Deleted {} rows'.format(deleted_rows)
