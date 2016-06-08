#!/usr/bin/env python

from datetime import datetime, timedelta

from models import CacheItem
from utilities import init_db

db = init_db()

one_month_ago = datetime.now() - timedelta(days=30)

(CacheItem.query
 .filter(CacheItem.request_time < one_month_ago)
 .delete())
