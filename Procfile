web: uwsgi uwsgi.ini
worker: celery -A client.celery_worker worker -Q celery -n worker.%h
priority: celery -A client.celery_worker worker -Q priority -n priority.%h
