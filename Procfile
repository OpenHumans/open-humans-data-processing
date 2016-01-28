web: uwsgi uwsgi.ini
worker: celery -A data_processing.celery_worker worker -Q celery -n worker.%h --without-gossip --without-mingle --without-heartbeat
priority: celery -A data_processing.celery_worker worker -Q priority -n priority.%h --without-gossip --without-mingle --without-heartbeat
