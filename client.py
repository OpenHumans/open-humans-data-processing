#!/usr/bin/python
"""Flask app to run data retrieval tasks for Open Humans"""
import json
import os

from celery.signals import after_task_publish, task_postrun, task_prerun

from flask import Flask, request
from flask_sslify import SSLify

import requests

from data_retrieval.american_gut import create_amgut_ohdatasets
from data_retrieval.twenty_three_and_me import create_23andme_ohdataset

from celery_worker import make_worker

PORT = os.getenv('PORT', 5000)

ohdata_app = Flask(__name__)

ohdata_app.config.update(
    DEBUG=os.getenv('DEBUG', False),
    CELERY_BROKER_URL=os.environ.get('CLOUDAMQP_URL', 'amqp://'),
    CELERY_ACCEPT_CONTENT=['json'],
    CELERY_TASK_SERIALIZER='json',
    CELERY_RESULT_SERIALIZER='json',
    BROKER_POOL_LIMIT=0)

sslify = SSLify(ohdata_app)

celery_worker = make_worker(ohdata_app)


@after_task_publish.connect()
def task_sent_handler_cb(sender=None, body=None, **other_kwargs):
    """
    Send update that task has been sent to queue.
    """
    task_data = {
        'task_id': body['kwargs']['task_id'],
        'tast_state': 'QUEUED',
    }
    update_url = body['kwargs']['update_url']
    requests.post(update_url, data={'task_data': json.dumps(task_data)})


@task_postrun.connect()
def task_postrun_handler_cb(sender=None, state=None, kwargs=None,
                            **other_kwargs):
    """
    Send update that task run is complete.
    """
    task_data = {
        'task_id': kwargs['task_id'],
        'task_state': state,
    }
    update_url = kwargs['update_url']
    requests.post(update_url, data={'task_data': json.dumps(task_data)})


@task_prerun.connect()
def task_prerun_handler_cb(sender=None, kwargs=None, **other_kwargs):
    """
    Send update that task is starting run.
    """
    task_data = {
        'task_id': kwargs['task_id'],
        'task_state': 'INITIATED',
    }
    update_url = kwargs['update_url']
    requests.post(update_url, data={'task_data': json.dumps(task_data)})


# Celery tasks
@celery_worker.task()
def make_amgut_ohdataset(**task_params):
    """
    Task to initiate retrieval of American Gut data set
    """
    create_amgut_ohdatasets(**task_params)


@celery_worker.task()
def make_23andme_ohdataset(**task_params):
    """
    Task to initiate retrieval of 23andme data set
    """
    print task_params
    create_23andme_ohdataset(**task_params)


# Pages to receive task requests
@ohdata_app.route('/twenty_three_and_me', methods=['GET', 'POST'])
def twenty_three_and_me():
    """
    Page to receive 23andme task request
    """
    task_params = json.loads(request.args['task_params'])
    make_23andme_ohdataset.delay(**task_params)
    return "23andme dataset started"


@ohdata_app.route('/american_gut', methods=['GET', 'POST'])
def american_gut():
    """
    Page to receive American Gut task request
    """
    task_params = json.loads(request.args['task_params'])
    make_amgut_ohdataset.delay(**task_params)
    return "Amgut dataset started"


@ohdata_app.route('/', methods=['GET', 'POST'])
def main_page():
    """
    Main page for the app.
    """
    return "Open Humans Data Processing"


if __name__ == '__main__':
    print "A local client for Open Humans data processing is now initialized."

    ohdata_app.run(debug=True, port=PORT)
