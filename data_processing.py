#!/usr/bin/python

"""
Flask app to run data retrieval tasks for Open Humans
"""

import json
import logging
import os

import requests

from celery.signals import (after_setup_logger, after_task_publish,
                            task_postrun, task_prerun)

from flask import Flask, request
from flask_sslify import SSLify

from raven.contrib.flask import Sentry
from werkzeug.contrib.fixers import ProxyFix

from celery_worker import make_worker

from data_retrieval.american_gut import create_amgut_datafiles
from data_retrieval.pgp_harvard import create_pgpharvard_datafiles
from data_retrieval.twenty_three_and_me import create_23andme_datafiles
from data_retrieval.go_viral import create_go_viral_datafiles
from data_retrieval.runkeeper import create_runkeeper_ohdatasets
from data_retrieval.wildlife import create_wildlife_ohdataset

app = Flask(__name__)

DEBUG = os.getenv('DEBUG', False)
PORT = os.getenv('PORT', 5000)

logging.basicConfig(level=logging.DEBUG if DEBUG else logging.INFO)
logging.info('Starting data-processing')

# trust X-Forwarded-For on Heroku for better debugging information with Sentry
if os.getenv('HEROKU') == 'true':
    app.wsgi_app = ProxyFix(app.wsgi_app)

app.config.update(
    DEBUG=DEBUG,
    CELERY_BROKER_URL=os.environ.get('CLOUDAMQP_URL', 'amqp://'),
    CELERY_ACCEPT_CONTENT=['json'],
    CELERY_TASK_SERIALIZER='json',
    CELERY_RESULT_SERIALIZER='json',
    CELERYD_LOG_COLOR=True,
    BROKER_POOL_LIMIT=0)

sentry = Sentry(app)
sslify = SSLify(app)

celery_worker = make_worker(app)


@after_setup_logger.connect
def after_setup_logger_cb(logger, **kwargs):
    """
    Update the Celery logger's level.
    """
    if DEBUG:
        logger.setLevel(logging.DEBUG)


def debug_json(value):
    """
    Return a human-readable representation of JSON data.
    """
    return json.dumps(value, sort_keys=True, indent=2, separators=(',', ': '))


def make_task_data(task_id, task_state):
    """
    Format task data for the Open Humans update endpoint.
    """
    return {
        'task_data': json.dumps({
            'task_id': task_id,
            'task_state': task_state,
        })
    }


@celery_worker.task
def task_update(update_url, task_data):
    """
    The 'after_task_publish' signal runs synchronously so we use celery itself
    to run it asynchronously.
    """
    logging.info('Sending queued update')

    requests.post(update_url, data=task_data)


@after_task_publish.connect
def task_sent_handler_cb(sender=None, body=None, **other_kwargs):
    """
    Send update that task has been sent to queue.
    """
    if sender == 'data_processing.task_update':
        return

    logging.debug('after_task_publish sender: %s', sender)
    logging.debug('after_task_publish body: %s', debug_json(body))

    update_url = body['kwargs']['update_url']
    task_data = make_task_data(body['kwargs']['task_id'], 'QUEUED')

    logging.info('Scheduling after_task_publish update')

    task_update.apply_async(args=[update_url, task_data], queue='priority')


@task_prerun.connect
def task_prerun_handler_cb(sender=None, kwargs=None, **other_kwargs):
    """
    Send update that task is starting run.
    """
    if sender == task_update:
        return

    logging.debug('task_prerun sender: %s', sender)
    logging.debug('task_prerun kwargs: %s', debug_json(kwargs))

    update_url = kwargs['update_url']
    task_data = make_task_data(kwargs['task_id'], 'INITIATED')

    logging.info('Scheduling task_prerun update')

    task_update.apply_async(args=[update_url, task_data], queue='priority')


@task_postrun.connect
def task_postrun_handler_cb(sender=None, state=None, kwargs=None,
                            **other_kwargs):
    """
    Send update that task run is complete.
    """
    if sender == task_update:
        return

    logging.debug('task_postrun sender: %s', sender)
    logging.debug('task_postrun kwargs: %s', debug_json(kwargs))

    update_url = kwargs['update_url']
    task_data = make_task_data(kwargs['task_id'], state)

    logging.info('Scheduling task_postrun update')

    task_update.apply_async(args=[update_url, task_data], queue='priority')


# Celery tasks
@celery_worker.task
def make_23andme_datafiles(**task_params):
    """
    Task to initiate retrieval of 23andMe data set.
    """
    file_url = task_params.pop('file_url')
    create_23andme_datafiles(file_url=file_url, sentry=sentry, **task_params)


@celery_worker.task
def make_amgut_datafiles(**task_params):
    """
    Task to initiate retrieval of American Gut data set.

    Data retrieval is based on the survey IDs, which American Gut pushes
    to Open Humans as an object in the generic 'data' field, e.g.:
    { 'surveyIds': [ '614a55f251eb12ec' ] }
    """
    data = task_params.pop('data')
    if 'surveyIds' in data:
        create_amgut_datafiles(survey_ids=data['surveyIds'], **task_params)


@celery_worker.task
def make_pgpharvard_datafiles(**task_params):
    """
    Task to initiate retrieval of PGP Harvard data set
    """
    create_pgpharvard_datafiles(sentry=sentry, **task_params)


@celery_worker.task
def make_go_viral_datafiles(**task_params):
    """
    Task to initiate retrieval of GoViral data set
    """
    create_go_viral_datafiles(**task_params)


@celery_worker.task
def make_runkeeper_ohdataset(**task_params):
    """
    Task to initiate retrieval of RunKeeper data set
    """
    create_runkeeper_ohdatasets(**task_params)


@celery_worker.task
def make_wildlife_ohdataset(**task_params):
    """
    Task to initiate retrieval of American Gut data set.

    Data retrieval is based on the survey IDs, which American Gut pushes
    to Open Humans as an object in the generic 'data' field, e.g.:
    { 'surveyIds': [ '614a55f251eb12ec' ] }
    """
    data = task_params.pop('data')
    if 'files' in data:
        create_wildlife_ohdataset(files=data['files'], **task_params)


# Pages to receive task requests
@app.route('/twenty_three_and_me', methods=['GET', 'POST'])
def twenty_three_and_me():
    """
    Page to receive 23andme task request

    'task_params' specific to this task:
        'file_url' (string, for accessing the uploaded file)
    """
    task_params = json.loads(request.args['task_params'])
    make_23andme_datafiles.delay(**task_params)
    return '23andMe dataset started'


@app.route('/american_gut', methods=['GET', 'POST'])
def american_gut():
    """
    Page to receive American Gut task request

    'task_params' specific to this task:
        'data' (JSON format, must contain 'surveyIDs')
    """
    task_params = json.loads(request.args['task_params'])
    make_amgut_datafiles.delay(**task_params)
    return 'Amgut dataset started'


@app.route('/pgp', methods=['GET', 'POST'])
def pgp_harvard():
    """
    Page to receive PGP Harvard task request

    'task_params' specific to this task:
        'huID' (string with PGP ID, eg 'hu1A2B3C')
    """
    task_params = json.loads(request.args['task_params'])
    make_pgpharvard_datafiles.delay(**task_params)
    return 'PGP Harvard dataset started'


@app.route('/go_viral', methods=['GET', 'POST'])
def go_viral():
    """
    Page to receive GoViral task request

    'task_params' specific to this task:
        'go_viral_id' (string with GoViral ID)
    """
    task_params = json.loads(request.args['task_params'])
    make_go_viral_datafiles.delay(**task_params)
    return 'GoViral dataset started'


@app.route('/runkeeper', methods=['GET', 'POST'])
def runkeeper():
    """
    Page to receive RunKeeper task request

    'task_params' specific to this task:
        'access_token' (string, token for accessing data via RunKeeper API)
    """
    task_params = json.loads(request.args['task_params'])
    make_runkeeper_ohdataset.delay(**task_params)
    return 'RunKeeper dataset started'


@app.route('/wildlife', methods=['GET', 'POST'])
def wildlife():
    """
    Page to receive Wild Life of Our Homes task request

    'task_params' specific to this task:
        'data' (JSON format, expected to contain 'files')
    """
    task_params = json.loads(request.args['task_params'])
    make_wildlife_ohdataset.delay(**task_params)
    return 'Wildlife dataset started'


@app.route('/', methods=['GET', 'POST'])
def main_page():
    """
    Main page for the app.
    """
    return 'Open Humans Data Processing'
