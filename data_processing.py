#!/usr/bin/python

"""
Flask app to run data retrieval tasks for Open Humans
"""

import imp
import json
import logging
import os
import pkgutil
import sys

from functools import partial

import requests

from celery.signals import (after_setup_logger, after_task_publish,
                            task_postrun, task_prerun)

from flask import Flask, request
from flask_sslify import SSLify

from raven.contrib.flask import Sentry
from werkzeug.contrib.fixers import ProxyFix

from celery_worker import make_worker

app = Flask(__name__)

DEBUG = os.getenv('DEBUG', False)
PORT = os.getenv('PORT', 5000)

# A mapping of name/source argument pairs to send to the create_datafiles
# method
EXTRA_DATA = {
    'american_gut': {
        'survey_ids': 'surveyIds',
    },
    'wildlife': {
        'files': 'files',
    },
}

DATAFILES = {}

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
    CELERY_SEND_EVENTS=False,
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
        'task_data': {
            'task_id': task_id,
            'task_state': task_state,
        }
    }


@celery_worker.task
def task_update(update_url, task_data):
    """
    The 'after_task_publish' signal runs synchronously so we use celery itself
    to run it asynchronously.
    """
    logging.info('Sending queued update')

    requests.post(update_url, json=task_data)


@after_task_publish.connect
def task_sent_handler_cb(sender=None, body=None, **other_kwargs):
    """
    Send update that task has been sent to queue.
    """
    if sender == 'data_processing.task_update':
        return

    logging.debug('after_task_publish body: %s', debug_json(body))

    update_url = body['kwargs'].get('update_url')
    task_id = body['kwargs'].get('task_id')

    if not update_url or not task_id:
        return

    task_data = make_task_data(task_id, 'QUEUED')

    logging.info('Scheduling after_task_publish update')

    task_update.apply_async(args=[update_url, task_data], queue='priority')


@task_prerun.connect
def task_prerun_handler_cb(sender=None, kwargs=None, **other_kwargs):
    """
    Send update that task is starting run.
    """
    if sender == task_update:
        return

    logging.debug('task_prerun kwargs: %s', debug_json(kwargs))

    update_url = kwargs.get('update_url')
    task_id = kwargs.get('task_id')

    if not update_url or not task_id:
        return

    task_data = make_task_data(task_id, 'INITIATED')

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

    logging.debug('task_postrun kwargs: %s', debug_json(kwargs))

    update_url = kwargs.get('update_url')
    task_id = kwargs.get('task_id')

    if not update_url or not task_id:
        return

    task_data = make_task_data(task_id, state)

    logging.info('Scheduling task_postrun update')

    task_update.apply_async(args=[update_url, task_data], queue='priority')


def load_sources():
    """
    A generator that iterates and loads all of the modules in the sources/
    directory.
    """
    source_path = [os.path.join(
        os.path.dirname(os.path.abspath(__file__)), 'sources')]

    for _, name, _ in pkgutil.iter_modules(source_path):
        f, pathname, desc = imp.find_module(name, source_path)

        yield (name, imp.load_module(name, f, pathname, desc))


@celery_worker.task
def datafiles_task(name, **task_params):
    """
    A Celery task that runs a create_datafiles method and applies the argument
    mappings from EXTRA_DATA.
    """
    mapping = EXTRA_DATA.get(name)

    if mapping:
        for key, value in mapping.items():
            if value not in task_params['data']:
                return

            task_params[key] = task_params['data'][value]

    DATAFILES[name](sentry=sentry, **task_params)


def generic_handler(name):
    datafiles_task.delay(name, **request.json['task_params'])

    return '{} dataset started'.format(name)


def add_rules():
    for name, source in load_sources():
        logging.info('Adding "%s"', name)

        DATAFILES[name] = source.create_datafiles

        app.add_url_rule('/{}/'.format(name),
                         name,
                         partial(generic_handler, name),
                         methods=['GET', 'POST'])


@app.route('/', methods=['GET', 'POST'])
def index():
    """
    Main page for the app, primarily to give Pingdom something to monitor.
    """
    return 'Open Humans Data Processing'


add_rules()
