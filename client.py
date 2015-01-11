#!/usr/bin/python
"""Flask app to run data retrieval tasks for Open Humans"""

import os

from celery import Celery
from celery.signals import task_postrun
import flask
from flask import request
import requests

from data_retrieval.american_gut import create_amgut_ohdataset
from data_retrieval.twenty_three_and_me import create_23andme_ohdataset

PORT = 5000


#####################################################################
# Set up celery and tasks.
def make_celery(app):
    """Set up celery tasks for an app."""
    celery = Celery(app.import_name, broker=app.config['CELERY_BROKER_URL'])
    celery.conf.update(app.config)
    TaskBase = celery.Task

    class ContextTask(TaskBase):
        abstract = True

        def __call__(self, *args, **kwargs):
            with app.app_context():
                return TaskBase.__call__(self, *args, **kwargs)
    celery.Task = ContextTask
    return celery

ohdata_app = flask.Flask("client")
ohdata_app.config.update(
    CELERY_BROKER_URL=os.environ.get('CLOUDAMQP_URL', 'amqp://'),
    BROKER_POOL_LIMIT=1,
)
celery_worker = make_celery(ohdata_app)


@task_postrun.connect()
def task_postrun_handler(sender=None, state=None, kwargs=None, **other_kwargs):
    params = {'name': sender.name,
              'state': state,
              's3_key_name': kwargs['s3_key_name']}
    url = "https://open-humans-staging.herokuapp.com/activity/task_update/"
    requests.post(url, data=params)


@celery_worker.task()
def make_amgut_ohdataset(barcode, s3_key_name, s3_bucket_name):
    """Task to initiate retrieval of American Gut data set"""
    create_amgut_ohdataset(barcode=barcode,
                           s3_bucket_name=S3_bucket_name,
                           s3_key_name=s3_key_name,)


@celery_worker.task()
def make_23andme_ohdataset(access_token, profile_id,
                           s3_key_name, s3_bucket_name):
    """Task to initiate retrieval of 23andme data set"""
    create_23andme_ohdataset(access_token=access_token,
                             profile_id=profile_id,
                             s3_bucket_name=s3_bucket_name,
                             s3_key_name=s3_key_name)


#####################################################################
# Pages to receive task requests.
@ohdata_app.route('/23andme', methods=['GET', 'POST'])
def twenty_three_and_me():
    """Page to receive 23andme task request"""
    # if request.method == 'POST':
    make_23andme_ohdataset.delay(access_token=request.args['access_token'],
                                 profile_id=request.args['profile_id'],
                                 s3_key_name=request.args['s3_key_name'],
                                 s3_bucket_name=request.args['s3_bucket_name'])
    return "23andme dataset started"


@ohdata_app.route('/amgut', methods=['GET', 'POST'])
def american_gut():
    """Page to receive American Gut task request"""
    # if request.method == 'POST':
    make_amgut_ohdataset.delay(barcode=request.args['barcode'],
                               s3_key_name=request.args['s3_key_name'],
                               s3_bucket_name=request.args['s3_bucket_name'])
    return "Amgut dataset started"


@ohdata_app.route('/', methods=['GET', 'POST'])
def main_page():
    """Main page for the app."""
    return "Open Humans Data Extraction - our Flask app"


if __name__ == '__main__':
    print "A local client for Open Humans data extraction is now initialized."
    ohdata_app.run(debug=True, port=PORT)
