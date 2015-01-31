#!/usr/bin/python
"""Flask app to run data retrieval tasks for Open Humans"""

import os

from celery.signals import task_postrun

from flask import Flask, request
from flask_sslify import SSLify

import requests

from data_retrieval.american_gut import create_amgut_ohdataset
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


@task_postrun.connect()
def task_postrun_handler_cb(sender=None, state=None, kwargs=None,
                            **other_kwargs):
    params = {
        'name': sender.name,
        'state': state,
        's3_key_name': kwargs['s3_key_name']
    }

    url = kwargs['update_url']

    requests.post(url, data=params)


# Celery tasks
@celery_worker.task()
def make_amgut_ohdataset(barcode, s3_key_name, s3_bucket_name, update_url):
    """
    Task to initiate retrieval of American Gut data set
    """
    print "Starting work on American Gut dataset"

    create_amgut_ohdataset(barcode=barcode,
                           s3_bucket_name=s3_bucket_name,
                           s3_key_name=s3_key_name,
                           update_url=update_url)


@celery_worker.task()
def make_23andme_ohdataset(access_token, profile_id, s3_key_name,
                           s3_bucket_name, update_url):
    """
    Task to initiate retrieval of 23andme data set
    """
    print "Starting work on 23andMe dataset"

    create_23andme_ohdataset(access_token=access_token,
                             profile_id=profile_id,
                             s3_bucket_name=s3_bucket_name,
                             s3_key_name=s3_key_name,
                             update_url=update_url)


# Pages to receive task requests
@ohdata_app.route('/twenty_three_and_me', methods=['GET', 'POST'])
def twenty_three_and_me():
    """
    Page to receive 23andme task request
    """
    make_23andme_ohdataset.delay(access_token=request.args['access_token'],
                                 profile_id=request.args['profile_id'],
                                 s3_key_name=request.args['s3_key_name'],
                                 s3_bucket_name=request.args['s3_bucket_name'],
                                 update_url=request.args['update_url'])

    return "23andme dataset started"


@ohdata_app.route('/american_gut', methods=['GET', 'POST'])
def american_gut():
    """
    Page to receive American Gut task request
    """
    make_amgut_ohdataset.delay(barcode=request.args['barcode'],
                               s3_key_name=request.args['s3_key_name'],
                               s3_bucket_name=request.args['s3_bucket_name'],
                               update_url=request.args['update_url'])

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
