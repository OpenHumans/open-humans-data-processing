#!/usr/bin/python

import os

from celery import Celery
import flask
from flask import request

from data_retrieval.american_gut import create_amgut_ohdataset
from data_retrieval.twenty_three_and_me import create_23andme_ohdataset

PORT = 5000
STORAGE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'files')

def make_celery(app):
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

app = flask.Flask("client")
app.config.update(
    CELERY_BROKER_URL='amqp://',
)

celery_worker = make_celery(app)


@celery_worker.task()
def start_amgut_ohdataset(barcode):
    print "In start amgut ohdataset"
    create_amgut_ohdataset(barcode=barcode)


@celery_worker.task()
def start_23andme_ohdataset(access_token, profile_id, file_id):
    create_23andme_ohdataset(access_token=access_token,
                             profile_id=profile_id,
                             file_id=file_id)


@app.route('/23andme', methods=['GET', 'POST'])
def twenty_three_and_me():
    # if request.method == 'POST':
    access_token = request.args['access_token']
    profile_id = request.args['profile_id']
    file_id = request.args['file_id']
    start_23andme_ohdataset.delay(access_token=access_token,
                                  profile_id=profile_id,
                                  file_id=file_id,
                                  output_dir=STORAGE_DIR)
    return "23andme dataset started"


@app.route('/amgut', methods=['GET', 'POST'])
def american_gut():
    # if request.method == 'POST':
    barcode = request.args['barcode']
    start_amgut_ohdataset.delay(barcode=barcode, output_dir=STORAGE_DIR)
    return "Amgut dataset started"


if __name__ == '__main__':
    print "A local client for Open Humans data extraction is now initialized."
    app.run(debug=True, port=PORT)
