# import json
import os
import shutil
import tempfile

import requests

from data_retrieval.files import copy_file_to_s3

OPEN_HUMANS_TOKEN_REFRESH_URL = os.getenv(
    'OPEN_HUMANS_TOKEN_URL',
    'https://www.openhumans.org/api/processing/refresh-token/')

PRE_SHARED_KEY = os.getenv('PRE_SHARED_KEY')


class BaseSource(object):
    """
    The base class for all data processing sources.

    Required arguments:
        oh_user_id: Open Humans user ID
        oh_member_id: Open Humans member ID

    Optional arguments:
        output_directory: Local filepath, folder in which to place the
        resulting file.
        s3_bucket_name: S3 bucket to write resulting file.
        s3_key_dir: S3 key "directory" to write resulting file. The full S3 key
                    name will add a filename to the end of s3_key_dir.

    Either 'output_directory' (and no S3 arguments), or both S3 arguments (and
    no 'output_directory') must be specified.
    """

    def __init__(self, input_file=None, local=False, oh_member_id=None,
                 oh_update_url=None, oh_user_id=None, output_directory=None,
                 sentry=None, s3_key_dir=None, s3_bucket_name=None, **kwargs):
        if not output_directory and not (s3_key_dir and s3_bucket_name):
            raise Exception(
                'output_directory or S3 parameters must be provided')

        self.input_file = input_file
        self.local = local
        self.oh_member_id = oh_member_id
        self.oh_update_url = oh_update_url
        self.oh_user_id = oh_user_id
        self.output_directory = output_directory
        self.sentry = sentry
        self.s3_key_dir = s3_key_dir
        self.s3_bucket_name = s3_bucket_name

        self.temp_files = []
        self.data_files = []
        self.temp_directory = tempfile.mkdtemp()

    def should_update(self):
        """
        Sources should override this method and return True if the member's
        source data needs updating.
        """
        return True

    def refresh_token(self):
        """
        Get a fresh token from Open Humans for the given user ID and OAuth2
        provider.
        """
        response = requests.post(
            OPEN_HUMANS_TOKEN_REFRESH_URL,
            params={'key': PRE_SHARED_KEY},
            data={'user_id': self.oh_user_id, 'provider': self.oh_provider})

        try:
            result = response.json()
        except ValueError:
            print 'Unable to decode: {}'.format(response.text)

            raise

        return result['access_token']

    def move_file(self, filename):
        shutil.move(os.path.join(self.temp_directory, filename),
                    os.path.join(self.output_directory, filename))

    def move_file_s3(self, filename, metadata):
        """
        Copy a temp file to S3 or local permanent directory, then delete temp
        copy.
        """
        source = os.path.join(self.temp_directory, filename)
        destination = os.path.join(self.s3_key_dir, filename)

        copy_file_to_s3(bucket=self.s3_bucket_name,
                        filepath=source,
                        keypath=destination)

        os.remove(source)

        self.data_files.append({
            's3_key': destination,
            'metadata': metadata,
        })

    def move_files(self):
        for file_info in self.temp_files:
            filename = file_info['tmp_filename']

            if self.local:
                self.move_file(filename)
            else:
                self.move_file_s3(filename, file_info['metadata'])

        os.rmdir(self.temp_directory)

        if not self.local:
            self.update_open_humans()

    def update_open_humans(self):
        task_data = {
            'data_files': self.data_files,
            'oh_member_id': self.oh_member_id,
            'oh_user_id': self.oh_user_id,
            'oh_source': self.oh_source,
        }

        # status_msg = ('Updating main site ({}) with completed files'
        #               ' with task_data:\n{}'.format(
        #                   self.update_url, json.dumps(task_data)))

        # TODO: change to logger
        # print status_msg

        requests.post(self.update_url,
                      params={'key': PRE_SHARED_KEY},
                      json={'task_data': task_data})
