import bz2
import gzip
import json
import logging
import os
import re
import shutil
import tempfile
import urlparse
import zipfile

from urlparse import urljoin, urlsplit

import bcrypt
import click
import requests

from data_retrieval.files import copy_file_to_s3

logger = logging.getLogger(__name__)

# Used to authenticate data-processing to open-humans (and the inverse); must
# be set in the environment on both sides
PRE_SHARED_KEY = os.getenv('PRE_SHARED_KEY')


class BaseSource(object):
    """
    The base class for all data processing sources.

    Required arguments:
        oh_user_id: Open Humans user ID

    Optional arguments:
        output_directory: Local filepath, folder in which to place the
        resulting file.
        s3_bucket_name: S3 bucket to write resulting file.
        s3_key_dir: S3 key "directory" to write resulting file. The full S3 key
                    name will add a filename to the end of s3_key_dir.

    Either 'output_directory' (and no S3 arguments), or both S3 arguments (and
    no 'output_directory') must be specified.
    """

    def __init__(self, access_token=None, file_url=None, force=False,
                 input_file=None, local=False,
                 oh_base_url='https://www.openhumans.org/data-import/',
                 oh_user_id=None, oh_username=None, output_directory=None,
                 return_status=None, s3_bucket_name=None, s3_key_dir=None,
                 sentry=None, **kwargs):
        self.access_token = access_token
        self.file_url = file_url
        self.force = force
        self.input_file = input_file
        self.local = local
        self.oh_base_url = oh_base_url
        self.oh_user_id = oh_user_id
        self.oh_username = oh_username
        self.output_directory = output_directory
        self.return_status = return_status
        self.s3_bucket_name = s3_bucket_name
        self.s3_key_dir = s3_key_dir
        self.sentry = sentry

        self.temp_files = []
        self.data_files = []
        self.temp_directory = tempfile.mkdtemp()

    @property
    def files_url(self):
        return urljoin(self.oh_base_url, 'data-files/')

    @property
    def parameters_url(self):
        return urljoin(self.oh_base_url, 'parameters/')

    @property
    def update_url(self):
        return urljoin(self.oh_base_url, 'task-update/')

    @property
    def archive_url(self):
        return urljoin(self.oh_base_url, 'archive-data-files/')

    def get_current_files(self):
        return self.open_humans_request(
            url=self.files_url,
            data={'user_id': self.oh_user_id, 'source': self.source},
            method='get')['results']

    def archive_files(self):
        """
        The default implementation overrides all files; can be overridden by
        subclasses.
        """
        self.archive_current_files()

    def archive_current_files(self):
        current_files = self.get_current_files()

        if not current_files:
            logger.info('no files to archive')

            return

        response = self.open_humans_request(
            url=self.archive_url,
            data={'data_file_ids': [data_file['id']
                                    for data_file in current_files]},
            method='post')

        logger.info('remove files with IDs: "%s"', response.json()['ids'])

    def update_parameters(self):
        params = self.open_humans_request(
            url=self.parameters_url,
            data={'user_id': self.oh_user_id, 'source': self.source})

        logger.info('params: %s', params)

        for name, value in params.items():
            setattr(self, name, value)

    def validate_parameters(self):
        if not self.output_directory and not (self.s3_key_dir and
                                              self.s3_bucket_name):
            raise Exception(
                'output_directory or S3 parameters must be provided')

    def coerce_file(self):
        """
        Map remote file URLs into a local input file, if necessary.
        """
        if self.file_url and self.input_file:
            raise Exception('Run with input_file or file_url, not both')
        elif self.file_url and not self.input_file:
            self.input_file = self.temp_join(
                self.get_remote_file(self.file_url))

    def open_archive(self):
        error_message = ("Input file is expected to be either '.txt', "
                         "'.txt.gz', '.txt.bz2', or a single '.txt' file in a "
                         "'.zip' ZIP archive.")

        if self.input_file.endswith('.zip'):
            zip_file = zipfile.ZipFile(self.input_file)
            zip_files = self.filter_archive(zip_file)

            if len(zip_files) != 1:
                self.sentry_log(error_message)
                raise ValueError(error_message)

            return zip_file.open(zip_files[0])
        elif self.input_file.endswith('.txt.gz'):
            return gzip.open(self.input_file)
        elif self.input_file.endswith('.txt.bz2'):
            return bz2.BZ2File(self.input_file)
        elif self.input_file.endswith('.txt'):
            return open(self.input_file)

        self.sentry_log(error_message)
        raise ValueError(error_message)

    @staticmethod
    def filter_archive(zip_file):
        return [f for f in zip_file.namelist()
                if not f.startswith('__MACOSX/')]

    def sentry_log(self, message):
        message += ' Username: "{}", Source: "{}"'.format(self.oh_username, self.source)

        logger.warn(message)

        if self.sentry:
            self.sentry.captureMessage(message)

    def temp_join(self, path):
        return os.path.join(self.temp_directory, path)

    def get_remote_file(self, url):
        """
        Get and save a remote file to temporary directory. Return filename
        used.
        """
        logger.info('get_remote_file: retrieving "%s"', url)
        logger.info('get_remote_file: using temporary directory "%s"', url)

        # start a GET request but don't retrieve the data; we'll start
        # retrieval below
        request = requests.get(url, stream=True)

        if request.status_code != 200:
            raise Exception('File URL not working! Data processing aborted: {}'
                            .format(url))

        specified_filename = ''

        # try to retrieve the filename via the 'Content-Disposition' filename
        # header
        if 'Content-Disposition' in request.headers:
            filename = re.match(r'attachment; filename="(.*)"$',
                                request.headers['Content-Disposition'])

            if filename:
                specified_filename = filename.groups()[0]

        # if that header isn't sent then use the last portion of the URL as the
        # filename ('https://test.com/hello/world.zip' becomes 'world.zip')
        if not specified_filename:
            specified_filename = urlsplit(request.url)[2].split('/')[-1]

        logger.info('get_remote_file: filename "%s"', specified_filename)

        with open(self.temp_join(specified_filename), 'wb') as temp_file:
            # write each streamed chunk to the temporary file
            for chunk in request.iter_content(chunk_size=512 * 1024):
                if chunk:
                    temp_file.write(chunk)

        return specified_filename

    def should_update(self, files):
        """
        Sources should override this method and return True if the member's
        source data needs updating.
        """
        return True

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
            filename = file_info['temp_filename']

            if self.local:
                self.move_file(filename)
            else:
                self.move_file_s3(filename, file_info['metadata'])

        shutil.rmtree(self.temp_directory)

    @staticmethod
    def open_humans_request(data, url=None, method='get'):
        args = {
            'params': {
                'key': PRE_SHARED_KEY,
            },
            'url': url,
        }

        if method == 'get' and data:
            args['params'].update(data)
        elif method == 'post' and data:
            args['json'] = data

        response = getattr(requests, method)(**args)

        if method == 'get':
            return response.json()

        return response

    def update_open_humans(self):
        task_data = {
            'data_files': self.data_files,
            'oh_source': self.source,
            'oh_user_id': self.oh_user_id,
        }

        logger.info('Updating main site (%s) with completed files with '
                    'task_data: %s', self.update_url, json.dumps(task_data))

        self.open_humans_request(url=self.update_url,
                                 data={'task_data': task_data},
                                 method='post')

    def run(self):
        if not self.local:
            self.update_parameters()

        if not self.should_update(self.get_current_files()) and not self.force:
            return

        self.coerce_file()
        self.validate_parameters()

        result = self.create_files()

        # A result is only returned if we didn't successfully create files
        if result:
            return result

        self.move_files()

        if not self.local:
            self.archive_files()
            self.update_open_humans()

    def run_cli(self):
        self.run()

    @classmethod
    def make_cli(cls):
        """
        Make a cli method that can be further extended by subclasses.
        """
        @click.command()
        @click.option('-a', '--access-token')
        @click.option('-f', '--file-url')
        @click.option('-i', '--input-file')
        @click.option('-o', '--output-directory')
        @click.option('-u', '--oh-username')
        @click.option('-d', '--oh-user-id')
        @click.option('-f', '--force', is_flag=True, default=False)
        @click.option('-l', '--local', is_flag=True, default=True)
        def base_cli(**kwargs):
            logging.basicConfig(level=logging.DEBUG)

            source = cls(**kwargs)
            source.run_cli()

        return base_cli

    @classmethod
    def cli(cls):
        cls.make_cli()()
