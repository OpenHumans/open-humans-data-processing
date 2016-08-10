"""
Run tests with `nosetests data_retrieval/tests.py`

* Make sure a ".env" exists with the appropriate variables (see "env.example").
* Make sure a "test-data" directory exists.
"""

import os

from unittest import TestCase

from .utilities import apply_env, get_env

apply_env(get_env())


def get_twenty_three_and_me_dataset(**kwargs):
    # For now, the following URL works as a publicly available example file.
    file_url = 'https://my.pgp-hms.org/user_file/download/1101'

    return datafiles_twenty_three_and_me(file_url=file_url, **kwargs)


def get_american_gut_dataset(**kwargs):
    survey_id = '94644ad5bb33a6b5'

    return datafiles_american_gut([survey_id], **kwargs)


def get_pgp_datasets(**kwargs):
    hu_id = 'hu43860C'

    return datafiles_pgp(hu_id, **kwargs)


def get_runkeeper_datasets(**kwargs):
    access_token = os.getenv('RUNKEEPER_ACCESS_TOKEN')

    return datafiles_runkeeper(access_token=access_token, **kwargs)


def get_go_viral_dataset(**kwargs):
    token = os.getenv('GO_VIRAL_MANAGEMENT_TOKEN')
    go_viral_id = 'simplelogin:5'

    return datafiles_go_viral(token, go_viral_id, **kwargs)


def get_wildlife_dataset(**kwargs):
    test_homedata_file = os.getenv('WILDLIFE_TESTFILE_HOMEDATA_URL')
    test_bacteria_file = os.getenv('WILDLIFE_TESTFILE_BACTERIADATA_URL')

    if test_homedata_file and test_bacteria_file:
        files = {'home_data.json': test_homedata_file,
                 'bacteria.csv.bz2': test_bacteria_file}

    return datafiles_wildlife(files, **kwargs)


class RetrievalTestCase(TestCase):
    def check_dataset(self, dataset):
        print dataset.filepath

        self.assertIsNotNone(dataset.filepath)
        self.assertIsNotNone(dataset.metadata)
        self.assertIsNotNone(dataset.source)


class S3Tests(FileTests):
    """
    Test the S3 case for each data retrieval module.
    """
    def get_test_kwargs(self):
        return {'s3_bucket_name': os.getenv('TEST_AWS_S3_BUCKET'),
                's3_key_dir': 'test-data'}

    @staticmethod
    def setup_class():
        os.environ['AWS_ACCESS_KEY_ID'] = os.getenv('TEST_AWS_ACCESS_KEY_ID')
        os.environ['AWS_SECRET_ACCESS_KEY'] = os.getenv(
            'TEST_AWS_SECRET_ACCESS_KEY')
