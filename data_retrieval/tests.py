"""
Run tests with `nosetests data_retrieval/tests.py`

* Make sure a ".env" exists with the appropriate variables (see "env.example").
* Make sure a "test-data" directory exists.
"""
import os

from unittest import TestCase

from .utilities import apply_env, get_env

from .american_gut import create_amgut_ohdataset
from .go_viral import create_go_viral_ohdataset
from .pgp_harvard import create_pgpharvard_ohdatasets
from .runkeeper import create_runkeeper_ohdatasets
from .twenty_three_and_me import create_23andme_ohdataset
from .wildlife import create_wildlife_ohdataset

apply_env(get_env())


def get_23andme_dataset(**kwargs):
    # For now, the following URL works as a publicly available example file.
    file_url = 'https://my.pgp-hms.org/user_file/download/1101'

    return create_23andme_ohdataset(file_url=file_url, **kwargs)


def get_american_gut_dataset(**kwargs):
    survey_id = '94644ad5bb33a6b5'

    return create_amgut_ohdataset([survey_id], **kwargs)


def get_pgp_datasets(**kwargs):
    hu_id = 'hu43860C'

    return create_pgpharvard_ohdatasets(hu_id, **kwargs)


def get_runkeeper_datasets(**kwargs):
    access_token = os.getenv('RUNKEEPER_ACCESS_TOKEN')

    return create_runkeeper_ohdatasets(access_token=access_token, **kwargs)


def get_go_viral_dataset(**kwargs):
    token = os.getenv('GO_VIRAL_MANAGEMENT_TOKEN')
    go_viral_id = 'simplelogin:5'

    return create_go_viral_ohdataset(token, go_viral_id, **kwargs)


def get_wildlife_dataset(**kwargs):
    test_homedata_file = os.getenv('WILDLIFE_TESTFILE_HOMEDATA_URL')
    test_bacteria_file = os.getenv('WILDLIFE_TESTFILE_BACTERIADATA_URL')
    if test_homedata_file and test_bacteria_file:
        files = {'home_data.json': test_homedata_file,
                 'bacteria.csv.bz2': test_bacteria_file}
    return create_wildlife_ohdataset(files, **kwargs)


class RetrievalTestCase(TestCase):
    def check_dataset(self, dataset):
        print dataset.filepath
        self.assertIsNotNone(dataset.filepath)
        self.assertIsNotNone(dataset.metadata)
        self.assertIsNotNone(dataset.source)


class FileTests(RetrievalTestCase):
    """
    Test the filedir case for each data retrieval module.
    """
    def __init__(self, *args, **kwargs):
        self.test_kwargs = self.get_test_kwargs()
        super(FileTests, self).__init__(*args, **kwargs)

    def get_test_kwargs(self):
        return {'filedir': 'test_data'}

    """
    def test_american_gut(self):
        self.check_dataset(get_american_gut_dataset(**self.test_kwargs))

    def test_go_viral(self):
        self.check_dataset(get_go_viral_dataset(**self.test_kwargs))

    def test_pgp(self):
        for dataset in get_pgp_datasets(**self.test_kwargs):
            self.check_dataset(dataset)

    def test_runkeeper(self):
        for dataset in get_runkeeper_datasets(**self.test_kwargs):
            self.check_dataset(dataset)

    def test_twenty_three_and_me(self):
        self.check_dataset(get_23andme_dataset(**self.test_kwargs))
    """
    def test_wildlife(self):
        self.check_dataset(get_wildlife_dataset(**self.test_kwargs))


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
