import os

from unittest import TestCase

from .utilities import apply_env, get_env

from .american_gut import create_amgut_ohdatasets
from .go_viral import create_go_viral_ohdataset
from .pgp_harvard import create_pgpharvard_ohdatasets
from .twenty_three_and_me import create_23andme_ohdataset

apply_env(get_env())


def get_23andme_dataset(**kwargs):
    token = os.getenv('TWENTY_THREE_AND_ME_TOKEN')
    profile = os.getenv('TWENTY_THREE_AND_ME_PROFILE')

    return create_23andme_ohdataset(token, profile, **kwargs)


def get_american_gut_datasets(**kwargs):
    barcode = '000007080'

    return create_amgut_ohdatasets([barcode], **kwargs)


def get_pgp_datasets(**kwargs):
    hu_id = 'hu43860C'

    return create_pgpharvard_ohdatasets(hu_id, **kwargs)


def get_go_viral_dataset(**kwargs):
    token = os.getenv('GO_VIRAL_MANAGEMENT_TOKEN')
    go_viral_id = 'simplelogin:5'

    return create_go_viral_ohdataset(token, go_viral_id, **kwargs)


class RetrievalTestCase(TestCase):
    def check_dataset(self, dataset):
        self.assertIsNotNone(dataset.filepath)
        self.assertIsNotNone(dataset.metadata)
        self.assertIsNotNone(dataset.source)


class FileTests(RetrievalTestCase):
    """
    Test the filedir case for each data retrieval module.
    """
    def __init__(self, *args, **kwargs):
        self.test_kwargs = {'filedir': 'test_data'}

        super(FileTests, self).__init__(*args, **kwargs)

    def test_twenty_three_and_me(self):
        self.check_dataset(get_23andme_dataset(**self.test_kwargs))

    def test_american_gut(self):
        for dataset in get_american_gut_datasets(**self.test_kwargs):
            self.check_dataset(dataset)

    def test_pgp(self):
        for dataset in get_pgp_datasets(**self.test_kwargs):
            self.check_dataset(dataset)

    def test_go_viral(self):
        self.check_dataset(get_go_viral_dataset(**self.test_kwargs))


class S3Tests(FileTests):
    """
    Test the S3 case for each data retrieval module.
    """
    def __init__(self, *args, **kwargs):
        self.test_kwargs = {
            's3_bucket_name': os.getenv('TEST_AWS_S3_BUCKET'),
            's3_key_dir': 'test-data'
        }

        super(FileTests, self).__init__(*args, **kwargs)

    @staticmethod
    def setup_class():
        os.environ['AWS_ACCESS_KEY_ID'] = os.getenv('TEST_AWS_ACCESS_KEY_ID')
        os.environ['AWS_SECRET_ACCESS_KEY'] = os.getenv(
            'TEST_AWS_SECRET_ACCESS_KEY')
