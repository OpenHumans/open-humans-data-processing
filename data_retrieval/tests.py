import os

from unittest import TestCase

from .utilities import apply_env, get_env

from .american_gut import create_amgut_ohdatasets
from .go_viral import create_go_viral_ohdataset
from .pgp_harvard import create_pgpharvard_ohdatasets
from .twenty_three_and_me import create_23andme_ohdataset

apply_env(get_env())


class RetrievalTestCase(TestCase):
    def check_dataset(self, dataset):
        self.assertIsNotNone(dataset.filepath)
        self.assertIsNotNone(dataset.metadata)
        self.assertIsNotNone(dataset.source)


class FileTests(RetrievalTestCase):
    """
    Test the filedir case for each data retrieval module.
    """
    def test_twenty_three_and_me(self):
        token = os.getenv('TWENTY_THREE_AND_ME_TOKEN')
        profile = os.getenv('TWENTY_THREE_AND_ME_PROFILE')

        dataset = create_23andme_ohdataset(token, profile, filedir='test_data')

        self.check_dataset(dataset)

    def test_american_gut(self):
        barcode = '000007080'

        datasets = create_amgut_ohdatasets([barcode],
                                           filedir='test_data')

        for dataset in datasets:
            self.check_dataset(dataset)

    def test_pgp(self):
        hu_id = 'hu43860C'

        datasets = create_pgpharvard_ohdatasets(hu_id,
                                                filedir='test_data')

        for dataset in datasets:
            self.check_dataset(dataset)

    def test_go_viral(self):
        token = os.getenv('GO_VIRAL_MANAGEMENT_TOKEN')
        go_viral_id = 'simplelogin:5'

        dataset = create_go_viral_ohdataset(token, go_viral_id,
                                            filedir='test_data')

        self.check_dataset(dataset)


class S3Tests(RetrievalTestCase):
    """
    Test the S3 case for each data retrieval module.
    """
    @staticmethod
    def setup_class():
        os.environ['AWS_ACCESS_KEY_ID'] = os.getenv('TEST_AWS_ACCESS_KEY_ID')
        os.environ['AWS_SECRET_ACCESS_KEY'] = os.getenv(
            'TEST_AWS_SECRET_ACCESS_KEY')

    def test_twenty_three_and_me(self):
        pass

    def test_american_gut(self):
        pass

    def test_pgp(self):
        hu_id = 'hu43860C'

        datasets = create_pgpharvard_ohdatasets(
            hu_id,
            s3_bucket_name=os.getenv('TEST_AWS_S3_BUCKET'),
            s3_key_dir='test-data')

        for dataset in datasets:
            self.check_dataset(dataset)

    def test_go_viral(self):
        pass
