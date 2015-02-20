from os import getenv
from unittest import TestCase

from .utilities import apply_env, get_env

from .american_gut import create_amgut_ohdatasets
from .pgp_harvard import create_pgpharvard_ohdatasets
from .twenty_three_and_me import create_23andme_ohdataset

apply_env(get_env())


class FileTests(TestCase):
    """
    Test the filedir case for each data retrieval module.
    """
    def test_twenty_three_and_me(self):
        token = getenv('TWENTY_THREE_AND_ME_TOKEN')
        profile = getenv('TWENTY_THREE_AND_ME_PROFILE')

        dataset = create_23andme_ohdataset(token, profile, filedir='test_data')

        self.assertIsNotNone(dataset.filepath)
        self.assertIsNotNone(dataset.metadata)
        self.assertIsNotNone(dataset.source)

    def test_american_gut(self):
        barcode = '000007080'

        datasets = create_amgut_ohdatasets([barcode], filedir='test_data')

        for dataset in datasets:
            self.assertIsNotNone(dataset.filepath)
            self.assertIsNotNone(dataset.metadata)
            self.assertIsNotNone(dataset.source)

    def test_pgp(self):
        hu_id = 'hu43860C'

        datasets = create_pgpharvard_ohdatasets(hu_id, filedir='test_data')

        for dataset in datasets:
            self.assertIsNotNone(dataset.filepath)
            self.assertIsNotNone(dataset.metadata)
            self.assertIsNotNone(dataset.source)

    def test_go_viral(self):
        pass


class S3Tests(TestCase):
    """
    Test the S3 case for each data retrieval module.
    """
    def test_twenty_three_and_me(self):
        pass

    def test_american_gut(self):
        pass

    def test_pgp(self):
        pass

    def test_go_viral(self):
        pass
