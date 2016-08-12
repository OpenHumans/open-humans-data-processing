r"""
mPower genotyping data extraction.

Copyright (C) 2016 PersonalGenomes.org

This software is shared under the "MIT License" license (aka "Expat License"),
see LICENSE.TXT for full license text.

May be used on the command line. For example, the following command:

    python -m sources.mpower ~/Downloads/mPower.zip files

Will assemble processed data sets in files/
"""

import shutil
import zipfile

from base_source import BaseSource


class MPowerSource(BaseSource):
    """
    Create Open Humans Dataset from uploaded mPower data.
    """

    def verify_mpower(self, input_filepath):
        """
        Create clean file in mPower format from downloaded version
        """
        if input_filepath.endswith('.zip'):
            zip_file = zipfile.ZipFile(input_filepath)
            zip_files = self.filter_archive(zip_file)

            for filename in zip_files:
                if not filename.startswith('parkinson-'):
                    self.sentry_log(
                        'mPower file did not conform to expected format.')

                    raise ValueError(
                        'Found a filename that did not start with '
                        '"parkinson-": "{}"'.format(filename))
        else:
            raise ValueError('Input file is expected to be a ZIP archive')

    def create_files(self):
        self.verify_mpower(self.input_file)

        shutil.copyfile(self.input_file,
                        self.temp_join('mPower-Parkinsons.zip'))

        self.temp_files.append({
            'temp_filename': 'mPower-Parkinsons.zip',
            'metadata': {
                'description': 'mPower data, original format',
                'tags': ['mPower', 'CSV', 'JSON'],
            },
        })


if __name__ == '__main__':
    MPowerSource.cli()
