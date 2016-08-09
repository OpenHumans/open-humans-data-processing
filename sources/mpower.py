r"""
mPower genotyping data extraction.

Copyright (C) 2016 PersonalGenomes.org

This software is shared under the "MIT License" license (aka "Expat License"),
see LICENSE.TXT for full license text.

May be used on the command line. For example, the following command:

    python -m sources.mpower ~/Downloads/mPower.zip files

Will assemble processed data sets in files/
"""

import os
import shutil
import sys
import zipfile

from base_source import BaseSource


class MPowerSource(BaseSource):
    """
    Create Open Humans Dataset from uploaded mPower data.
    """

    def __init__(self, username, **kwargs):
        self.username = username

        super(MPowerSource, self).__init__(**kwargs)

    def verify_mpower(self, input_filepath):
        """
        Create clean file in mPower format from downloaded version
        """
        if input_filepath.endswith('.zip'):
            zip_file = zipfile.ZipFile(input_filepath)

            file_list = [f for f in zip_file.namelist()
                         if not f.startswith('__MACOSX/')]

            for filename in file_list:
                if not filename.startswith('parkinson-'):
                    self.sentry_log('mPower file did not conform to expected '
                                    'format. Username: {}'.format(
                                        self.username))

                    raise ValueError(
                        'Found a filename that did not start with '
                        '"parkinson-": "{}"'.format(filename))
        else:
            raise ValueError('Input file is expected to be a ZIP archive')

    def create_datafiles(self):
        if self.file_url and not self.input_file:
            filename = self.get_remote_file(self.file_url)
            input_file = os.path.join(self.temp_directory, filename)
        elif self.input_file and not self.file_url:
            pass
        else:
            raise Exception('Run with either input_file, or file_url')

        self.verify_mpower(input_file)

        shutil.copyfile(input_file, os.path.join(self.temp_directory,
                                                 'mPower-Parkinsons.zip'))

        self.temp_files.append({
            'temp_filename': 'mPower-Parkinsons.zip',
            'metadata': {
                'description': 'mPower data, original format',
                'tags': ['mPower', 'CSV', 'JSON'],
            },
        })

    def cli(self):
        self.create_datafiles()

if __name__ == '__main__':
    if len(sys.argv) != 4:
        print ('Please specify a remote file URL, username, and target local '
               'directory.')

        sys.exit(1)

    mpower = MPowerSource(file_url=sys.argv[1],
                          username=sys.argv[2],
                          output_directory=sys.argv[3])
