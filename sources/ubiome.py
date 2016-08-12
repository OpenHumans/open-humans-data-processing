"""
uBiome fastq data extraction.

Copyright (C) 2016 PersonalGenomes.org

This software is shared under the "MIT License" license (aka "Expat License"),
see LICENSE.TXT for full license text.
"""

import os
import shutil
import zipfile

from cStringIO import StringIO

from base_source import BaseSource


class UBiomeSource(BaseSource):
    """
    Create Open Humans Dataset from uploaded uBiome data.

    Optional arguments:
        samples: JSON that describes the member's uBiome samples
    """

    def __init__(self, samples, **kwargs):
        self.samples = samples

        super(UBiomeSource, self).__init__(**kwargs)

    def verify_ubiome(self, input_filepath):
        """
        Verify that this is a uBiome file.
        """
        if input_filepath.endswith('.zip'):
            zip_file = zipfile.ZipFile(input_filepath)
            zip_files = self.filter_archive(zip_file)

            for filename in zip_files:
                if not filename.endswith('.fastq.gz'):
                    self.sentry_log(
                        'uBiome file did not conform to expected format.')

                    raise ValueError(
                        'Found a filename that did not end with ".fastq.gz": '
                        '"{}"'.format(filename))
        else:
            raise ValueError('Input file is expected to be a ZIP archive')

    def create_files(self):
        for sample in enumerate(self.samples):
            filename = self.get_remote_file(sample[1]['sequence_file']['url'])
            input_file = self.temp_join(filename)

            self.verify_ubiome(input_file)

            fastq_filename = 'uBiome-fastq{}.zip'.format(
                '-' + str(sample[0] + 1) if len(self.samples) > 1 else '')

            shutil.move(input_file,
                        os.path.join(self.temp_directory, fastq_filename))

            metadata = {
                'description': 'uBiome 16S FASTQ raw sequencing data.',
                'tags': ['fastq', 'uBiome', '16S']
            }

            if sample[1]['additional_notes']:
                metadata['user_notes'] = sample[1]['additional_notes']

            self.temp_files.append({
                'temp_filename': fastq_filename,
                'metadata': metadata,
            })

            taxonomy = StringIO(sample[1]['taxonomy'])
            taxonomy_filename = 'taxonomy{}.json'.format(
                '-' + str(sample[0] + 1) if len(self.samples) > 1 else '')

            shutil.copyfileobj(
                taxonomy,
                file(os.path.join(self.temp_directory, taxonomy_filename),
                     'w'))

            metadata = {
                'description': 'uBiome 16S taxonomy data, JSON format.',
                'tags': ['json', 'uBiome', '16S']
            }

            if sample[1]['additional_notes']:
                metadata['user_notes'] = sample[1]['additional_notes']

            self.temp_files.append({
                'temp_filename': taxonomy_filename,
                'metadata': metadata,
            })


if __name__ == '__main__':
    import click

    cli = UBiomeSource.make_cli()
    cli = click.option('--samples')(cli)

    cli()
