"""
Genome/Exome VCF data processing.

Copyright (C) 2016 PersonalGenomes.org

This software is shared under the "MIT License" license (aka "Expat License"),
see LICENSE.TXT for full license text.
"""
import bz2
import json
import os
import vcf

from base_source import BaseSource


class VCFDataSource(BaseSource):
    """
    Process user-contributed VCF data (uploaded files)

    Optional arguments:
        vcf_data: array with vcf file links and metadata
    """

    source = 'vcf_data'

    @staticmethod
    def verify_vcf(input_file):
        """
        Verify that this is a VCF file.
        """
        if input_file.endswith('.vcf.gz'):
            input_vcf = vcf.Reader(filename=input_file, compressed=True)
        elif input_file.endswith('.vcf.bz2'):
            vcf_file = bz2.BZ2File(input_file)
            input_vcf = vcf.Reader(vcf_file)
        elif input_file.endswith('.vcf'):
            input_vcf = vcf.Reader(filename=input_file)
        else:
            raise ValueError("Input filename doesn't match .vcf, .vcf.gz, "
                             'or .vcf.bz2')

        # Check that it can advance one record without error.
        input_vcf.next()

        return input_vcf.metadata

    def create_files(self):
        for vcf_data_item in self.vcf_data:
            filename = self.get_remote_file(vcf_data_item['vcf_file']['url'])
            input_file = os.path.join(self.temp_directory, filename)

            try:
                header_data = self.verify_vcf(input_file)
            except Exception as e:
                self.sentry_log(
                    'vcf_data: error in processing! File URL: {0}, '
                    'Error: "{2}"'.format(
                        vcf_data_item['vcf_file']['url'], e))

                continue

            metadata = {
                'description': 'User-contributed VCF data',
                'tags': ['vcf'],
                'vcf_source': vcf_data_item['vcf_source'],
            }

            if vcf_data_item['additional_notes']:
                metadata['user_notes'] = vcf_data_item['additional_notes']

            self.temp_files.append({
                'temp_filename': filename,
                'metadata': metadata,
            })

            # Create metadata file.
            base_filename = filename

            if filename.endswith('.gz'):
                base_filename = filename[0:-3]
            elif filename.endswith('.bz2'):
                base_filename = filename[0:-4]

            metadata_filename = base_filename + '.metadata.json'
            metadata_filepath = os.path.join(self.temp_directory,
                                             metadata_filename)

            with open(metadata_filepath, 'w') as f:
                json.dump(header_data, f)

            metadata = {
                'description': 'VCF file metadata',
                'tags': ['vcf']
            }

            self.temp_files.append({
                'temp_filename': metadata_filename,
                'metadata': metadata,
            })


if __name__ == '__main__':
    import click

    cli = VCFDataSource.make_cli()
    cli = click.option('--vcf-data')(cli)

    cli()
