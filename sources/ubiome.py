r"""
uBiome fastq data extraction.

Copyright (C) 2016 PersonalGenomes.org

This software is shared under the "MIT License" license (aka "Expat License"),
see LICENSE.TXT for full license text.

May be used on the command line. For example, the following command:

    python -m sources.ubiome ~/Downloads/uBiome.zip files

Will assemble processed data sets in files/
"""

import os
import shutil
import tempfile
import zipfile

from cStringIO import StringIO

import requests

from data_retrieval.files import get_remote_file, mv_tempfile_to_output


def verify_ubiome(input_filepath, sentry=None, username=None):
    """
    Verify that this is a uBiome file.
    """
    if input_filepath.endswith('.zip'):
        zip_file = zipfile.ZipFile(input_filepath)

        file_list = [f for f in zip_file.namelist()
                     if not f.startswith('__MACOSX/')]

        for filename in file_list:
            if not filename.endswith('.fastq.gz'):
                if sentry:
                    sentry_msg = ('uBiome file did not conform to expected '
                                  'format.')

                    if username:
                        sentry_msg += ' Username: {}'.format(username)

                    sentry.captureMessage(sentry_msg)

                raise ValueError(
                    'Found a filename that did not end with ".fastq.gz": '
                    '"{}"'.format(filename))
    else:
        raise ValueError('Input file is expected to be a ZIP archive')


def create_datafiles(username, samples=None, task_id=None, update_url=None,
                     sentry=None, **kwargs):
    """
    Create Open Humans Dataset from uploaded uBiome data.

    Optional arguments:
        samples: path to an online copy of the input file
        filedir: Local filepath, folder in which to place the resulting file.
        s3_bucket_name: S3 bucket to write resulting file.
        s3_key_dir: S3 key "directory" to write resulting file. The full S3 key
                    name will add a filename to the end of s3_key_dir.

    For output: iither 'filedir' (and no S3 arguments), or both
    's3_bucket_name' and 's3_key_dir' (and no 'filedir') must be specified.
    """
    tempdir = tempfile.mkdtemp()
    temp_files = []
    data_files = []

    if not samples:
        raise Exception('`samples` parameter missing')

    for sample in enumerate(samples):
        filename = get_remote_file(sample[1]['sequence_file']['url'], tempdir)
        input_file = os.path.join(tempdir, filename)

        verify_ubiome(input_file, sentry, username)

        fastq_filename = 'uBiome-fastq{}.zip'.format(
            '-' + str(sample[0] + 1) if len(samples) > 1 else '')
        shutil.move(input_file, os.path.join(tempdir, fastq_filename))
        metadata = {
            'description': 'uBiome 16S FASTQ raw sequencing data.',
            'tags': ['fastq', 'uBiome', '16S']
        }
        if sample[1]['additional_notes']:
            metadata['user_notes'] = sample[1]['additional_notes']
        temp_files.append({
            'temp_filename': fastq_filename,
            'tempdir': tempdir,
            'metadata': metadata,
        })

        taxonomy = StringIO(sample[1]['taxonomy'])
        taxonomy_filename = 'taxonomy{}.json'.format(
            '-' + str(sample[0] + 1) if len(samples) > 1 else '')
        shutil.copyfileobj(taxonomy,
                           file(os.path.join(tempdir, taxonomy_filename), 'w'))
        metadata = {
            'description': 'uBiome 16S taxonomy data, JSON format.',
            'tags': ['json', 'uBiome', '16S']
        }
        if sample[1]['additional_notes']:
            metadata['user_notes'] = sample[1]['additional_notes']
        temp_files.append({
            'temp_filename': taxonomy_filename,
            'tempdir': tempdir,
            'metadata': metadata,
        })

    print 'Finished creating all datasets locally.'

    for file_info in temp_files:
        print 'File info: {}'.format(str(file_info))

        filename = file_info['temp_filename']
        file_tempdir = file_info['tempdir']

        output_path = mv_tempfile_to_output(
            os.path.join(file_tempdir, filename), filename, **kwargs)

        if 's3_key_dir' in kwargs and 's3_bucket_name' in kwargs:
            data_files.append({
                's3_key': output_path,
                'metadata': file_info['metadata'],
            })

    print 'Finished moving all datasets to permanent storage.'

    os.rmdir(tempdir)

    if not (task_id and update_url):
        return

    task_data = {'task_id': task_id, 'data_files': data_files}

    requests.post(update_url, json={'task_data': task_data})


# if __name__ == '__main__':
#     if len(sys.argv) != 4:
#         print ('Please specify a remote file URL, target local directory, '
#                'and username.')

#         sys.exit(1)

#     create_datafiles(input_file=sys.argv[1], filedir=sys.argv[2],
#                      username=sys.argv[3])
