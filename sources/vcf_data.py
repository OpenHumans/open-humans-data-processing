r"""
Genome/Exome VCF data processing.

Copyright (C) 2016 PersonalGenomes.org

This software is shared under the "MIT License" license (aka "Expat License"),
see LICENSE.TXT for full license text.
"""
import bz2
import gzip
import json
import os
import shutil
import tempfile
import vcf

import requests

from boto.s3.connection import S3Connection

from data_retrieval.files import get_remote_file, mv_tempfile_to_output
from data_retrieval.sort_vcf import sort_vcf


def s3_connection():
    """
    Get an S3 connection using environment variables.
    """
    key = os.getenv('AWS_ACCESS_KEY_ID')
    secret = os.getenv('AWS_SECRET_ACCESS_KEY')

    if not (key and secret):
        raise Exception('You must specify AWS credentials.')

    return S3Connection(key, secret)


def verify_vcf(input_filepath, sentry=None, username=None):
    """
    Verify that this is a VCF file.
    """
    if input_filepath.endswith('.vcf.gz'):
        input_vcf = vcf.Reader(filename=input_filepath, compressed=True)
    elif input_filepath.endswith('.vcf.bz2'):
        vcf_file = bz2.BZ2File(input_filepath)
        input_vcf = vcf.Reader(vcf_file)
    elif input_filepath.endswith('.vcf'):
        input_vcf = vcf.Reader(filename=input_filepath)
    else:
        raise ValueError("Input filename doesn't match .vcf, .vcf.gz, "
                         'or .vcf.bz2')
    # Check that it can advance one record without error.
    input_vcf.next()
    return input_vcf.metadata


def standardize_vcf(input_filepath, base_filename, tempdir):
    """
    Standardize VCF data to have natural chromosome order and gzip compression.
    """
    if input_filepath.endswith('.gz'):
        inputfile = gzip.open(input_filepath)
    elif input_filepath.endswith('.bz2'):
        inputfile = bz2.BZ2File(input_filepath)
    else:
        inputfile = open(input_filepath)
    inputfile.seek(0)
    sorted_file = sort_vcf(inputfile, tempdir=tempdir)
    inputfile.close()
    processed_filename = base_filename[0:-4] + '.sorted.vcf.gz'
    with gzip.open(os.path.join(tempdir, processed_filename), 'w') as vcf_file:
        sorted_file.seek(0)
        shutil.copyfileobj(sorted_file, vcf_file)
    sorted_file.close()
    return processed_filename


def create_datafiles(username, vcf_data=None, task_id=None, update_url=None,
                     sentry=None, **kwargs):
    """
    Process user-contributed VCF data (uploaded files)

    Optional arguments:
        vcf_data: array with vcf file links and metadata
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

    if not vcf_data:
        raise Exception('`vcf_data` parameter missing')

    for vcf_data_item in enumerate(vcf_data):
        filename = get_remote_file(
            vcf_data_item[1]['vcf_file']['url'], tempdir)
        input_file = os.path.join(tempdir, filename)

        try:
            header_data = verify_vcf(input_file, sentry, username)
        except Exception, e:
            error_msg = (
                'vcf_data: error in processing! '
                'File URL: {0}, Username: {1}, Error: "{2}"'.format(
                    vcf_data_item[1]['vcf_file']['url'], username, e))
            if sentry:
                sentry.captureMessage(error_msg)
            continue

        base_filename = filename
        if filename.endswith('.gz'):
            base_filename = filename[0:-3]
        elif filename.endswith('.bz2'):
            base_filename = filename[0:-4]
        vcf_processed = standardize_vcf(input_file, base_filename, tempdir)

        metadata = {
            'description': 'User-contributed VCF data, processed to '
            'standardize chromosome ordering.',
            'tags': ['vcf']
        }
        if vcf_data_item[1]['additional_notes']:
            metadata['user_notes'] = vcf_data_item[1]['additional_notes']
        temp_files.append({
            'temp_filename': vcf_processed,
            'tempdir': tempdir,
            'metadata': metadata,
        })

        # Create metadata file.
        metadata_filename = base_filename + '.metadata.json'
        metadata_filepath = os.path.join(tempdir, metadata_filename)
        with open(metadata_filepath, 'w') as f:
            json.dump(header_data, f)
        metadata = {
            'description': 'VCF file metadata',
            'tags': ['json']
        }
        temp_files.append({
            'temp_filename': metadata_filename,
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

    shutil.rmtree(tempdir)

    if not (task_id and update_url):
        return

    task_data = {
        'task_id': task_id,
        's3_keys': [data_file['s3_key'] for data_file in data_files],
        'data_files': data_files
    }

    requests.post(update_url, json={'task_data': task_data})


# if __name__ == '__main__':
#     if len(sys.argv) != 4:
#         print ('Please specify a remote file URL, target local directory, '
#                'and username.')

#         sys.exit(1)

#     create_datafiles(input_file=sys.argv[1], filedir=sys.argv[2],
#                      username=sys.argv[3])
