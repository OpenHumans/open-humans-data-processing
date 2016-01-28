"""
Create data set for Wild Life of Our Homes

Copyright (C) 2015 PersonalGenomes.org

This software is shared under the "MIT License" license (aka "Expat License"),
see LICENSE.TXT for full license text.
"""
import json
import os
import requests
import tempfile

from .files import get_remote_file, mv_tempfile_to_output


def create_wildlife_datafiles(files,
                              task_id=None,
                              update_url=None,
                              **kwargs):
    """
    Create datafiles from a set of Wild Life of Our Homes file links.

    Required arguments:
        files: dict containing filenames and URLs to the files
        filepath OR (s3_bucket_name and s3_key_name): (see below)

    Optional arguments:
        filedir: Local filepath, folder in which to place the resulting file.
        s3_bucket_name: S3 bucket to write resulting file.
        s3_key_dir: S3 key "directory" to write resulting file. The full S3 key
                    name will add a filename to the end of s3_key_dir.

    Either 'filedir' (and no S3 arguments), or both S3 arguments (and no
    'filedir') must be specified.
    """
    tempdir = tempfile.mkdtemp()
    temp_files = []
    data_files = []

    for filename in files:
        url = files[filename]
        filename = get_remote_file(url, tempdir)
        print filename
        temp_files += [{
            'temp_filename': filename,
            'tempdir': tempdir,
            'metadata': {
                'description': 'dunno',
                'tages': [],
            }
        }]

    print 'Finished creating all datasets locally.'

    for file_info in temp_files:
        print "File info: {}".format(str(file_info))
        filename = file_info['temp_filename']
        file_tempdir = file_info['tempdir']
        output_path = mv_tempfile_to_output(
            os.path.join(file_tempdir, filename), filename, **kwargs)
        if 's3_key_dir' in kwargs and 's3_bucket_name' in kwargs:
            data_files.append({
                's3_key': output_path,
                'metadata': file_info['metadata'],
            })
    os.rmdir(tempdir)

    print 'Finished moving all datasets to permanent storage.'

    if not (task_id and update_url):
        return

    task_data = {'task_id': task_id,
                 's3_keys': [df['s3_key'] for df in data_files],
                 'data_files': data_files}
    status_msg = ('Updating main site ({}) with completed files for task_id={}'
                  ' with task_data:\n{}'.format(
                      update_url, task_id, json.dumps(task_data)))
    print status_msg
    requests.post(update_url, data={'task_data': json.dumps(task_data)})
