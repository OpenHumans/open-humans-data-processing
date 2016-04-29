"""
Create data files from a user's GoViral data.

Copyright (C) 2016 PersonalGenomes.org

This software is shared under the "MIT License" license (aka "Expat License"),
see LICENSE.TXT for full license text.
"""

import json
import os
import sys
import tempfile

import requests

from data_retrieval.files import mv_tempfile_to_output

GO_VIRAL_DATA_URL = 'https://www.goviralstudy.com/participants/{}/data'


def get_go_viral_data(access_token, go_viral_id, sentry=None):
    """
    Retrieve GoViral data from the API for a given user.
    """
    request = requests.get(GO_VIRAL_DATA_URL.format(go_viral_id), params={
        'access_token': access_token
    })

    if request.status_code != 200:
        msg = 'GoViral website not permitting any access! Bad access token?'
        print msg
        if sentry:
            sentry.captureMessage(msg)
        return None

    request_data = request.json()
    if 'code' in request_data and request_data['code'] == 'PERMISSION_DENIED':
        msg = 'Data access denied by GoViral. User: {}'.format(go_viral_id)
        print msg
        if sentry:
            sentry.captureMessage(msg)
        return None

    data = request.json()
    for item in data.keys():
        if not data[item]:
            data.pop(item)
    if not data:
        return None

    return data


def handle_go_viral_data(data, tempdir):
    json_filename = 'GoViral-sickness-data.json'
    json_filepath = os.path.join(tempdir, json_filename)
    with open(json_filepath, 'w') as f:
        json.dump(data, f, indent=2, sort_keys=True)
    return [{
        'temp_filename': json_filename,
        'tempdir': tempdir,
        'metadata': {
            'description': ('GoViral annual surveys, and sickness reports with'
                            ' viral lab test results (if available)'),
            'tags': ['viral', 'survey', 'GoViral', 'json'],
        }
    }]


def create_datafiles(access_token, go_viral_id, task_id=None, update_url=None,
                     sentry=None, **kwargs):
    """
    Create a GoViral dataset for the given ID.

    Required arguments:
        access_token: the management access token for GoViral
        go_viral_id: the user's GoViral ID

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

    print 'Fetching GoViral data.'
    data_go_viral = get_go_viral_data(access_token, go_viral_id, sentry=sentry)

    # Don't create a file if there's no data from GoViral
    if not data_go_viral:
        return

    temp_files += handle_go_viral_data(data=data_go_viral, tempdir=tempdir)

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
    requests.post(update_url, json={'task_data': task_data})


if __name__ == '__main__':
    if len(sys.argv) != 4:
        print 'Please specify a token, ID, and directory.'

        sys.exit(1)

    create_datafiles(*sys.argv[1:-1], filedir=sys.argv[3])
