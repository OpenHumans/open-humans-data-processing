"""
Moves data processing.

Copyright (C) 2016 PersonalGenomes.org

This software is shared under the "MIT License" license (aka "Expat License"),
see LICENSE.TXT for full license text.

May be used on the command line from this project's base directory, e.g.

   python -m sources.moves [accesstoken] files

...where [accesstoken] is the private token the Moves API has created that
grants permission to access a user's data. (Keep it safe!) This will assemble
a data set for the user in that directory:

   files/moves-storyline-data.tar.gz
"""

from __future__ import unicode_literals

from datetime import date, datetime
import json
import os
import sys
import time
import tempfile

import requests

from data_retrieval.files import mv_tempfile_to_output

# We may encounter a "too many requests" error if more than one process is
# attempting to query Moves data. If this is encountered, the process is
# instructed to wait a random number of minutes, between MIN_WAIT_MINUTES
# and MAX_WAIT_MINUTES, before trying again.
MIN_WAIT_MINUTES = 1
MAX_WAIT_MINUTES = 6

BACKGROUND_DATA_KEYS = ['timestamp', 'steps', 'calories_burned', 'source']
FITNESS_SUMMARY_KEYS = ['type', 'equipment', 'start_time', 'utc_offset',
                        'total_distance', 'duration', 'total_calories',
                        'climb', 'source']
FITNESS_PATH_KEYS = ['latitude', 'longitude', 'altitude', 'timestamp', 'type']

PAGESIZE = '10000'


def moves_query(access_token, path, retries=0):
    """
    Query Moves API and return data.
    """
    headers = {'Authorization': 'Bearer %s' % access_token}
    data_url = 'https://api.moves-app.com/api/1.1{}'.format(path)
    data_response = requests.get(data_url, headers=headers)

    # The docs imply rate cap applies to the app, in which case the following
    # is very likely to occur. We should use a single worker Moves queue to
    # avoids more than one worker being tied up with waiting this way.
    if data_response.status_code == 429:
        print "{}: Moves rate cap encountered. Waiting 1 minute...".format(
            datetime.utcnow().strftime('%Y%m%dT%H%M%S'))
        if retries >= 60:
            raise RuntimeError('Moves import: rate cap errors! '
                               'Retries still failing after 60 attempts.')
        retries += 1
        time.sleep(60)
        return moves_query(access_token, path, retries=retries)
    data = data_response.json()
    return data


def get_full_storyline(access_token):
    """
    Iterate to get all items for a given access_token and path.

    RunKeeper uses the same pages format for items in various places.
    """
    current_year = int(datetime.utcnow().strftime('%Y'))
    current_week = int(datetime.utcnow().strftime('%W'))
    in_user_range = True
    all_data = []
    while in_user_range:
        week_data = moves_query(
            access_token=access_token,
            path='/user/storyline/daily/{0}-W{1}?trackPoints=true'.format(
                current_year,
                str(current_week).zfill(2)
            ))
        if 'error' in week_data:
            in_user_range = False
        else:
            all_data = week_data + all_data
        if current_week > 1:
            current_week = current_week - 1
        else:
            current_year = current_year - 1
            current_week = int(
                date(year=current_year, month=12, day=31).strftime('%W'))
    return all_data


def create_datafiles(access_token, task_id=None, update_url=None, **kwargs):
    """
    Create Open Humans Dataset from RunKeeper API data

    Required arguments:
        access_token: RunKeeper access token

    Optional arguments:
        filedir: Local filepath, folder in which to place the resulting file.
        s3_bucket_name: S3 bucket to write resulting file.
        s3_key_dir: S3 key "directory" to write resulting file. The full S3 key
                    name will add a filename to the end of s3_key_dir.

    Either 'filedir' (and no S3 arguments), or both S3 arguments (and no
    'filedir') must be specified.
    """
    tempdir = tempfile.mkdtemp()
    filename = 'moves-storyline-data.json'
    filepath = os.path.join(tempdir, filename)
    temp_files = [{
        'temp_filename': filename,
        'tempdir': tempdir,
        'metadata': {
            'description': ('Moves GPS maps, locations, and steps data.'),
            'tags': ['GPS', 'Moves', 'steps'],
        }
    }]
    data_files = []

    user_data = get_full_storyline(access_token)

    with open(filepath, 'w') as f:
        json.dump(user_data, f, indent=2)

    print 'Finished creating Moves dataset locally.'

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

    print 'Finished moving Moves data to permanent storage.'

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
    if len(sys.argv) != 3:
        print 'Please specify a token and directory.'
        sys.exit(1)

    create_datafiles(*sys.argv[1:-1], filedir=sys.argv[-1])
