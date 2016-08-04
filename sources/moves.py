"""
Moves data processing.

Copyright (C) 2016 PersonalGenomes.org

This software is shared under the "MIT License" license (aka "Expat License"),
see LICENSE.TXT for full license text.

May be used on the command line from this project's base directory, e.g.

   foreman run python -m sources.moves <access token> files/

...where <access token> is the private token the Moves API has created that
grants permission to access a user's data. (Keep it safe!) This will assemble
a data set for the user in that directory:

   files/moves-storyline-data.tar.gz
"""

from __future__ import unicode_literals

from datetime import date, datetime

import json
import os
import sys
import tempfile
import time

import requests

from data_retrieval.files import mv_tempfile_to_output
from models import CacheItem

if __name__ == '__main__':
    from utilities import init_db

    db = init_db()
else:
    from models import db


def moves_query(access_token, path):
    """
    Query Moves API and return result.

    Result is a dict with the following keys:
        'response_json': data from the query JSON, or None if rate cap hit.
        'rate_cap_encountered': None, or True if rate cap hit.
    """
    headers = {'Authorization': 'Bearer %s' % access_token}
    data_url = 'https://api.moves-app.com/api/1.1{}'.format(path)
    data_key = '{}{}'.format(data_url, access_token)

    # Return dict. Either contains data, or indicates rate cap encountered.
    query_result = {
        'response_json': None,
        'rate_cap_encountered': None,
    }

    cached_response = (CacheItem.query
                       .filter_by(key=data_key)
                       .order_by(CacheItem.request_time.desc())
                       .first())

    if cached_response:
        query_result['response_json'] = cached_response.response
        return query_result

    data_response = requests.get(data_url, headers=headers)

    # If a rate cap is encountered, return a result reporting this.
    if data_response.status_code == 429:
        query_result['rate_cap_encountered'] = True
        return query_result

    query_result['response_json'] = data_response.json()

    db.session.add(CacheItem(data_key, query_result['response_json']))
    db.session.commit()

    return query_result


def get_full_storyline(access_token):
    """
    Iterate to get all items for a given access_token and path, return result.

    Result is a dict with the following keys:
        'all_data': data from all items, or None if rate cap hit.
        'rate_cap_encountered': None, or True if rate cap hit.
    """
    full_storyline_result = {
        'all_data': [],
        'rate_cap_encountered': None,
    }
    current_year = int(datetime.utcnow().strftime('%Y'))
    current_week = int(datetime.utcnow().strftime('%W'))
    in_user_range = True
    while in_user_range:
        query_result = moves_query(
            access_token=access_token,
            path='/user/storyline/daily/{0}-W{1}?trackPoints=true'.format(
                current_year,
                str(current_week).zfill(2)
            ))
        if query_result['rate_cap_encountered']:
            full_storyline_result['rate_cap_encountered'] = True
            return full_storyline_result
        week_data = query_result['response_json']
        if 'error' in week_data:
            in_user_range = False
        else:
            full_storyline_result['all_data'] = (
                week_data + full_storyline_result['all_data'])
        if current_week > 1:
            current_week = current_week - 1
        else:
            current_year = current_year - 1
            current_week = int(
                date(year=current_year, month=12, day=31).strftime('%W'))
    return full_storyline_result


def create_datafiles(access_token, task_id=None, update_url=None, **kwargs):
    """
    Create Open Humans Dataset from Moves API data

    Required arguments:
        access_token: Moves access token

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

    full_storyline_result = get_full_storyline(access_token)

    if full_storyline_result['rate_cap_encountered']:
        # If this is previously called and we got no new data this round,
        # double the wait period for resubmission.
        if 'return_status' in kwargs and not full_storyline_result['all_data']:
            countdown = 2 * kwargs['return_status']['countdown']
        else:
            countdown = 60
        return {'countdown': countdown}

    user_data = full_storyline_result['all_data']

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

    task_data = {'task_id': task_id, 'data_files': data_files}
    status_msg = ('Updating main site ({}) with completed files for task_id={}'
                  ' with task_data:\n{}'.format(
                      update_url, task_id, json.dumps(task_data)))
    print status_msg
    requests.post(update_url, json={'task_data': task_data})


if __name__ == '__main__':
    if len(sys.argv) != 3:
        print 'Please specify a token and directory.'
        sys.exit(1)

    while True:
        result = create_datafiles(*sys.argv[1:-1], filedir=sys.argv[-1])
        if result:
            countdown = result['countdown']
            print("Rate cap hit. Pausing {}s before resuming...".format(
                countdown))
            time.sleep(countdown)
        else:
            break
