"""
Fitbit data processing.

Copyright (C) 2016 PersonalGenomes.org

This software is shared under the "MIT License" license (aka "Expat License"),
see LICENSE.TXT for full license text.

May be used on the command line from this project's base directory, e.g.

   foreman run python -m sources.fitbit <access token> files/

...where <access token> is the private token the Fitbit API has created that
grants permission to access a user's data. (Keep it safe!) This will assemble
a data set for the user in that directory:

   files/fitbit-storyline-data.tar.gz
"""

from __future__ import unicode_literals

# from datetime import date, datetime

import json
import os
import sys
import tempfile
import time

import arrow
import requests

from data_retrieval.files import mv_tempfile_to_output
from models import CacheItem

if __name__ == '__main__':
    from utilities import init_db

    db = init_db()
else:
    from models import db  # noqa

fitbit_urls = [
    # general user data
    {'name': 'profile', 'url': '/-/profile.json', 'period': None},

    {'name': 'devices', 'url': '/-/devices.json', 'period': None},

    {'name': 'activities-overview',
     'url': '/{user_id}/activities.json',
     'period': None},

    # interday timeline data
    {'name': 'heart',
     'url': '/{user_id}/activities/heart/date/{date}/1m.json',
     'period': 'month'},
    {'name': 'tracker-activity-calories',
     'url': '/{user_id}/activities/tracker/activityCalories/date/{date}/1y.json',
     'period': 'year'},
    {'name': 'tracker-calories',
     'url': '/{user_id}/activities/tracker/calories/date/{date}/1y.json',
     'period': 'year'},
    {'name': 'tracker-distance',
     'url': '/{user_id}/activities/tracker/distance/date/{date}/1y.json',
     'period': 'year'},
    {'name': 'tracker-elevation',
     'url': '/{user_id}/activities/tracker/elevation/date/{date}/1y.json',
     'period': 'year'},
    {'name': 'tracker-floors',
     'url': '/{user_id}/activities/tracker/floors/date/{date}/1y.json',
     'period': 'year'},
    {'name': 'tracker-minutes-fairly-active',
     'url': '/{user_id}/activities/tracker/minutesFairlyActive/date/{date}/1y.json',
     'period': 'year'},
    {'name': 'tracker-minutes-lightly-active',
     'url': '/{user_id}/activities/tracker/minutesLightlyActive/date/{date}/1y.json',
     'period': 'year'},
    {'name': 'tracker-minutes-sedentary',
     'url': '/{user_id}/activities/tracker/minutesSedentary/date/{date}/1y.json',
     'period': 'year'},
    {'name': 'tracker-minutes-very-active',
     'url': '/{user_id}/activities/tracker/minutesVeryActive/date/{date}/1y.json',
     'period': 'year'},
    {'name': 'tracker-steps',
     'url': '/{user_id}/activities/tracker/steps/date/{date}/1y.json',
     'period': 'year'},
    {'name': 'weight-log',
     'url': '/{user_id}/body/log/weight/date/{date}/1m.json',
     'period': 'month'},
    {'name': 'weight',
     'url': '/{user_id}/body/weight/date/{date}/1y.json',
     'period': 'year'},
    {'name': 'sleep-awakenings',
     'url': '/{user_id}/sleep/awakeningsCount/date/{date}/1y.json',
     'period': 'year'},
    {'name': 'sleep-efficiency',
     'url': '/{user_id}/sleep/efficiency/date/{date}/1y.json',
     'period': 'year'},
    {'name': 'sleep-minutes-after-wakeup',
     'url': '/{user_id}/sleep/minutesAfterWakeup/date/{date}/1y.json',
     'period': 'year'},
    {'name': 'sleep-minutes',
     'url': '/{user_id}/sleep/minutesAsleep/date/{date}/1y.json',
     'period': 'year'},
    {'name': 'awake-minutes',
     'url': '/{user_id}/sleep/minutesAwake/date/{date}/1y.json',
     'period': 'year'},
    {'name': 'minutes-to-sleep',
     'url': '/{user_id}/sleep/minutesToFallAsleep/date/{date}/1y.json',
     'period': 'year'},
    {'name': 'sleep-start-time',
     'url': '/{user_id}/sleep/startTime/date/{date}/1y.json',
     'period': 'year'},
    {'name': 'time-in-bed',
     'url': '/{user_id}/sleep/timeInBed/date/{date}/1y.json',
     'period': 'year'},

    # intraday timeline data
    {'url': '/-/activities/heart/date/{date}/1d/1sec.json', 'period': 'day'},
    {'url': '/-/activities/tracker/steps/date/{date}/1d/1min.json', 'period': 'day'},
]


def fitbit_query(access_token, path, parameters=None):
    """
    Query Fitbit API and return result.

    Result is a dict with the following keys:
        'response_json': data from the query JSON, or None if rate cap hit.
        'rate_cap_encountered': None, or True if rate cap hit.
    """
    if not parameters:
        parameters = {}

    headers = {'Authorization': 'Bearer %s' % access_token}

    path = path.format(**parameters)
    data_url = 'https://api.fitbit.com/1/user{}'.format(path)
    data_key = '{}{}'.format(data_url, access_token)

    # TODO: raise a RateCapEncountered exception instead of passing it back in
    # the object (easier to consume)

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


def get_fitbit_data(access_token):
    """
    Iterate to get all items for a given access_token and path, return result.

    Result is a dict with the following keys:
        'all_data': data from all items, or None if rate cap hit.
        'rate_cap_encountered': None, or True if rate cap hit.
    """
    fitbit_data = {}
    user_id = None

    try:
        for url in [url for url in fitbit_urls if url['period'] is None]:
            if not user_id and 'profile' in fitbit_data:
                user_id = fitbit_data['profile']['user']['encodedId']

            query_result = fitbit_query(access_token=access_token,
                                        path=url['url'],
                                        parameters={'user_id': user_id})

            fitbit_data[url['name']] = query_result['response_json']

        for url in [url for url in fitbit_urls if url['period'] == 'year']:
            member_since = fitbit_data['profile']['user']['memberSince']
            start_year = arrow.get(member_since, 'YYYY-MM-DD').year
            current_year = arrow.get().year

            for year in xrange(start_year, current_year + 1):
                print 'retrieving {}: {}'.format(url['name'], year)

                fitbit_data[url['name']] = []

                query_result = fitbit_query(
                    access_token=access_token,
                    path=url['url'],
                    parameters={
                        'user_id': user_id,
                        'date': '{}-01-01'.format(year),
                    })

                fitbit_data[url['name']].append(query_result)

        # TODO
        for url in [url for url in fitbit_urls if url['period'] == 'month']:
            print url

        # TODO
        for url in [url for url in fitbit_urls if url['period'] == 'day']:
            print url
    except:  # TODO: RateCapEncountered
        return {
            'all_data': fitbit_data,
            'rate_cap_encountered': True,
        }

    return {
        'all_data': fitbit_data,
        'rate_cap_encountered': False,
    }


def create_datafiles(access_token, task_id=None, update_url=None, **kwargs):
    """
    Create Open Humans Dataset from Fitbit API data

    Required arguments:
        access_token: Fitbit access token

    Optional arguments:
        filedir: Local filepath, folder in which to place the resulting file.
        s3_bucket_name: S3 bucket to write resulting file.
        s3_key_dir: S3 key "directory" to write resulting file. The full S3 key
                    name will add a filename to the end of s3_key_dir.

    Either 'filedir' (and no S3 arguments), or both S3 arguments (and no
    'filedir') must be specified.
    """
    tempdir = tempfile.mkdtemp()
    filename = 'fitbit-data.json'
    filepath = os.path.join(tempdir, filename)
    data_files = []

    temp_files = [{
        'temp_filename': filename,
        'tempdir': tempdir,
        'metadata': {
            'description': ('Fitbit activity, health, and fitness data.'),
            'tags': ['weight', 'Fitbit', 'steps', 'activity'],
        }
    }]

    fitbit_data = get_fitbit_data(access_token)

    if fitbit_data['rate_cap_encountered']:
        # If this is previously called and we got no new data this round,
        # double the wait period for resubmission.
        if 'return_status' in kwargs and not fitbit_data['all_data']:
            countdown = 2 * kwargs['return_status']['countdown']
        else:
            countdown = 60

        return {'countdown': countdown}

    user_data = fitbit_data['all_data']

    with open(filepath, 'w') as f:
        json.dump(user_data, f, indent=2)

    print 'Finished creating Fitbit dataset locally.'

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

    print 'Finished moving Fitbit data to permanent storage.'

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


def cli_get_data():
    if len(sys.argv) != 3:
        print 'Please specify a token and directory.'

        sys.exit(1)

    while True:
        result = create_datafiles(*sys.argv[1:-1], filedir=sys.argv[-1])

        if result:
            countdown = result['countdown']

            print('Rate cap hit. Pausing {}s before resuming...'.format(
                countdown))

            time.sleep(countdown)
        else:
            break


if __name__ == '__main__':
    cli_get_data()
