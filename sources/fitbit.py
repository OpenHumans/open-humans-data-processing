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

import json
import os
import sys
import tempfile
import time
import urlparse

import arrow

from requests_respectful import (RespectfulRequester,
                                 RequestsRespectfulRateLimitedError)

from data_retrieval.files import mv_tempfile_to_output
from models import CacheItem
from utilities import get_fresh_token

if __name__ == '__main__':
    from utilities import init_db

    db = init_db()
else:
    from models import db  # noqa

redis_url = os.getenv('REDIS_URL', 'redis://localhost:6379')
url = urlparse.urlparse(redis_url)

print 'Connecting to redis at {}:{}, {}'.format(url.hostname, url.port,
                                                url.password)

RespectfulRequester.configure(
    redis={
        'host': url.hostname,
        'port': url.port,
        'password': url.password,
        'database': 0,
    },
    safety_threshold=5)

requests = RespectfulRequester()
requests.register_realm('fitbit', max_requests=3600, timespan=60)

fitbit_urls = [
    # Requires the 'settings' scope, which we haven't asked for
    # {'name': 'devices', 'url': '/-/devices.json', 'period': None},

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
    {'name': 'intraday-heart',
     'url': '/-/activities/heart/date/{date}/1d/1sec.json',
     'period': 'day'},
    {'name': 'intraday-steps',
     'url': '/-/activities/tracker/steps/date/{date}/1d/1min.json',
     'period': 'day'},
]


class RateLimitException(Exception):
    """
    An exception that is raised if we reach a request rate cap.
    """

    # TODO: add the source of the rate limit we hit for logging (fitit,
    # internal global fitbit, internal user-specific fitbit)

    pass


def fitbit_query(access_token, path, open_humans_id, parameters=None):
    """
    Query Fitbit API and return result.
    """
    if not parameters:
        parameters = {}

    headers = {
        'Authorization': 'Bearer %s' % access_token,
        # Required for American units (miles, pounds)
        'Accept-Language': 'en_US',
    }

    path = path.format(**parameters)
    data_url = 'https://api.fitbit.com/1/user{}'.format(path)
    data_key = '{}-{}'.format(data_url, open_humans_id)

    cached_response = (CacheItem.query
                       .filter_by(key=data_key)
                       .order_by(CacheItem.request_time.desc())
                       .first())

    if cached_response:
        return cached_response.response

    try:
        data_response = requests.get(
            data_url,
            headers=headers,
            realms=['fitbit', 'fitbit-{}'.format(open_humans_id)])
    except RequestsRespectfulRateLimitedError:
        raise RateLimitException()

    # If a rate cap is encountered, return a result reporting this.
    if data_response.status_code == 429:
        raise RateLimitException()

    query_result = data_response.json()

    if ('errors' in query_result and 'success' in query_result
            and not query_result['success']):
        raise Exception(query_result['errors'])

    db.session.add(CacheItem(data_key, query_result))
    db.session.commit()

    return query_result


def get_fitbit_data(access_token, open_humans_id):
    """
    Iterate to get all items for a given access_token and path, return result.

    Result is a dict with the following keys:
        'all_data': data from all items, or None if rate cap hit.
        'rate_cap_encountered': None, or True if rate cap hit.
    """
    requests.register_realm('fitbit-{}'.format(open_humans_id),
                            max_requests=3600, timespan=60)

    query_result = fitbit_query(access_token=access_token,
                                path='/-/profile.json',
                                open_humans_id=open_humans_id)

    # store the user ID since it's used in all future queries
    user_id = query_result['user']['encodedId']
    member_since = query_result['user']['memberSince']

    fitbit_data = {
        'profile': {
            'averageDailySteps': query_result['user']['averageDailySteps'],
            'encodedId': user_id,
            'height': query_result['user']['height'],
            'memberSince': member_since,
            'strideLengthRunning': query_result['user']['strideLengthRunning'],
            'strideLengthWalking': query_result['user']['strideLengthWalking'],
            'weight': query_result['user']['weight'],
        },
    }

    try:
        for url in [url for url in fitbit_urls if url['period'] is None]:
            if not user_id and 'profile' in fitbit_data:
                user_id = fitbit_data['profile']['user']['encodedId']

            query_result = fitbit_query(access_token=access_token,
                                        path=url['url'],
                                        parameters={'user_id': user_id},
                                        open_humans_id=open_humans_id)

            fitbit_data[url['name']] = query_result

        for url in [url for url in fitbit_urls if url['period'] == 'year']:
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
                    },
                    open_humans_id=open_humans_id)

                fitbit_data[url['name']].append(query_result)

        for url in [url for url in fitbit_urls if url['period'] == 'month']:
            start_date = arrow.get(member_since, 'YYYY-MM-DD')
            today_date = arrow.get()

            dates = []

            for year in xrange(start_date.year, today_date.year + 1):
                start_month = 1
                end_month = 12

                if year == start_date.year == today_date.year:
                    start_month = start_date.month
                    end_month = today_date.month
                elif year == start_date.year:
                    start_month = start_date.month
                elif year == today_date.year:
                    end_month = today_date.month

                dates += [(year, month) for month
                          in range(start_month, end_month + 1)]

            for year, month in dates:
                print 'retrieving {}: {}, {}'.format(url['name'], year, month)

                fitbit_data[url['name']] = []

                query_result = fitbit_query(
                    access_token=access_token,
                    path=url['url'],
                    parameters={
                        'user_id': user_id,
                        'date': '{}-{:02d}-01'.format(year, month),
                    },
                    open_humans_id=open_humans_id)

                fitbit_data[url['name']].append(query_result)

        # TODO: implement these once we're approved for Fitbit intraday access
        for url in [url for url in fitbit_urls if url['period'] == 'day']:
            pass
    except RateLimitException:
        return {
            'all_data': fitbit_data,
            'rate_cap_encountered': True,
        }

    return {
        'all_data': fitbit_data,
        'rate_cap_encountered': False,
    }


def create_datafiles(user_id, task_id=None, update_url=None, **kwargs):
    """
    Create Open Humans Dataset from Fitbit API data

    Required arguments:
        user_id: Open Humans user ID

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

    access_token = get_fresh_token(user_id, provider='fitbit')

    fitbit_data = get_fitbit_data(access_token, user_id)

    if fitbit_data['rate_cap_encountered']:
        return {'countdown': 60}

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
        print 'Please specify a user ID and directory.'

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
