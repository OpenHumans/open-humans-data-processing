"""
Fitbit data processing.

Copyright (C) 2016 PersonalGenomes.org

This software is shared under the "MIT License" license (aka "Expat License"),
see LICENSE.TXT for full license text.
"""

from __future__ import unicode_literals

import json
import logging
import os
import time
import urlparse

from collections import defaultdict

import arrow

from requests_respectful import (RespectfulRequester,
                                 RequestsRespectfulRateLimitedError)

from base_source import BaseSource
from models import CacheItem

logger = logging.getLogger(__name__)

if __name__ == '__main__':
    from utilities import init_db

    db = init_db()
else:
    from models import db  # noqa

redis_url = os.getenv('REDIS_URL', 'redis://localhost:6379')
url_object = urlparse.urlparse(redis_url)

logger.info('Connecting to redis at %s:%s',
            url_object.hostname,
            url_object.port)

RespectfulRequester.configure(
    redis={
        'host': url_object.hostname,
        'port': url_object.port,
        'password': url_object.password,
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
     'url': '/{user_id}/activities/heart/date/{start_date}/{end_date}.json',
     'period': 'month'},
    {'name': 'tracker-activity-calories',
     'url': '/{user_id}/activities/tracker/activityCalories/date/{start_date}/{end_date}.json',
     'period': 'year'},
    {'name': 'tracker-calories',
     'url': '/{user_id}/activities/tracker/calories/date/{start_date}/{end_date}.json',
     'period': 'year'},
    {'name': 'tracker-distance',
     'url': '/{user_id}/activities/tracker/distance/date/{start_date}/{end_date}.json',
     'period': 'year'},
    {'name': 'tracker-elevation',
     'url': '/{user_id}/activities/tracker/elevation/date/{start_date}/{end_date}.json',
     'period': 'year'},
    {'name': 'tracker-floors',
     'url': '/{user_id}/activities/tracker/floors/date/{start_date}/{end_date}.json',
     'period': 'year'},
    {'name': 'tracker-minutes-fairly-active',
     'url': '/{user_id}/activities/tracker/minutesFairlyActive/date/{start_date}/{end_date}.json',
     'period': 'year'},
    {'name': 'tracker-minutes-lightly-active',
     'url': '/{user_id}/activities/tracker/minutesLightlyActive/date/{start_date}/{end_date}.json',
     'period': 'year'},
    {'name': 'tracker-minutes-sedentary',
     'url': '/{user_id}/activities/tracker/minutesSedentary/date/{start_date}/{end_date}.json',
     'period': 'year'},
    {'name': 'tracker-minutes-very-active',
     'url': '/{user_id}/activities/tracker/minutesVeryActive/date/{start_date}/{end_date}.json',
     'period': 'year'},
    {'name': 'tracker-steps',
     'url': '/{user_id}/activities/tracker/steps/date/{start_date}/{end_date}.json',
     'period': 'year'},
    {'name': 'weight-log',
     'url': '/{user_id}/body/log/weight/date/{start_date}/{end_date}.json',
     'period': 'month'},
    {'name': 'weight',
     'url': '/{user_id}/body/weight/date/{start_date}/{end_date}.json',
     'period': 'year'},
    {'name': 'sleep-awakenings',
     'url': '/{user_id}/sleep/awakeningsCount/date/{start_date}/{end_date}.json',
     'period': 'year'},
    {'name': 'sleep-efficiency',
     'url': '/{user_id}/sleep/efficiency/date/{start_date}/{end_date}.json',
     'period': 'year'},
    {'name': 'sleep-minutes-after-wakeup',
     'url': '/{user_id}/sleep/minutesAfterWakeup/date/{start_date}/{end_date}.json',
     'period': 'year'},
    {'name': 'sleep-minutes',
     'url': '/{user_id}/sleep/minutesAsleep/date/{start_date}/{end_date}.json',
     'period': 'year'},
    {'name': 'awake-minutes',
     'url': '/{user_id}/sleep/minutesAwake/date/{start_date}/{end_date}.json',
     'period': 'year'},
    {'name': 'minutes-to-sleep',
     'url': '/{user_id}/sleep/minutesToFallAsleep/date/{start_date}/{end_date}.json',
     'period': 'year'},
    {'name': 'sleep-start-time',
     'url': '/{user_id}/sleep/startTime/date/{start_date}/{end_date}.json',
     'period': 'year'},
    {'name': 'time-in-bed',
     'url': '/{user_id}/sleep/timeInBed/date/{start_date}/{end_date}.json',
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

    Result is a dict of all of the fitbit data.
    """
    requests.register_realm('fitbit-{}'.format(open_humans_id),
                            max_requests=150, timespan=60)

    query_result = fitbit_query(access_token=access_token,
                                path='/-/profile.json',
                                open_humans_id=open_humans_id)

    # store the user ID since it's used in all future queries
    user_id = query_result['user']['encodedId']
    member_since = query_result['user']['memberSince']

    fitbit_data = defaultdict(list, {
        'profile': {
            'averageDailySteps': query_result['user']['averageDailySteps'],
            'encodedId': user_id,
            'height': query_result['user']['height'],
            'memberSince': member_since,
            'strideLengthRunning': query_result['user']['strideLengthRunning'],
            'strideLengthWalking': query_result['user']['strideLengthWalking'],
            'weight': query_result['user']['weight'],
        },
    })

    for url in [u for u in fitbit_urls if u['period'] is None]:
        if not user_id and 'profile' in fitbit_data:
            user_id = fitbit_data['profile']['user']['encodedId']

        query_result = fitbit_query(access_token=access_token,
                                    path=url['url'],
                                    parameters={'user_id': user_id},
                                    open_humans_id=open_humans_id)

        fitbit_data[url['name']] = query_result

    for url in [u for u in fitbit_urls if u['period'] == 'year']:
        start_year = arrow.get(member_since, 'YYYY-MM-DD').year
        current_year = arrow.get().year

        for year in xrange(start_year, current_year + 1):
            logger.info('retrieving %s: %s', url['name'], year)

            query_result = fitbit_query(
                access_token=access_token,
                path=url['url'],
                parameters={
                    'user_id': user_id,
                    'start_date': '{}-01-01'.format(year),
                    'end_date': '{}-12-31'.format(year),
                },
                open_humans_id=open_humans_id)

            fitbit_data[url['name']].append(query_result)

    for url in [u for u in fitbit_urls if u['period'] == 'month']:
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
            logger.info('retrieving %s: %s, %s', url['name'], year, month)

            day = arrow.get(year, month, 1).ceil('month').day

            query_result = fitbit_query(
                access_token=access_token,
                path=url['url'],
                parameters={
                    'user_id': user_id,
                    'start_date': '{}-{:02d}-01'.format(year, month),
                    'end_date': '{}-{:02d}-{}'.format(year, month, day),
                },
                open_humans_id=open_humans_id)

            fitbit_data[url['name']].append(query_result)

    # TODO: implement these once we're approved for Fitbit intraday access
    for url in [u for u in fitbit_urls if u['period'] == 'day']:
        pass

    return fitbit_data


class FitbitSource(BaseSource):
    """
    Create an Open Humans Dataset from Fitbit API data.
    """

    source = 'fitbit'

    def create_files(self):
        filename = 'fitbit-data.json'
        filepath = os.path.join(self.temp_directory, filename)

        self.temp_files.append({
            'temp_filename': filename,
            'metadata': {
                'description': ('Fitbit activity, health, and fitness data.'),
                'tags': ['weight', 'Fitbit', 'steps', 'activity'],
            }
        })

        try:
            fitbit_data = get_fitbit_data(self.access_token, self.oh_user_id)
        except RateLimitException:
            return {'countdown': 60}

        with open(filepath, 'w') as f:
            json.dump(fitbit_data, f, indent=2)

    def run_cli(self):
        while True:
            if (not self.should_update(self.get_current_files()) and
                    not self.force):
                return

            if not self.local:
                self.update_parameters()

            self.coerce_file()
            self.validate_parameters()

            result = self.create_files()

            if result:
                countdown = result['countdown']

                logger.info('Rate cap hit. Pausing %ds before resuming...',
                            countdown)

                time.sleep(countdown)
            else:
                break

            if not self.local:
                self.archive_files()
                self.update_open_humans()

        self.move_files()


if __name__ == '__main__':
    FitbitSource.cli()
