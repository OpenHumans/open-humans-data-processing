"""
Fitbit data processing.

Copyright (C) 2016 PersonalGenomes.org

This software is shared under the "MIT License" license (aka "Expat License"),
see LICENSE.TXT for full license text.
"""

from __future__ import unicode_literals

from datetime import timedelta
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
requests.register_realm('fitbit', max_requests=3600, timespan=3600)
requests.update_realm('fitbit', max_requests=3600, timespan=3600)

# Only use cached data if cached less than CACHE_MAX before now.
# Cache target dates older than CACHE_MIN before now.
# Based on these settings 23 URLs won't be cached, and full data retrieval
# requires around 6 to 7 hours per year of data.
CACHE_MAX = timedelta(weeks=1)
CACHE_MIN = timedelta(days=1)

# Use stored data if older than STORAGE_MIN relative to guessed storage date.
# Refreshing stored data rejects ~85 URLs, but the hourly API cap is unlikely
# to be hit unless more than a couple weeks have passed since previous storage.
STORAGE_MIN = timedelta(weeks=4)

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
     'url': '/-/activities/steps/date/{date}/1d/1min.json',
     'period': 'day'},
]


class RateLimitException(Exception):
    """
    An exception that is raised if we reach a request rate cap.
    """

    # TODO: add the source of the rate limit we hit for logging (fitit,
    # internal global fitbit, internal user-specific fitbit)

    pass


def fitbit_query(access_token, path, open_humans_id, parameters=None,
                 target_date=None):
    """
    Query Fitbit API and return result.

    Cache queries if the date for target data is greater than CACHE_MIN
    before now. Use cached queries if the cache time was less than CACHE_MAX
    before now.

    If no date is associated with target data, don't use query caching.
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
        cache_time = arrow.get(cached_response.request_time)
        if arrow.get() - cache_time <= CACHE_MAX:
            # Extra check -- data shouldn't have been cached in first place.
            if target_date and cache_time - target_date > CACHE_MIN:
                logging.debug('Loading cache for {}, cache time {} vs target date {} less than '
                              'CACHE_MIN'.format(data_key, cache_time, target_date))
                return cached_response.response
            else:
                logging.debug('Rejecting cache for {}, data should not have '
                              'been cached.'.format(data_key))
        else:
            logging.debug('Rejecting cache for {}, cache date more than '
                          'CACHE_MAX'.format(data_key))

    try:
        data_response = requests.get(
            data_url,
            headers=headers,
            realms=['fitbit', 'fitbit-{}'.format(open_humans_id)])
    except RequestsRespectfulRateLimitedError:
        logging.info('Requests-respectful reports rate limit hit.')
        raise RateLimitException()

    # If a rate cap is encountered, return a result reporting this.
    if data_response.status_code == 429:
        logging.info('Fitbit reports rate limit hit!')
        raise RateLimitException()
    if data_response.status_code == 504:
        logging.info('Fitbit server reports 504 response timeout!')
        raise RateLimitException()
    logging.debug('Fitbit returns status code: {}'.format(
        data_response.status_code))

    query_result = data_response.json()

    if ('errors' in query_result and 'success' in query_result
            and not query_result['success']):
        raise Exception(query_result['errors'])

    # Cache if the data's target date is more than CACHE_MIN before now.
    if target_date and (arrow.get() - target_date > CACHE_MIN):
        db.session.add(CacheItem(data_key, query_result))
        db.session.commit()
    else:
        logging.debug('{} not cached, data less than CACHE_MIN before '
                      'now.'.format(data_key))

    return query_result


def get_fitbit_data(access_token, open_humans_id, fitbit_data):
    """
    Iterate to get all items for a given access_token and path, return result.

    Result is a dict of all of the fitbit data.
    """
    user_realm = 'fitbit-{}'.format(open_humans_id)
    requests.register_realm(user_realm, max_requests=150, timespan=3600)
    requests.update_realm(user_realm, max_requests=150, timespan=3600)

    query_result = fitbit_query(access_token=access_token,
                                path='/-/profile.json',
                                open_humans_id=open_humans_id)

    # store the user ID since it's used in all future queries
    user_id = query_result['user']['encodedId']
    member_since = query_result['user']['memberSince']
    start_date = arrow.get(member_since, 'YYYY-MM-DD')

    # Reset data if user account ID has changed.
    if 'profile' in fitbit_data:
        if fitbit_data['profile']['encodedId'] != user_id:
            logging.info(
                'User ID changed from {} to {}. Resetting all data.'.format(
                    fitbit_data['profile']['encodedId'], user_id))
            fitbit_data = defaultdict(dict)
        else:
            logging.debug('User ID ({}) matches old data.'.format(user_id))

    fitbit_data['profile'] = {
        'averageDailySteps': query_result['user']['averageDailySteps'],
        'encodedId': user_id,
        'height': query_result['user']['height'],
        'memberSince': member_since,
        'strideLengthRunning': query_result['user']['strideLengthRunning'],
        'strideLengthWalking': query_result['user']['strideLengthWalking'],
        'weight': query_result['user']['weight'],
        }

    for url in [u for u in fitbit_urls if u['period'] is None]:
        if not user_id and 'profile' in fitbit_data:
            user_id = fitbit_data['profile']['user']['encodedId']

        query_result = fitbit_query(access_token=access_token,
                                    path=url['url'],
                                    parameters={'user_id': user_id},
                                    open_humans_id=open_humans_id,
                                    target_date=arrow.get())

        fitbit_data[url['name']] = query_result

    for url in [u for u in fitbit_urls if u['period'] == 'year']:
        years = arrow.Arrow.range('year', start_date.floor('year'),
                                  arrow.get())
        for year_date in years:
            year = year_date.format('YYYY')

            if year in fitbit_data[url['name']]:
                logger.info('Skip retrieval {}: {}'.format(url['name'], year))
                continue

            logger.info('Retrieving %s: %s', url['name'], year)
            query_result = fitbit_query(
                access_token=access_token,
                path=url['url'],
                parameters={
                    'user_id': user_id,
                    'start_date': year_date.floor('year').format('YYYY-MM-DD'),
                    'end_date': year_date.ceil('year').format('YYYY-MM-DD'),
                },
                open_humans_id=open_humans_id,
                target_date=year_date.ceil('year'))

            fitbit_data[url['name']][str(year)] = query_result

    for url in [u for u in fitbit_urls if u['period'] == 'month']:
        months = arrow.Arrow.range('month', start_date.floor('month'),
                                   arrow.get())
        for month_date in months:
            month = month_date.format('YYYY-MM')

            if month in fitbit_data[url['name']]:
                logger.info('Skip retrieval {}: {}'.format(url['name'], month))
                continue

            logger.info('Retrieving %s: %s', url['name'], month)
            query_result = fitbit_query(
                access_token=access_token,
                path=url['url'],
                parameters={
                    'user_id': user_id,
                    'start_date': month_date.floor('month').format('YYYY-MM-DD'),
                    'end_date': month_date.ceil('month').format('YYYY-MM-DD'),
                },
                open_humans_id=open_humans_id,
                target_date=month_date.ceil('month'))

            fitbit_data[url['name']][month] = query_result

    # Intraday retrieval -- not currently authorized.
    """
    for url in [u for u in fitbit_urls if u['period'] == 'day']:
        days = arrow.Arrow.range('day', start_date.floor('day'),
                                 arrow.get())

        for day_date in days:
            # continue

            # Intraday retrieval -- not currently authorized.
            day = day_date.format('YYYY-MM-DD')

            if day in fitbit_data[url['name']]:
                logger.info('Skip retrieval {}: {}'.format(url['name'], day))
                continue

            logger.info('Retrieving %s: %s', url['name'], day)
            query_result = fitbit_query(
                access_token=access_token,
                path=url['url'],
                parameters={
                    'user_id': user_id,
                    'date': day,
                },
                open_humans_id=open_humans_id,
                target_date=day_date.ceil('day'))

            fitbit_data[url['name']][day] = query_result
    """

    return fitbit_data


class FitbitSource(BaseSource):
    """
    Create an Open Humans Dataset from Fitbit API data.
    """

    source = 'fitbit'

    @staticmethod
    def _guess_storage_date(stored_data):
        """
        Return earliest date stored data may have been generated.

        This date is used to determine which stored data is/was old enough
        to trust (and not refresh). Data older than STORAGE_MIN relative
        to the guessed storage date is trusted and reused.

        We impose the STORAGE_MIN cutoff to try to reduce the chances of
        permanently storing incomplete data.
        """
        for url in [u for u in fitbit_urls if u['period'] == 'month']:
            current_months = stored_data[url['name']].keys()
            if current_months:
                most_recent = max([arrow.get(m) for m in
                                   current_months]).floor('month')
                logging.debug('Storage date guess: {}'.format(most_recent))
                return most_recent

        for url in [u for u in fitbit_urls if u['period'] == 'year']:
            current_years = stored_data[url['name']].keys()
            if current_years:
                most_recent = max([arrow.get('{}-01'.format(y)) for y in
                                   current_years]).floor('year')
                logging.debug('Storage date guess: {}'.format(most_recent))
                return most_recent

        # Fall back on an arbitrary early date.
        most_recent = arrow.get('2000-01-01')
        logging.debug('Storage date guess: {}'.format(most_recent))
        return most_recent

    def load_existing_fitbit_data(self):
        """
        Load existing fitbit data.

        Based on the guessed date this data was previously retrieved,
        re-use any data that was older than STORAGE_MIN relative to that date.

        Also return profile information in returned data. This should be
        overwritten, but is needed to check that the target Fitbit user hasn't
        changed.
        """
        file_info = self.get_current_files()
        target_file = None
        stored_data = defaultdict(dict)

        for file_item in file_info:
            if file_item['basename'] == 'fitbit-data.json':
                target_file = file_item
        if target_file:
            local_data_file = self.get_remote_file(target_file['download_url'])
            with open(self.temp_join(local_data_file)) as f:
                current_data = json.load(f)

            stored_data = defaultdict(dict)

            storage_date = self._guess_storage_date(current_data)

            # Profile saved to check user ID but should be overwritten later.
            if 'profile' in current_data:
                stored_data['profile'] = current_data['profile']

            # Copy in stored data older than STORAGE_MIN at time of storage.
            for url in [u for u in fitbit_urls if u['period'] == 'year']:
                name = url['name']
                for year in current_data[name].keys():
                    year_date = arrow.get('{}-01'.format(year))
                    if storage_date - year_date.ceil('year') < STORAGE_MIN:
                        logging.debug(
                            'Skip loading "{}" storage, less than {} relative '
                            'to guessed storage date "{}"'.format(
                                year, STORAGE_MIN, storage_date))
                        continue
                    stored_data[name][year] = current_data[name][year]
            for url in [u for u in fitbit_urls if u['period'] == 'month']:
                name = url['name']
                for month in current_data[name].keys():
                    month_date = arrow.get(month)
                    if storage_date - month_date.ceil('month') < STORAGE_MIN:
                        logging.debug(
                            'Skip loading "{}" storage, less than {} relative '
                            'to guessed storage date "{}"'.format(
                                month, STORAGE_MIN, storage_date))
                        continue
                    stored_data[name][month] = current_data[name][month]

        return stored_data

    def create_files(self):
        """
        Retrieve data and create fitbit data files.

        If an API cap is encountered, skip file creation and instead return
        a countdown. Data retrieval will be resubmitted, and when it runs again
        it will use previously cached queries. (This iterates until all
        queries can be completed.)
        """
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
            stored_data = self.load_existing_fitbit_data()
        except AttributeError:
            stored_data = defaultdict(dict)

        try:
            fitbit_data = get_fitbit_data(self.access_token, self.oh_user_id,
                                          fitbit_data=stored_data)
        except RateLimitException:
            return {'countdown': 900}

        with open(filepath, 'w') as f:
            json.dump(fitbit_data, f)

    def run_cli(self):
        """
        Override to loop/wait for command line use (no celery requeuing).
        """
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
    """
    Call the client method to run via the commpand line.
    """
    FitbitSource.cli()
