"""
RunKeeper data processing.

Copyright (C) 2015 PersonalGenomes.org

This software is shared under the "MIT License" license (aka "Expat License"),
see LICENSE.TXT for full license text.

May be used on the command line from this project's base directory, e.g.

   python -m sources.runkeeper [accesstoken] files

...where [accesstoken] is the private token RunKeeper's API has created that
grants permission to access a user's data. (Keep it safe!) This will assemble
data sets for the user at:

   files/RunKeeper_individual-12345678_activity-data_20150102T030405Z.tar.gz
   files/RunKeeper_individual-12345678_social-data_20150102T030405Z.tar.gz
   files/RunKeeper_individual-12345678_sleep-data_20150102T030405Z.tar.gz

(These filenames include an example user ID, "12345678", and an example
datetime stamp, "January 2rd 2015 3:04:05am".)
"""

from __future__ import unicode_literals

import json
import os

from datetime import datetime, timedelta

import requests

from base_source import BaseSource

BACKGROUND_DATA_KEYS = ['timestamp', 'steps', 'calories_burned', 'source']
FITNESS_SUMMARY_KEYS = ['type', 'equipment', 'start_time', 'utc_offset',
                        'total_distance', 'duration', 'total_calories',
                        'climb', 'source']
FITNESS_PATH_KEYS = ['latitude', 'longitude', 'altitude', 'timestamp', 'type']

PAGESIZE = '10000'


def data_for_keys(data_dict, data_keys):
    """
    Return a dict with data for requested keys, or empty strings if missing.
    """
    return {x: data_dict[x] if x in data_dict else '' for x in data_keys}


def yearly_items(items):
    current_year = (datetime.now() - timedelta(days=1)).year

    result = {}
    complete_years = []

    for item in items:
        try:
            time_string = item['start_time']
        except KeyError:
            time_string = item['timestamp']

        start_time = datetime.strptime(time_string, '%a, %d %b %Y %H:%M:%S')

        if start_time.year not in result:
            result[start_time.year] = []

            if start_time.year < current_year:
                complete_years.append(start_time.year)

        result[start_time.year].append(item)

    return result, complete_years


class RunKeeperSource(BaseSource):
    """
    Create Open Humans Dataset from RunKeeper API data

    Required arguments:
        access_token: RunKeeper access token
    """

    def __init__(self, access_token, **kwargs):
        self.access_token = access_token

        super(RunKeeperSource, self).__init__(**kwargs)

    def runkeeper_query(self, path, content_type=None):
        """
        Query RunKeeper API and return data.
        """
        headers = {'Authorization': 'Bearer {}'.format(self.access_token)}

        if content_type:
            headers['Content-Type'] = content_type

        data_url = 'https://api.runkeeper.com{}'.format(path)

        data_response = requests.get(data_url, headers=headers)
        data = data_response.json()

        return data

    def get_items(self, path, recurse='both'):
        """
        Iterate to get all items for a given access_token and path.

        RunKeeper uses the same pages format for items in various places.
        """
        query_data = self.runkeeper_query(path)
        items = query_data['items']

        if 'previous' in query_data and recurse in ['both', 'prev']:
            prev_items = self.get_items(self.query_data['previous'],
                                        recurse='prev')
            items = prev_items + items

        if 'next' in query_data and recurse in ['both', 'next']:
            next_items = self.get_items(query_data['next'], recurse='next')
            items = items + next_items

        if recurse == 'both':
            # Assert we have correct size.
            if len(items) != query_data['size']:
                error_msg = ('Activity items for retrieved for {} ({}) '
                             "doesn't match expected array size ({})").format(
                                 path, len(items), query_data['size'])
                raise AssertionError(error_msg)

        return items

    def create_files(self):
        """
        Data is split per-year, in JSON format.
        Each JSON is an object (dict) in the following format (pseudocode):

        { 'background_activities':
            [
              { key: value for each of BACKGROUND_DATA_KEYS },
              { key: value for each of BACKGROUND_DATA_KEYS },
              ...
            ],
          'fitness_activities':
            [
              { 'path': { key: value for each of FITNESS_PATH_KEYS },
                 and key: value for each of the FITNESS_ACTIVITY_KEYS },
              { 'path': { key: value for each of FITNESS_PATH_KEYS },
                 and key: value for each of the FITNESS_ACTIVITY_KEYS },
              ...
            ]
        }

        Notes:
            - items are sorted according to start_time or timestamp
            - The item_uri for fitness_activities matches item_uri in
              fitness_activity_sharing.
        """
        user_data = self.runkeeper_query('/user')

        # Get activity data.
        fitness_activity_path = '{}?pageSize={}'.format(
            user_data['fitness_activities'], PAGESIZE)
        fitness_activity_items, complete_fitness_activity_years = yearly_items(
            self.get_items(path=fitness_activity_path))

        # Background activities.
        background_activ_path = '{}?pageSize={}'.format(
            user_data['background_activities'], PAGESIZE)
        background_activ_items, complete_background_activ_years = yearly_items(
            self.get_items(background_activ_path))

        all_years = sorted(set(fitness_activity_items.keys() +
                               background_activ_items.keys()))
        all_completed_years = set(
            complete_fitness_activity_years + complete_background_activ_years)

        for year in all_years:
            outdata = {'fitness_activities': [],
                       'background_activities': []}

            fitness_items = sorted(
                fitness_activity_items.get(year, []),
                key=lambda item: datetime.strptime(
                    item['start_time'], '%a, %d %b %Y %H:%M:%S'))

            for item in fitness_items:
                item_data = self.runkeeper_query(item['uri'])
                item_data_out = data_for_keys(item_data, FITNESS_SUMMARY_KEYS)
                item_data_out['path'] = [
                    data_for_keys(datapoint, FITNESS_PATH_KEYS)
                    for datapoint in item_data['path']]
                outdata['fitness_activities'].append(item_data_out)

            background_items = sorted(
                background_activ_items.get(year, []),
                key=lambda item: datetime.strptime(
                    item['timestamp'], '%a, %d %b %Y %H:%M:%S'))

            for item in background_items:
                outdata['background_activities'].append(
                    data_for_keys(item, BACKGROUND_DATA_KEYS))

            filename = 'Runkeeper-activity-data-{}.json'.format(str(year))
            filepath = os.path.join(self.temp_directory, filename)

            with open(filepath, 'w') as f:
                json.dump(outdata, f, indent=2, sort_keys=True)

            self.temp_files.append({
                'temp_filename': filename,
                'metadata': {
                    'description': ('Runkeeper GPS maps and imported '
                                    'activity data.'),
                    'tags': ['GPS', 'Runkeeper'],
                    'dataYear': year,
                    'complete': year in all_completed_years,
                }
            })


if __name__ == '__main__':
    RunKeeperSource.cli()
