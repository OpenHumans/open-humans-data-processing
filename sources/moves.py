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
import time

import requests

from base_source import BaseSource
from models import CacheItem

if __name__ == '__main__':
    from utilities import init_db

    db = init_db()
else:
    from models import db


class MovesSource(BaseSource):
    """
    Create Open Humans Dataset from Moves API data

    Required arguments:
        access_token: Moves access token
    """

    def __init__(self, access_token, **kwargs):
        self.access_token = access_token

        super(MovesSource, self).__init__(**kwargs)

    def moves_query(self, path):
        """
        Query Moves API and return result.

        Result is a dict with the following keys:
            'response_json': data from the query JSON, or None if rate cap hit.
            'rate_cap_encountered': None, or True if rate cap hit.
        """
        headers = {'Authorization': 'Bearer %s' % self.access_token}
        data_url = 'https://api.moves-app.com/api/1.1{}'.format(path)
        data_key = '{}{}'.format(data_url, self.access_token)

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

    def get_full_storyline(self):
        """
        Iterate to get all items for a given access_token and path, return
        result.

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
            query_result = self.moves_query(
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

    def create_files(self):
        filename = 'moves-storyline-data.json'
        filepath = os.path.join(self.temp_directory, filename)

        self.temp_files.append({
            'temp_filename': filename,
            'metadata': {
                'description': ('Moves GPS maps, locations, and steps data.'),
                'tags': ['GPS', 'Moves', 'steps'],
            }
        })

        full_storyline_result = self.get_full_storyline()

        if full_storyline_result['rate_cap_encountered']:
            # If this is previously called and we got no new data this round,
            # double the wait period for resubmission.
            if self.return_status and not full_storyline_result['all_data']:
                countdown = 2 * self.return_status['countdown']
            else:
                countdown = 60

            return {'countdown': countdown}

        user_data = full_storyline_result['all_data']

        with open(filepath, 'w') as f:
            json.dump(user_data, f, indent=2)

    def run_cli(self):
        while True:
            result = self.create_files()

            if result:
                countdown = result['countdown']

                print('Rate cap hit. Pausing {}s before resuming...'.format(
                    countdown))

                time.sleep(countdown)
            else:
                break

        self.move_files()


if __name__ == '__main__':
    MovesSource.cli()
