"""
Create data files from a user's GoViral data.

Copyright (C) 2016 PersonalGenomes.org

This software is shared under the "MIT License" license (aka "Expat License"),
see LICENSE.TXT for full license text.
"""

import json
import os

import requests

from base_source import BaseSource

GO_VIRAL_DATA_URL = 'https://www.goviralstudy.com/participants/{}/data'


class GoViralSource(BaseSource):
    """
    Create a GoViral dataset for the given ID.

    Required arguments:
        access_token: the management access token for GoViral
        go_viral_id: the user's GoViral ID
    """

    def __init__(self, access_token, go_viral_id, **kwargs):
        self.access_token = access_token
        self.go_viral_id = go_viral_id

        super(GoViralSource, self).__init__(**kwargs)

    def get_go_viral_data(self):
        """
        Retrieve GoViral data from the API for a given user.
        """
        request = requests.get(GO_VIRAL_DATA_URL.format(self.go_viral_id),
                               params={'access_token': self.access_token})

        if request.status_code != 200:
            self.sentry_log('GoViral website not permitting any access! Bad '
                            'access token?')

            return None

        request_data = request.json()

        if ('code' in request_data and
                request_data['code'] == 'PERMISSION_DENIED'):
            self.sentry_log('Data access denied by GoViral. User: {}'
                            .format(self.go_viral_id))

            return None

        data = request.json()

        for item in data.keys():
            if not data[item]:
                data.pop(item)

        if not data:
            return None

        return data

    def handle_go_viral_data(self, data):
        json_filename = 'GoViral-sickness-data.json'
        json_filepath = os.path.join(self.temp_directory, json_filename)

        with open(json_filepath, 'w') as f:
            json.dump(data, f, indent=2, sort_keys=True)

        return {
            'temp_filename': json_filename,
            'tempdir': self.temp_directory,
            'metadata': {
                'description': ('GoViral annual surveys, and sickness reports '
                                'with viral lab test results (if available)'),
                'tags': ['viral', 'survey', 'GoViral', 'json'],
            }
        }

    def create_files(self):
        data_go_viral = self.get_go_viral_data()

        # Don't create a file if there's no data from GoViral
        if not data_go_viral:
            return

        self.temp_files.append(self.handle_go_viral_data(data=data_go_viral))


if __name__ == '__main__':
    GoViralSource.cli()
