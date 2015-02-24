"""
Create an OHDataSet from a user's GoViral data.

Copyright (C) 2015 PersonalGenomes.org

This software is shared under the "MIT License" license (aka "Expat License"),
see LICENSE.TXT for full license text.
"""
import json
import sys

from cStringIO import StringIO
from datetime import datetime

import requests

from .participant_data_set import get_dataset, OHDataSource

GO_VIRAL_DATA_URL = 'http://www.goviralstudy.com/participants/{}/data'


def get_go_viral_data(access_token, go_viral_id):
    """
    Retrieve GoViral data from the API for a given user.
    """
    request = requests.get(GO_VIRAL_DATA_URL.format(go_viral_id), params={
        'access_token': access_token
    })

    return request.json()


def create_go_viral_ohdataset(access_token, go_viral_id,
                              task_id=None, update_url=None,
                              **kwargs):
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
    now = datetime.now().strftime('%Y%m%d%H%M%S')
    filename = '{}-go-viral.tar.gz'.format(now)

    source = OHDataSource(name='GoViral Integration API',
                          url='http://www.goviralstudy.com/')

    dataset = get_dataset(filename, source, **kwargs)

    print 'Fetching GoViral data.'

    data_go_viral = get_go_viral_data(access_token, go_viral_id)

    # Don't create a file if there's no data from GoViral
    if not data_go_viral:
        return

    dataset.add_file(file=StringIO(json.dumps(data_go_viral, indent=2)),
                     name='go-viral.json')
    dataset.close()

    dataset.update(update_url, task_id)

    return dataset


if __name__ == '__main__':
    if len(sys.argv) != 4:
        print 'Please specify a token, ID, and directory.'

        sys.exit(1)

    create_go_viral_ohdataset(*sys.argv[1:-1], filedir=sys.argv[3])
