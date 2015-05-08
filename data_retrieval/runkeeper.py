"""
RunKeeper data processing.

Copyright (C) 2015 PersonalGenomes.org

This software is shared under the "MIT License" license (aka "Expat License"),
see LICENSE.TXT for full license text.

May be used on the command line from this project's base directory, e.g.

   python -m data_retrieval.runkeeper [accesstoken] files

...where [accesstoken] is the private token RunKeeper's API has created that
grants permission to access a user's data. (Keep it safe!) This will assemble
data sets for the user at:

   files/RunKeeper_individual-12345678_activity-data_20150102T030405Z.tar.gz
   files/RunKeeper_individual-12345678_social-data_20150102T030405Z.tar.gz
   files/RunKeeper_individual-12345678_sleep-data_20150102T030405Z.tar.gz

(These filenames include an example user ID, "12345678", and an example
datetime stamp, "January 2rd 2015 3:04:05am".)
"""
import sys

import requests

from .participant_data_set import format_filename, get_dataset, OHDataSource


def runkeeper_query(access_token, path):
    """Get full genotype data from 23andme API."""
    headers = {'Authorization': 'Bearer %s' % access_token}
    data_url = 'https://api.runkeeper.com%s' % path
    data_response = requests.get(data_url, headers=headers)
    data = data_response.json()
    return data


def get_items(access_token, path, recurse='both'):
    query_data = runkeeper_query(access_token, path)
    items = query_data['items']
    if 'previous' in query_data and recurse in ['both', 'prev']:
        prev_items = get_items(
            access_token, query_data['previous'], recurse='prev')
        items = prev_items + items
    if 'next' in query_data and recurse in ['both', 'next']:
        next_items = get_items(
            access_token, query_data['next'], recurse='next')
        items = items + next_items
    if recurse == 'both':
        # Assert we have correct size.
        if not (len(items) == query_data['size']):
            error_msg = ("Activity items for retrieved for {} ({}) doesn't " +
                         "match expected array size ({})").format(
                path, len(items), query_data['size'])
            raise AssertionError(error_msg)
    return items


def create_runkeeper_ohdataset(access_token,
                               task_id=None,
                               update_url=None,
                               **kwargs):
    """Create Open Humans Dataset from RunKeeper API data

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
    user_data = runkeeper_query(access_token, '/user')
    identifier = 'individual-{}'.format(user_data['userID'])
    filename_activity = format_filename('RunKeeper', identifier,
                                        'activity-data')
    filename_social = format_filename('RunKeeper', identifier, 'social-data')
    filename_sleep = format_filename('RunKeeper', identifier, 'sleep-data')

    # Initial data storage.
    activity_data = {'fitness_activities': [], 'background_activities': []}
    social_data = {'fitness_activity_sharing': [], 'friends': [],
                   'userID': user_data['userID']}
    sleep_data = {'sleep_logs': []}

    # Get activity data.
    fitness_activity_items = get_items(
        access_token, user_data['fitness_activities'])
    background_activity_items = get_items(
        access_token, user_data['background_activities'])
    # Fitness activities.
    for item in fitness_activity_items:
        item_data = runkeeper_query(access_token, item['uri'])
        # Record activity data.
        activity_data_fields = ['uri', 'type', 'start_time', 'utc_offset',
                                'total_distance', 'duration', 'total_calories',
                                'climb', 'path', 'source']
        item_activity_data = {x: item_data[x] for x in activity_data_fields}
        activity_data['fitness_activities'].append(item_activity_data)
        # Record social data.
        social_data_fields = ['uri', 'share', 'share_map']
        item_social_data = {x: item_data[x] for x in social_data_fields}
        social_data['fitness_activity_sharing'].append(item_social_data)
    # Background activities.
    for item in background_activity_items:
        activity_data_fields = ['timestamp', 'calories_burned', 'steps',
                                'source']
        item_data = runkeeper_query(access_token, item['uri'])
        item_activity_data = {x: item_data[x] for x in activity_data_fields}
        activity_data['background_activities'].append(item_activity_data)

    # Get friend data.
    friends_items = get_items(access_token, user_data['team'])
    for item in friends_items:
        social_data_fields = ['userID', 'status']
        item_data = runkeeper_query(access_token, item['url'])
        friends_social_data = {x: item_data[x] for x in social_data_fields}
        social_data['friends'].append(friends_social_data)

    # Get friend data.
    sleep_items = get_items(access_token, user_data['sleep'])
    for item in sleep_items:
        sleep_data_fields = ['timestamp', 'total_sleep', 'source']
        sleep_data = {x: item[x] for x in sleep_data_fields}
        sleep_data['sleep_logs'].append(sleep_data)

    print activity_data
    print social_data
    print sleep_data

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print 'Please specify a token and directory.'
        sys.exit(1)

    create_runkeeper_ohdataset(*sys.argv[1:-1], filedir=sys.argv[-1])
