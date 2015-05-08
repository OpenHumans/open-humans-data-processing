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
from __future__ import unicode_literals

from cStringIO import StringIO
import csv
import json
import sys

import requests

from .participant_data_set import format_filename, get_dataset, OHDataSource

BACKGROUND_DATA_KEYS = ['timestamp', 'steps', 'calories_burned', 'source']
FITNESS_SUMMARY_KEYS = ['uri', 'type', 'start_time', 'utc_offset',
                        'total_distance', 'duration', 'total_calories',
                        'climb', 'source']
FITNESS_PATH_KEYS = ['latitude', 'longitude', 'altitude', 'timestamp', 'type']
FITNESS_SOCIAL_KEYS = ['uri', 'share', 'share_map']
FRIENDS_SOCIAL_KEYS = ['userID', 'status']
SLEEP_DATA_KEYS = ['timestamp', 'total_sleep', 'source']


def runkeeper_query(access_token, path):
    """
    Query RunKeeper API and return data.
    """
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


def get_runkeeper_data(access_token, user_data):
    """
    Get activity, social, and sleep data from RunKeeper for a given user.
    """
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
        item_activity_data = {x: item_data[x] for x in FITNESS_SUMMARY_KEYS}
        item_activity_data['path'] = [
            {x: datapoint[x] for x in FITNESS_PATH_KEYS}
            for datapoint in item_data['path']]
        activity_data['fitness_activities'].append(item_activity_data)
        # Record social data.
        item_social_data = {x: item_data[x] for x in FITNESS_SOCIAL_KEYS}
        social_data['fitness_activity_sharing'].append(item_social_data)
    # Background activities.
    for item in background_activity_items:
        item_data = runkeeper_query(access_token, item['uri'])
        item_activity_data = {x: item_data[x] for x in BACKGROUND_DATA_KEYS}
        activity_data['background_activities'].append(item_activity_data)

    # Get friend data.
    friends_items = get_items(access_token, user_data['team'])
    for item in friends_items:
        item_data = runkeeper_query(access_token, item['url'])
        friends_social_data = {x: item_data[x] for x in FRIENDS_SOCIAL_KEYS}
        social_data['friends'].append(friends_social_data)

    # Get sleep data.
    sleep_items = get_items(access_token, user_data['sleep'])
    for item in sleep_items:
        sleep_data = {x: item[x] for x in SLEEP_DATA_KEYS}
        sleep_data['sleep_logs'].append(sleep_data)

    return {'activity_data': activity_data,
            'social_data': social_data,
            'sleep_data': sleep_data}


def make_activity_dataset(data, filename, source, **kwargs):
    dataset = get_dataset(filename, source, **kwargs)
    filename_base = filename.rstrip('.tar.gz')

    # Store data as JSON file.
    json_out = StringIO(json.dumps(data, indent=2, sort_keys=True) + '\n')
    filename_json = filename_base + '.json'
    dataset.add_file(file=json_out, name=filename_json)

    # Store background data as CSV files, if it exists.
    if data['background_activities']:
        print "Writing background data to csv"
        background_data = data['background_activities']

        # Gather data as CSV data in a StringIO file-like object.
        csv_out_background = StringIO()
        csv_writer_background = csv.writer(csv_out_background)
        header = background_data[0].keys()
        csv_writer_background.writerow(header)
        for item in background_data:
            print "Writing row..."
            csv_writer_background.writerow([item[x] for x in header])

        # Add as file to the dataset.
        csv_out_background.seek(0)
        filename_csv_background = filename_base + '.background-activities.csv'
        dataset.add_file(file=csv_out_background, name=filename_csv_background)

    # Store fitness activity data in a pair of CSV files, if it exists.
    if data['fitness_activities']:
        print "Writing fitness data to csv"
        fitness_data = data['fitness_activities']

        # Each fitness has summary data, and an array of GPS datapoints.
        # Storing both as CSV data in a StringIO file-like object.
        # Summary data...
        csv_out_fitness_summary = StringIO()
        csv_writer_fitness_summary = csv.writer(csv_out_fitness_summary)
        csv_writer_fitness_summary.writerow(FITNESS_SUMMARY_KEYS)
        # ... and path data, with URI to cross-reference with summary data.
        csv_out_fitness_path = StringIO()
        csv_writer_fitness_path = csv.writer(csv_out_fitness_path)
        csv_writer_fitness_path.writerow(['uri'] + FITNESS_PATH_KEYS)

        # Process fitness activity data into CSV data.
        for item in fitness_data:
            print "Writing to summary file..."
            csv_writer_fitness_summary.writerow([item[x] for
                                                 x in FITNESS_SUMMARY_KEYS])
            for datapoint in item['path']:
                print "Writing to path file..."
                csv_writer_fitness_path.writerow(
                    [item['uri']] + [datapoint[x] for x in FITNESS_PATH_KEYS])

        # Add as files to the dataset.
        csv_out_fitness_summary.seek(0)
        csv_out_fitness_path.seek(0)
        filename_csv_fitness_summary = (filename_base +
                                        '.fitness-activities-summary-data.csv')
        filename_csv_fitness_path = (filename_base +
                                     '.fitness-activities-path-data.csv')
        dataset.add_file(file=csv_out_fitness_summary,
                         name=filename_csv_fitness_summary)
        dataset.add_file(file=csv_out_fitness_path,
                         name=filename_csv_fitness_path)

    return dataset


def create_runkeeper_ohdataset(access_token,
                               task_id=None,
                               update_url=None,
                               **kwargs):
    """
    Create Open Humans Dataset from RunKeeper API data

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
    runkeeper_data = get_runkeeper_data(access_token, user_data)

    identifier = 'individual-{}'.format(user_data['userID'])
    filename_activity = format_filename('RunKeeper', identifier,
                                        'activity-data')
    filename_social = format_filename('RunKeeper', identifier, 'social-data')
    filename_sleep = format_filename('RunKeeper', identifier, 'sleep-data')
    source = OHDataSource(name='RunKeeper Health Graph API',
                          url='http://developer.runkeeper.com/healthgraph')

    # Make activity data file if there's activity data.
    activity_data = runkeeper_data['activity_data']
    if (activity_data['background_activities'] or
            activity_data['fitness_activities']):
        activity_dataset = make_activity_dataset(
            data=runkeeper_data['activity_data'],
            filename=filename_activity,
            source=source,
            **kwargs)
        activity_dataset.close()
        if update_url and task_id:
            activity_dataset.update(update_url, task_id, )


if __name__ == '__main__':
    if len(sys.argv) != 3:
        print 'Please specify a token and directory.'
        sys.exit(1)

    create_runkeeper_ohdataset(*sys.argv[1:-1], filedir=sys.argv[-1])
