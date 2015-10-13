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

from collections import OrderedDict
from cStringIO import StringIO
import csv
import json
import sys

import requests

from .participant_data_set import format_filename, get_dataset, OHDataSource

BACKGROUND_DATA_KEYS = ['timestamp', 'steps', 'calories_burned', 'source']
FITNESS_SUMMARY_KEYS = ['type', 'equipment', 'start_time', 'utc_offset',
                        'total_distance', 'duration', 'total_calories',
                        'climb', 'source']
FITNESS_PATH_KEYS = ['latitude', 'longitude', 'altitude', 'timestamp', 'type']
FITNESS_SOCIAL_KEYS = ['share', 'share_map']
FRIENDS_SOCIAL_KEYS = ['userID', 'status']
SLEEP_DATA_KEYS = ['timestamp', 'total_sleep', 'deep', 'rem', 'light', 'awake',
                   'times_woken', 'source']


def runkeeper_query(access_token, path, content_type=None):
    """
    Query RunKeeper API and return data.
    """
    headers = {'Authorization': 'Bearer %s' % access_token}
    if content_type:
        headers['Content-Type'] = content_type
    data_url = 'https://api.runkeeper.com%s' % path
    data_response = requests.get(data_url, headers=headers)
    data = data_response.json()
    return data


class CSVIO(object):
    """
    Shorthand for making a StringIO file-like object with CSV data.
    """
    def __init__(self):
        self.filehandle = StringIO()
        self.csvwriter = csv.writer(self.filehandle)

    def writerow(self, *args, **kwargs):
        return self.csvwriter.writerow(*args, **kwargs)

    def seek(self, *args, **kwargs):
        return self.filehandle.seek(*args, **kwargs)


def get_items(access_token, path, recurse='both'):
    """
    Iterate to get all items for a given access_token and path.

    RunKeeper uses the same pages format for items in various places.
    """
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


def data_for_keys(data_dict, data_keys):
    """
    Return a dict with data for requested keys, or empty strings if missing.
    """
    data_out = {x: data_dict[x] if x in data_dict else '' for x in data_keys}
    return data_out


def get_runkeeper_data(access_token, user_data):
    """
    Get activity, social, and sleep data from RunKeeper for a given user.

    Data is returned as a dict with the following format (pseudocode):
    { 'activities_data':
        { 'background_activities':
            { item_uri:
                { key: value for each of BACKGROUND_DATA_KEYS },
              ...
            },
          'fitness_activities':
            { item_uri:
                { 'path': { key: value for each of FITNESS_PATH_KEYS },
                  and key: value for each of the FITNESS_ACTIVITY_KEYS
                },
              ...
            },
          'userID': userID
        }
      'social_data':
        { 'fitness_activity_sharing':
            { item_uri:
                { key: value for each of FITNESS_SOCIAL_KEYS },
              ...
            },
          'friends':
            { item_url:
                { key: value for each of FRIENDS_SOCIAL_KEYS },
              ...
            },
          'userID': userID
        },
      'sleep_data':
        { 'sleep_logs':
            { item_uri:
                { key: value for each of SLEEP_DATA_KEYS },
              ...
            },
          'user_id': userID
        }
    }

    Notes:
        - the data for the following keys are OrderedDict objects where the
          each key is the item URI (or URL) used to retrieve data:
          ['activities_data']['background_activities']
          ['activities_data']['fitness_activities']
          ['social_data']['fitness_activity_sharing']
          ['social_data']['friends']
          ['sleep_data']['sleep_logs']
          This is done to preserve order that items are listed by RunKeeper.

        - The item_uri for fitness_activities matches item_uri in
          fitness_activity_sharing.

        - As of May 2015, RunKeeper's API uses 'url' for 'friends' data instead
          of 'uri'.
    """
    # Initial data storage.
    activity_data = {'fitness_activities': OrderedDict(),
                     'background_activities': OrderedDict(),
                     'userID': user_data['userID']}
    social_data = {'fitness_activity_sharing': OrderedDict(),
                   'friends': OrderedDict(),
                   'userID': user_data['userID']}
    sleep_data = {'sleep_logs': OrderedDict(),
                  'userID': user_data['userID']}

    # Get activity data.
    fitness_activity_items = get_items(
        access_token, user_data['fitness_activities'])
    background_activity_items = get_items(
        access_token, user_data['background_activities'])
    # Fitness activities.
    for item in fitness_activity_items:
        item_data = runkeeper_query(
            access_token,
            item['uri'],
            content_type='application/vnd.com.runkeeper.FitnessActivity+json')
        # Record activity data.
        item_activity_data = data_for_keys(item_data, FITNESS_SUMMARY_KEYS)
        item_activity_data['path'] = [
            data_for_keys(datapoint, FITNESS_PATH_KEYS)
            for datapoint in item_data['path']]
        activity_data['fitness_activities'][item['uri']] = item_activity_data
        # Record social data.
        item_social_data = data_for_keys(item_data, FITNESS_SOCIAL_KEYS)
        social_data['fitness_activity_sharing'][item['uri']] = item_social_data
    # Background activities.
    for item in background_activity_items:
        item_data = runkeeper_query(access_token, item['uri'])
        item_bkgrnd_data = data_for_keys(item_data, BACKGROUND_DATA_KEYS)
        activity_data['background_activities'][item['uri']] = item_bkgrnd_data

    # Get friend data.
    friends_items = get_items(access_token, user_data['team'])
    for item in friends_items:
        item_data = runkeeper_query(access_token, item['url'])
        friends_social_data = data_for_keys(item_data, FRIENDS_SOCIAL_KEYS)
        social_data['friends'][item['url']] = friends_social_data

    # Get sleep data.
    sleep_items = get_items(access_token, user_data['sleep'])
    for item in sleep_items:
        item_data = runkeeper_query(access_token, item['uri'])
        sleep_log_data = data_for_keys(item_data, SLEEP_DATA_KEYS)
        sleep_data['sleep_logs'][item['uri']] = sleep_log_data

    return {'activities_data': activity_data,
            'social_data': social_data,
            'sleep_data': sleep_data}


def make_activity_dataset(data, filename, source, **kwargs):
    """
    Process activity data to create OHDataSet with JSON and CSV data files.
    """
    dataset = get_dataset(filename, source, **kwargs)
    filename_base = filename.rstrip('.tar.gz')

    # Store data as JSON file.
    json_out = StringIO(json.dumps(data, indent=2, sort_keys=True) + '\n')
    filename_json = filename_base + '.json'
    dataset.add_file(file=json_out, name=filename_json)

    # Store user ID as a text file.
    userID_io = StringIO(str(data['userID']))
    userID_filename = filename_base + '.userID.txt'
    dataset.add_file(file=userID_io, name=userID_filename)

    # Store background data as CSV files, if it exists.
    if data['background_activities']:
        background_data = data['background_activities']
        # Format as CSV data in a StringIO file-like object.
        csv_background = CSVIO()
        csv_background.writerow(['uri'] + BACKGROUND_DATA_KEYS)
        for item_uri in background_data.keys():
            csv_background.writerow([item_uri] + [
                background_data[item_uri][x] for x in BACKGROUND_DATA_KEYS])
        # Add as file to the dataset.
        csv_background.seek(0)
        filename_csv_background = filename_base + '.background-activities.csv'
        dataset.add_file(file=csv_background.filehandle,
                         name=filename_csv_background)

    # Store fitness activity data in a pair of CSV files, if it exists.
    if data['fitness_activities']:
        fitness_data = data['fitness_activities']
        # Each fitness has summary data, and an array of GPS datapoints.
        # Format each as CSV data in a StringIO file-like object.
        csv_fitness_summary = CSVIO()
        csv_fitness_summary.writerow(['uri'] + FITNESS_SUMMARY_KEYS)
        csv_fitness_path = CSVIO()
        # Path data includes URI for cross-reference with CSV summary data.
        csv_fitness_path.writerow(['uri'] + FITNESS_PATH_KEYS)

        # Process fitness activity data into CSV data.
        for item_uri in fitness_data.keys():
            csv_fitness_summary.writerow([item_uri] + [
                fitness_data[item_uri][x] for x in FITNESS_SUMMARY_KEYS])
            for point in fitness_data[item_uri]['path']:
                csv_fitness_path.writerow([item_uri] + [point[x] for x in
                                                        FITNESS_PATH_KEYS])

        # Add as files to the dataset.
        csv_fitness_summary.seek(0)
        csv_fitness_path.seek(0)
        filename_csv_fitness_summary = (filename_base +
                                        '.fitness-activities-summary-data.csv')
        filename_csv_fitness_path = (filename_base +
                                     '.fitness-activities-path-data.csv')
        dataset.add_file(file=csv_fitness_summary.filehandle,
                         name=filename_csv_fitness_summary)
        dataset.add_file(file=csv_fitness_path.filehandle,
                         name=filename_csv_fitness_path)

    return dataset


def make_social_dataset(data, filename, source, **kwargs):
    """
    Process social data to create OHDataSet with JSON and CSV data files.
    """
    dataset = get_dataset(filename, source, **kwargs)
    filename_base = filename.rstrip('.tar.gz')

    # Store data as JSON file.
    json_out = StringIO(json.dumps(data, indent=2, sort_keys=True) + '\n')
    filename_json = filename_base + '.json'
    dataset.add_file(file=json_out, name=filename_json)

    # Store user ID as a text file.
    userID_io = StringIO(str(data['userID']))
    userID_filename = filename_base + '.userID.txt'
    dataset.add_file(file=userID_io, name=userID_filename)

    # Store fitness activity sharing data as CSV file, if it exists.
    if data['fitness_activity_sharing']:
        activity_sharing_data = data['fitness_activity_sharing']
        # Format as CSV data in a StringIO file-like object.
        csv_activity_sharing = CSVIO()
        csv_activity_sharing.writerow(['uri'] + FITNESS_SOCIAL_KEYS)
        for item_uri in activity_sharing_data.keys():
            csv_activity_sharing.writerow([item_uri] + [
                activity_sharing_data[item_uri][x] for x
                in FITNESS_SOCIAL_KEYS])
        # Add as file to the dataset.
        csv_activity_sharing.seek(0)
        filename_activity_sharing = (filename_base +
                                     '.fitness-activity-sharing.csv')
        dataset.add_file(file=csv_activity_sharing.filehandle,
                         name=filename_activity_sharing)

    # Store friend data as CSV file, if it exists.
    if data['friends']:
        friends_data = data['friends']
        # Format as CSV data in a StringIO file-like object.
        csv_friends = CSVIO()
        csv_friends.writerow(['url'] + FRIENDS_SOCIAL_KEYS)
        for item_url in friends_data.keys():
            csv_friends.writerow([item_url] + [
                friends_data[item_url][x] for x in FRIENDS_SOCIAL_KEYS])
        # Add as file to the dataset.
        csv_friends.seek(0)
        filename_csv_friends = filename_base + '.friends.csv'
        dataset.add_file(file=csv_friends.filehandle,
                         name=filename_csv_friends)

    return dataset


def make_sleep_dataset(data, filename, source, **kwargs):
    """
    Process sleep data to create OHDataSet with JSON and CSV data files.
    """
    dataset = get_dataset(filename, source, **kwargs)
    filename_base = filename.rstrip('.tar.gz')

    # Store data as JSON file.
    json_out = StringIO(json.dumps(data, indent=2, sort_keys=True) + '\n')
    filename_json = filename_base + '.json'
    dataset.add_file(file=json_out, name=filename_json)

    # Store user ID as a text file.
    userID_io = StringIO(str(data['userID']))
    userID_filename = filename_base + '.userID.txt'
    dataset.add_file(file=userID_io, name=userID_filename)

    # Store sleep logs as a text file.
    # Note that these must exist, otherwise this function would not be called.
    sleep_logs_data = data['sleep_logs']
    csv_sleep_logs = CSVIO()
    csv_sleep_logs.writerow(['uri'] + SLEEP_DATA_KEYS)
    for item_uri in sleep_logs_data.keys():
        csv_sleep_logs.writerow([item_uri] + [
            sleep_logs_data[item_uri][x] for x in SLEEP_DATA_KEYS])
    # Add as file to the dataset.
    csv_sleep_logs.seek(0)
    filename_sleep_logs = (filename_base + '.sleep-logs.csv')
    dataset.add_file(file=csv_sleep_logs.filehandle,
                     name=filename_sleep_logs)

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
                                        'activities-data')
    filename_social = format_filename('RunKeeper', identifier, 'social-data')
    filename_sleep = format_filename('RunKeeper', identifier, 'sleep-data')
    source = OHDataSource(name='RunKeeper Health Graph API',
                          url='http://developer.runkeeper.com/healthgraph')

    # Make activity data file if there's activity data.
    if (runkeeper_data['activities_data']['background_activities'] or
            runkeeper_data['activities_data']['fitness_activities']):
        activity_dataset = make_activity_dataset(
            data=runkeeper_data['activities_data'],
            filename=filename_activity,
            source=source,
            **kwargs)
        activity_dataset.metadata['runkeeper_id'] = str(user_data['userID'])
        activity_dataset.close()
        if update_url and task_id:
            activity_dataset.update(update_url, task_id,
                                    subtype='activities-data')

    # Make social data file if there's social data.
    if (runkeeper_data['social_data']['fitness_activity_sharing'] or
            runkeeper_data['social_data']['friends']):
        social_dataset = make_social_dataset(
            data=runkeeper_data['social_data'],
            filename=filename_social,
            source=source,
            **kwargs)
        social_dataset.metadata['runkeeper_id'] = str(user_data['userID'])
        social_dataset.close()
        if update_url and task_id:
            social_dataset.update(update_url, task_id, subtype='social-data')

    # Make sleep data file if there's sleep log data.
    if runkeeper_data['sleep_data']['sleep_logs']:
        sleep_dataset = make_sleep_dataset(
            data=runkeeper_data['sleep_data'],
            filename=filename_sleep,
            source=source,
            **kwargs)
        sleep_dataset.metadata['runkeeper_id'] = str(user_data['userID'])
        sleep_dataset.close()
        if update_url and task_id:
            sleep_dataset.update(update_url, task_id, subtype='sleep-data')


if __name__ == '__main__':
    if len(sys.argv) != 3:
        print 'Please specify a token and directory.'
        sys.exit(1)

    create_runkeeper_ohdataset(*sys.argv[1:-1], filedir=sys.argv[-1])
