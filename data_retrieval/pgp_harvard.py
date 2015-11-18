"""
PGP Harvard data extraction.

Copyright (C) 2014 PersonalGenomes.org

This software is shared under the "MIT License" license (aka "Expat License"),
see LICENSE.TXT for full license text.

May be used on the command line from this project's base directory, e.g.

   python -m data_retrieval.pgp_harvard hu43860C files

...will assemble data sets for the ID "hu43860C" at:

   files/PGPHarvard-hu43860C-CGIgenome-20150102030405.tar.gz
   files/PGPHarvard-hu43860C-surveys-20150102030405.tar.gz

(These filenames includes a datetime stamp, January 2rd 2015 3:04:05am.)
"""

import json
import os
import re
import sys

from cStringIO import StringIO
from datetime import datetime

import requests

from bs4 import BeautifulSoup

from .participant_data_set import format_filename, get_dataset, OHDataSource

BASE_URL = 'https://my.pgp-hms.org'

if os.environ.get('ENV') == 'staging':
    PASSWORD = os.environ.get('PGP_PASSWORD')
    BASE_URL = 'https://{}@my-dev.pgp-hms.org'.format(PASSWORD)


def parse_uploaded_div(profile_soup):
    """
    Parse PGP profile to return survey data.

    input: A bs4.BeautifulSoup object generated from the HTML content
           of a PGP Harvard public profile webpage.
    returns: A list of links to genome data files produced by PGP
             Harvard. In none are available, this list is empty.
    """
    data_heading = profile_soup.find(
        re.compile('^h[123456]$', re.I),
        text=re.compile(r'^\s*Uploaded\s*data\s*$', re.I))

    data_div = data_heading.find_next_sibling()

    if not (data_div.name == 'div' and 'profile-data' in data_div['class']):
        return []

    genome_file_links = []
    uploaded_data_rows = data_div.find_all('tr')

    for row in uploaded_data_rows:
        cols = row.find_all('td')

        if len(cols) < 3:
            continue

        # Handle rows containing data from Complete Genomics
        if re.search(r'^\s*Complete Genomics\s*$', cols[2].text, flags=re.I):
            link_elem = row.find('a', text=re.compile(r'^\s*Download\s*$',
                                                      re.I))

            if not link_elem:
                continue

            link = link_elem.attrs['href']
            info = 'Complete Genomics'

            if re.search(r'/var-[^/]*.tsv.bz2', link):
                info = 'Complete Genomics (var file)'
            elif re.search(r'/masterVarBeta-[^/]*.tsv.bz2', link):
                info = 'Complete Genomics (masterVarBeta file)'

            genome_file_links.append({'link': link, 'info': info})

    return genome_file_links


def parse_survey_div(profile_soup):
    """
    Parse PGP profile to return survey data.

    input: A bs4.BeautifulSoup object generated from the HTML content
           of a PGP Harvard public profile webpage.
    returns: An array of dict objects containing survey data in this format:
             {'title': title,
              'timestamp': timestamp,
              'responses': [{'query': query, 'response': response}, ...]}
             If there is no survey data available, this list is empty.
    """
    surveys = []

    # Find survey data div.
    survey_heading = profile_soup.find(
        re.compile(r'^h[123456]$', re.I),
        text=re.compile(r'^\s*Surveys\s*$', re.I))

    surv_div = survey_heading.find_next_sibling()

    # Check if it's what we wanted (if not, return empty list).
    if not (surv_div.name == 'div' and 'profile-data' in surv_div['class']):
        return surveys

    all_rows = surv_div.find_all('tr')
    data_rows = surv_div.find_all('tr', class_=re.compile(r'^survey_result_'))
    surv_rows = [r for r in all_rows if r not in data_rows]

    for survey in surv_rows:
        title = survey.find_all('th')[0].text.strip()

        timestamp = re.search(
            r'Responses submitted ([0-9/]{8,10} [0-9:]{7,8}).',
            survey.find_all('td')[-1].text).groups()[0]

        show_res = survey.find('a', text=re.compile(r'Show responses'))

        try:
            result_id = re.search(r'jQuery\(\'\.(survey_result_[0-9]*)',
                                  show_res.get('onclick')).groups()[0]
        except AttributeError:
            continue

        result_rows = [r for r in data_rows if result_id in r['class']]

        responses = [{'query': el[0].text, 'response': el[1].text}
                     for el in [r.find_all('td') for r in result_rows]]

        survey = {
            'title': title,
            'responses': responses,
            'timestamp': timestamp
        }

        surveys.append(survey)

    return surveys


def parse_pgp_profile_page(huID):
    """
    Parse PGP Harvard public profile page, return genome links and
    survey data.

    input: (string) A PGP Harvard ID (e.g. "hu1A2B3C")

    returns: (tuple) of (genome_file_links, surveys), which are (respectively)
             the outputs from parse_uploaded_div and parse_survey_div.
    """
    url = '{}/profile/{}'.format(BASE_URL, huID)
    profile_page = requests.get(url)
    assert profile_page.status_code == 200

    profile_soup = BeautifulSoup(profile_page.text)
    genome_file_links = parse_uploaded_div(profile_soup)
    surveys = parse_survey_div(profile_soup)

    return genome_file_links, surveys


def create_pgpharvard_ohdatasets(huID,
                                 task_id=None,
                                 update_url=None,
                                 **kwargs):
    """
    Create Open Humans data sets from a PGP Harvard ID.

    Required arguments:
        huID: PGP Harvard ID (string)

    Optional arguments:
        filedir: Local filepath, folder in which to place the resulting file.
        s3_bucket_name: S3 bucket to write resulting file.
        s3_key_dir: S3 key "directory" to write resulting file. The full S3 key
                    name will add a filename to the end of s3_key_dir.

    Either 'filedir' (and no S3 arguments), or both S3 arguments (and no
    'filedir') must be specified.
    """
    print kwargs
    source = OHDataSource(name='Harvard Personal Genome Project',
                          url='{}/profile/{}'.format(BASE_URL, huID),
                          huID=huID)

    filename_genome = format_filename(source='pgp', data_type='genome')
    filename_surveys = format_filename(source='pgp', data_type='surveys')

    print 'Parsing profile...'
    genome_links, survey_data = parse_pgp_profile_page(huID)

    datasets = []

    if survey_data:
        print 'Gathering survey data...'
        dataset = get_dataset(filename_surveys, source, **kwargs)
        survey_data = StringIO(json.dumps(survey_data, indent=2,
                                          sort_keys=True) + '\n')
        dataset.add_file(
            file=survey_data,
            name=('PGPHarvard-%s-surveys-%s.json' %
                  (huID, datetime.now().strftime('%Y%m%d%H%M%S'))))
        dataset.close()
        if update_url and task_id:
            dataset.update(update_url, task_id, subtype='surveys')
        datasets.append(dataset)

    if genome_links:
        print 'Gathering genome data...'
        dataset = get_dataset(filename_genome, source, **kwargs)
        for item in genome_links:
            link = item['link']
            print 'Retrieving {}'.format(link)
            dataset.add_remote_file(url=link,
                                    file_meta={'file_type': item['info']})
        dataset.close()
        if update_url and task_id:
            dataset.update(update_url, task_id, subtype='genome')
        datasets.append(dataset)

    print 'Finished with all datasets'
    return datasets


if __name__ == '__main__':
    create_pgpharvard_ohdatasets(huID=sys.argv[1], filedir=sys.argv[2])
