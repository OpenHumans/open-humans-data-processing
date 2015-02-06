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
from datetime import datetime
import json
import os
import re
import sys
import tempfile

from bs4 import BeautifulSoup
import requests

from .participant_data_set import OHDataSet, OHDataSource, S3OHDataSet


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
        text=re.compile('^\s*Uploaded\s*data\s*$', re.I))
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
        if re.search(r'^\s*Complete Genomics\s*$', cols[2].text,
                     flags=re.I):
            link_elem = row.find(
                'a', text=re.compile(r'^\s*Download\s*$', re.I))
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
        responses = [{'query': el[0].text, 'response': el[1].text} for el in
                     [r.find_all('td') for r in result_rows]]
        survey = {'title': title,
                  'responses': responses,
                  'timestamp': timestamp}
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
    url = 'http://my.pgp-hms.org/profile/%s' % huID
    profile_page = requests.get(url)
    assert profile_page.status_code == 200

    profile_soup = BeautifulSoup(profile_page.text)
    genome_file_links = parse_uploaded_div(profile_soup)
    surveys = parse_survey_div(profile_soup)

    return genome_file_links, surveys


def create_pgpharvard_ohdatasets(huID,
                                 filedir=None,
                                 s3_bucket_name=None,
                                 s3_key_dir=None,
                                 task_id=None,
                                 update_url=None):
    """Create Open Humans data sets from a PGP Harvard ID.

    Required arguments:
        huID: PGP Harvard ID (string)
        filedir OR (s3_bucket_dir and s3_key_name): (see below)

    Optional arguments:
        filedir: Local filepath, folder in which to place the resulting file.
        s3_bucket_name: S3 bucket to write resulting file.
        s3_key_dir: S3 key "directory" to write resulting file. The full S3 key
                    name will add a filename to the end of s3_key_dir.

    Either 'filedir' (and no S3 arguments), or both S3 arguments (and no
    'filedir') must be specified.
    """
    filedir_used = filedir and not (s3_bucket_name or s3_key_dir)
    s3_used = (s3_bucket_name and s3_key_dir) and not filedir
    # This is an XOR assertion.
    assert filedir_used != s3_used, "Specific filedir OR s3 info, not both."

    source = OHDataSource(name='Harvard Personal Genome Project',
                          url='http://my.pgp-hms.org/profile/%s' % huID)

    filename_genome = ('PGPHarvard-%s-genome-%s.tar.gz' %
                       (huID, datetime.now().strftime('%Y%m%d%H%M%S')))
    filename_surveys = ('PGPHarvard-%s-surveys-%s.tar.gz' %
                        (huID, datetime.now().strftime('%Y%m%d%H%M%S')))

    genome_links, survey_data = parse_pgp_profile_page(huID)

    created_s3_keys = []

    if survey_data:
        if filedir_used:
            filepath = os.path.join(filedir, filename_surveys)
            dataset = OHDataSet(mode='w', source=source, filepath=filepath)
        elif s3_used:
            s3_key_name = os.path.join(s3_key_dir, filename_surveys)
            dataset = S3OHDataSet(mode='w', source=source,
                                  s3_bucket_name=s3_bucket_name,
                                  s3_key_name=s3_key_name)
            created_s3_keys.append(s3_key_name)
        with tempfile.TemporaryFile() as survey_data_file:
            survey_data_file.write(json.dumps(survey_data,
                                   indent=2, sort_keys=True) + '\n')
            survey_data_file.seek(0)
            dataset.add_file(
                file=survey_data_file,
                name=('PGPHarvard-%s-surveys-%s.json' %
                      (huID, datetime.now().strftime('%Y%m%d%H%M%S'))))
        dataset.close()
    if genome_links:
        if filedir_used:
            filepath = os.path.join(filedir, filename_genome)
            dataset = OHDataSet(mode='w', source=source, filepath=filepath)
        elif s3_used:
            s3_key_name = os.path.join(s3_key_dir, filename_genome)
            dataset = S3OHDataSet(mode='w', source=source,
                                  s3_bucket_name=s3_bucket_name,
                                  s3_key_name=s3_key_name)
            created_s3_keys.append(s3_key_name)
        for item in genome_links:
            link = item['link']
            dataset.add_remote_file(url=link,
                                    file_meta={'file_type': item['info']})
        dataset.close()
    if task_id and update_url and s3_used and created_s3_keys:
        print ("Updating main site (%s) with " % update_url +
               "completed files for task_id=%s." % task_id)
        task_data = {
            'task_id': task_id,
            's3_keys': created_s3_keys,
        }
        requests.post(update_url,
                      data={'task_data': json.dumps(task_data)})


if __name__ == "__main__":
    create_pgpharvard_ohdatasets(huID=sys.argv[1], filedir=sys.argv[2])
