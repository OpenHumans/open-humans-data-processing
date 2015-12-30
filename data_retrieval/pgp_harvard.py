"""
PGP Harvard data extraction.

Copyright (C) 2015 PersonalGenomes.org

This software is shared under the "MIT License" license (aka "Expat License"),
see LICENSE.TXT for full license text.

May be used on the command line from this project's base directory, e.g.

   python -m data_retrieval.pgp_harvard hu43860C files

...assembles data sets for the ID "hu43860C" in the "files" directory, e.g.:

   files/PGP-Harvard-surveys-hu43860C-20160102T030405Z.json
   files/PGP-Harvard-var-hu43860C-20160102T030405Z.tsv.bz2
   files/PGP-Harvard-var-hu43860C-20160102T030405Z.vcf.bz2

(These filenames includes a datetime stamp, January 2rd 2016 3:04:05am UTC.)
"""
import json
import os
import re
import shutil
import sys
import tempfile

import cgivar2gvcf
import requests

from bs4 import BeautifulSoup

from .files import get_remote_file, mv_tempfile_to_output, now_string

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

    file_links = []
    uploaded_data_rows = data_div.find_all('tr')

    for row in uploaded_data_rows:
        cols = row.find_all('td')

        if len(cols) < 3:
            continue

        file_type = cols[2].text
        source = cols[3].text
        link_elem = row.find('a', text=re.compile(r'^\s*Download\s*$',
                                                  re.I))
        if not link_elem:
            continue
        link = link_elem.attrs['href']
        file_links.append({'link': link, 'type': file_type, 'source': source})

    return file_links


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

    profile_soup = BeautifulSoup(profile_page.text, 'lxml')
    genome_file_links = parse_uploaded_div(profile_soup)
    surveys = parse_survey_div(profile_soup)

    return genome_file_links, surveys


def vcf_from_var(vcf_filename, tempdir, var_filepath):
    data_files = []
    vcf_filepath = os.path.join(tempdir, vcf_filename)
    # Determine local storage directory
    storage_dir = os.path.join(
        os.path.dirname(os.path.realpath(__file__)),
        '..', 'resource_files')
    reference, build = cgivar2gvcf.get_reference_genome_file(
        refseqdir=storage_dir, build='b37')
    cgivar2gvcf.convert_to_file(
        cgi_input=var_filepath,
        output_file=vcf_filepath,
        twobit_ref=reference,
        build=build)
    data_files.append({
        'temp_filename': vcf_filename,
        'tempdir': tempdir,
        'description': ('PGP Harvard genome, gVCF file. Derived from '
                        'Complete Genomics file format.'),
        'tags': ['vcf', 'gvcf', 'genome', 'Complete Genomics']})
    return data_files


def handle_var_file(filename, tempdir, huID):
    data_files = []
    var_description = 'PGP Harvard genome, Complete Genomics var file format.'
    new_filename = 'PGP-Harvard-var-{}-{}.tsv.bz2'.format(huID, now_string())
    new_filepath = os.path.join(tempdir, new_filename)
    shutil.move(os.path.join(tempdir, filename), new_filepath)
    data_files.append({
        'temp_filename': new_filename,
        'tempdir': tempdir,
        'description': var_description,
        'tags': ['Complete Genomics', 'var', 'genome']})

    vcf_filename = re.sub(r'\.tsv', '.vcf', new_filename)
    data_files += vcf_from_var(
        vcf_filename, tempdir, var_filepath=new_filepath)

    return data_files


def handle_mastervarbeta_file(filename, tempdir, huID):
    data_files = []
    description = ('PGP Harvard genome, Complete Genomics masterVarBeta file '
                   'format.')
    new_filename = 'PGP-Harvard-masterVarBeta-{}-{}.tsv.bz2'.format(huID, now_string())
    new_filepath = os.path.join(tempdir, new_filename)
    shutil.move(os.path.join(tempdir, filename), new_filepath)
    data_files.append(
        {'temp_filename': new_filename,
         'tempdir': tempdir,
         'description': description,
         'tags': ['Complete Genomics', 'mastervarbeta', 'genome']})
    return data_files


def make_survey_file(survey_data, tempdir, huID):
    data_files = []
    description = 'PGP Harvard survey data, JSON format.'
    survey_filename = 'PGP-Harvard-surveys-{}-{}.json'.format(huID, now_string())
    survey_filepath = os.path.join(tempdir, survey_filename)
    with open(survey_filepath, 'w') as f:
        json.dump(survey_data, f, indent=2, sort_keys=True)
    data_files.append({
        'temp_filename': survey_filename,
        'tempdir': tempdir,
        'description': description,
        'tags': ['json', 'survey']})
    return data_files


def handle_uploaded_file(filename, tempdir, huID, **kwargs):
    temp_files = []
    if re.search(r'^var-[^/]*.tsv.bz2', filename):
        temp_files += handle_var_file(
            filename, tempdir, huID, **kwargs)
    elif re.search(r'^masterVarBeta-[^/]*.tsv.bz2', filename):
        temp_files += handle_mastervarbeta_file(
            filename, tempdir, huID, **kwargs)
    else:
        # TODO: Raise an alert. We expect all Complete Genomics files from
        # the PGP to match one of the two above conditions.
        pass
    return temp_files


def create_pgpharvard_datafiles(huID,
                                task_id=None,
                                update_url=None,
                                **kwargs):
    """
    Create DataFiles for Open Humans from a PGP Harvard ID.

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
    tempdir = tempfile.mkdtemp()
    temp_files = []
    data_files = []

    file_links, survey_data = parse_pgp_profile_page(huID)

    if survey_data:
        temp_files += make_survey_file(survey_data, tempdir, huID)
    if file_links:
        print 'Gathering files...'
        for item in file_links:
            # Only handling Complete Genomics data released by PGP.
            if not (item['source'] == 'PGP' and
                    item['type'] == 'Complete Genomics'):
                continue
            filename = get_remote_file(item['link'], tempdir)
            temp_files += handle_uploaded_file(filename, tempdir, huID)

    print 'Finished creating all datasets locally.'

    for file_info in temp_files:
        print file_info
        filename = file_info['temp_filename']
        file_tempdir = file_info['tempdir']
        output_path = mv_tempfile_to_output(
            os.path.join(file_tempdir, filename), filename, **kwargs)
        if 's3_key_dir' in kwargs and 's3_bucket_name' in kwargs:
            data_files.append({
                's3_key': output_path,
                'description': file_info['description'],
                'tags': file_info['tags']})
    os.rmdir(tempdir)

    print 'Finished moving all datasets to permanent storage.'

    if not (task_id and update_url):
        return

    task_data = {'task_id': task_id,
                 's3_keys': [df['s3_key'] for df in data_files],
                 'data_files': data_files}
    status_msg = ('Updating main site ({}) with completed files for task_id={}'
                  ' with task_data:\n{}'.format(
                      update_url, task_id, json.dumps(task_data)))
    print status_msg
    requests.post(update_url, data={'task_data': json.dumps(task_data)})


if __name__ == '__main__':
    create_pgpharvard_datafiles(huID=sys.argv[1], filedir=sys.argv[2])
