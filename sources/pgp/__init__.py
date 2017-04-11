"""
PGP Harvard data extraction.

Copyright (C) 2015 PersonalGenomes.org

This software is shared under the "MIT License" license (aka "Expat License"),
see LICENSE.TXT for full license text.

May be used on the command line from this project's base directory, e.g.

   python -m sources.pgp hu43860C files

...assembles data sets for the ID "hu43860C" in the "files" directory, e.g.:

   files/PGP-Harvard-hu43860C-surveys.json
   files/PGP-Harvard-hu43860C-var.tsv.bz2
   files/PGP-Harvard-hu43860C-var.vcf.bz2
   files/PGP-Harvard-hu43860C-masterVarBeta.tsv.bz2

(These filenames includes a datetime stamp, January 2rd 2016 3:04:05am UTC.)
"""

import datetime
import json
import logging
import os
import re
import shutil

import arrow
import cgivar2gvcf
import requests

from bs4 import BeautifulSoup

from base_source import BaseSource

logger = logging.getLogger(__name__)

BASE_URL = 'https://my.pgp-hms.org'

REFRESH_DAYS = 180

class PGPSource(BaseSource):
    """
    Create DataFiles for Open Humans from a PGP Harvard ID.

    Required arguments:
        hu_id: PGP Harvard ID (string)
    """

    source = 'pgp'

    def __init__(self, *args, **kwargs):
        if 'hu_id' in kwargs:
            self.hu_id = kwargs['hu_id']
        super(PGPSource, self).__init__(*args, **kwargs)

    def should_update(self, files):
        update = False
        file_links, survey_data, profile_url = self.parse_pgp_profile_page()
        source_urls = [x['link'] for x in file_links]
        for item in files:
            if 'survey' in item['metadata']['tags']:
                timedelta = arrow.get() - arrow.get(item['created'])
                if timedelta > datetime.timedelta(days=REFRESH_DAYS):
                    update = True
            elif 'sourceURL' in item['metadata']:
                if item['metadata']['sourceURL'] not in source_urls:
                    update = True
        if update:
            logger.info('Updating PGP data for {} (user id: {})...'.format(
                self.oh_username, self.oh_user_id))
        else:
            logger.info('No PGP update needed for {} (user id: {}).'.format(
                self.oh_username, self.oh_user_id))
        return update

    @staticmethod
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

        if not (data_div.name == 'div' and
                'profile-data' in data_div['class']):
            return []

        file_links = []
        uploaded_data_rows = data_div.find_all('tr')

        for row in uploaded_data_rows:
            cols = row.find_all('td')

            if len(cols) < 3:
                continue

            file_type = cols[2].text
            source = cols[3].text
            link_elem = row.find('a',
                                 text=re.compile(r'^\s*Download\s*$', re.I))
            if not link_elem:
                continue

            link = link_elem.attrs['href']

            file_links.append({
                'link': link,
                'type': file_type,
                'source': source
            })

        return file_links

    @staticmethod
    def parse_survey_div(profile_soup):
        """
        Parse PGP profile to return survey data.

        input: A bs4.BeautifulSoup object generated from the HTML content
               of a PGP Harvard public profile webpage.
        returns: An array of dict objects containing survey data in this
                 format:
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
        if not (surv_div.name == 'div' and
                'profile-data' in surv_div['class']):
            return surveys

        all_rows = surv_div.find_all('tr')
        data_rows = surv_div.find_all('tr',
                                      class_=re.compile(r'^survey_result_'))
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

    def parse_pgp_profile_page(self):
        """
        Parse PGP Harvard public profile page, return genome links and
        survey data.

        input: (string) A PGP Harvard ID (e.g. "hu1A2B3C")

        returns: (tuple) of (genome_file_links, surveys), which are
                 (respectively) the outputs from parse_uploaded_div and
                 parse_survey_div.
        """
        url = '{}/profile/{}'.format(BASE_URL, self.hu_id)
        profile_page = requests.get(url)

        assert profile_page.status_code == 200

        profile_soup = BeautifulSoup(profile_page.text, 'lxml')

        genome_file_links = self.parse_uploaded_div(profile_soup)
        surveys = self.parse_survey_div(profile_soup)

        return genome_file_links, surveys, url

    def vcf_from_var(self, vcf_filename, var_filepath):
        """
        Generate VCF from Complete Genomics var file.

        Returns temp file info as array of dicts. Only one dict expected.
        """
        vcf_filepath = os.path.join(self.temp_directory, vcf_filename)

        # Determine local storage directory
        storage_dir = os.path.join(
            os.path.dirname(os.path.realpath(__file__)), 'resources')
        reference, twobit_name = cgivar2gvcf.get_reference_genome_file(
            refseqdir=storage_dir, build='b37')

        # TODO: Mock this for performing tests. This is extremely slow.
        cgivar2gvcf.convert_to_file(
            cgi_input=var_filepath,
            output_file=vcf_filepath,
            twobit_ref=reference,
            twobit_name=twobit_name,
            var_only=True)

        self.temp_files.append({
            'temp_filename': vcf_filename,
            'metadata': {
                'description': ('PGP Harvard genome, VCF file. Derived from '
                                'Complete Genomics var file.'),
                'tags': ['vcf', 'genome', 'Complete Genomics'],
            }
        })

    def handle_var_file(self, filename, source):
        """
        Rename var data file from PGP Harvard genome data, generate VCF.

        Returns temp file info as array of dicts.
        """
        var_description = ('PGP Harvard genome, Complete Genomics var file '
                           'format.')
        new_filename = 'PGP-Harvard-{}-var.tsv'.format(self.hu_id)

        if filename.endswith('.bz2'):
            new_filename += '.bz2'
        elif filename.endswith('.gz'):
            new_filename += '.gz'

        new_filepath = os.path.join(self.temp_directory, new_filename)

        shutil.move(os.path.join(self.temp_directory, filename), new_filepath)

        self.temp_files.append({
            'temp_filename': new_filename,
            'metadata': {
                'description': var_description,
                'tags': ['Complete Genomics', 'var', 'genome'],
                'sourceURL': source,
                'originalFilename': filename,
            },
        })

        vcf_filename = re.sub(r'\.tsv', '.vcf', new_filename)

        if not (vcf_filename.endswith('.gz') or vcf_filename.endswith('.bz2')):
            vcf_filename += '.bz2'

        self.vcf_from_var(vcf_filename, var_filepath=new_filepath)

    def handle_mastervarbeta_file(self, filename, source):
        """
        Rename masterVarBeta data file from PGP Harvard genome data.

        Returns temp file info as array of dicts. Only one dict expected.
        """
        description = ('PGP Harvard genome, Complete Genomics masterVarBeta '
                       'file format.')
        new_filename = 'PGP-Harvard-{}-masterVarBeta.tsv'.format(self.hu_id)

        if filename.endswith('.bz2'):
            new_filename += '.bz2'
        elif filename.endswith('.gz'):
            new_filename += '.gz'

        new_filepath = os.path.join(self.temp_directory, new_filename)

        shutil.move(os.path.join(self.temp_directory, filename), new_filepath)

        self.temp_files.append({
            'temp_filename': new_filename,
            'metadata': {
                'description': description,
                'tags': ['Complete Genomics', 'mastervarbeta', 'genome'],
                'sourceURL': source,
                'originalFilename': filename,
            },
        })

    def make_survey_file(self, survey_data, source):
        """
        Create survey data file from PGP Harvard survey data.

        Returns temp file info as array of dicts. Only one dict expected.
        """
        description = 'PGP Harvard survey data, JSON format.'
        survey_filename = 'PGP-Harvard-{}-surveys.json'.format(self.hu_id)
        survey_filepath = os.path.join(self.temp_directory, survey_filename)

        with open(survey_filepath, 'w') as f:
            json.dump(survey_data, f, indent=2, sort_keys=True)

        self.temp_files.append({
            'temp_filename': survey_filename,
            'metadata': {
                'description': description,
                'tags': ['json', 'survey'],
                'sourceURL': source,
            },
        })

    def handle_uploaded_file(self, filename, source, **kwargs):
        if re.search(r'^var-[^/]*.tsv.bz2', filename):
            self.handle_var_file(filename, source, **kwargs)
        elif re.search(r'^masterVarBeta-[^/]*.tsv.bz2', filename):
            self.handle_mastervarbeta_file(filename, source, **kwargs)
        elif re.search(r'^GS00253-DNA[^/]*.tsv.bz2', filename):
            self.handle_var_file(filename, source, **kwargs)
        else:
            # We've had one case of an old file not matching standard name
            # format.  For this person there is a more recent file, so we'll
            # just skip it.
            if filename == 'GS000005532-ASM.tsv.bz2':
                pass

            if filename == 'genome_download.php':
                raise IOError(
                    'Filename "genome_download.php" for PGP genome '
                    'indicates a broken link to the genome data file for {}! '
                    'Aborting data retrieval.'.format(self.hu_id))

            self.sentry_log('PGP Complete Genomics filename in '
                            'unexpected format: {}'.format(filename))

    def create_files(self):
        file_links, survey_data, profile_url = self.parse_pgp_profile_page()

        if survey_data:
            self.make_survey_file(survey_data, source=profile_url)

        if file_links:
            for item in file_links:
                # Only handling Complete Genomics data released by PGP.
                if not (item['source'] == 'PGP' and
                        item['type'] == 'Complete Genomics'):
                    continue

                # TODO: Mock this for performing tests. This is slow.
                filename = self.get_remote_file(item['link'])

                self.handle_uploaded_file(filename, source=item['link'])
