"""
American Gut ENA data extraction.

Copyright (C) 2014 PersonalGenomes.org

This software is shared under the "MIT License" license (aka "Expat License"),
see LICENSE.TXT for full license text.
"""

import json
import os
import re
import shutil

import requests

from bs4 import BeautifulSoup

from base_source import BaseSource

SURVEYID_TO_SAMPACC_FILE = os.path.join(
    os.path.dirname(__file__),
    'survey_id_to_sample_accession.json')

ENA_STUDY_ACCESSIONS = ['ERP012803']

MAX_ATTEMPTS = 5


def get_ena_url_response(url):
    """
    ENA is sometimes unresponsive, use this to try multiple times.
    """
    attempts = 0

    while attempts < MAX_ATTEMPTS:
        attempts += 1

        try:
            req = requests.get(url)

            if req.status_code == 200:
                return req
        except requests.packages.urllib3.exceptions.ProtocolError:
            continue

    return None


def get_ena_info_set(accession, fields_list=None):
    """Get database information from ENA"""
    url = ('http://www.ebi.ac.uk/ena/data/warehouse/filereport?'
           'accession=%(accession)s&result=read_run' %
           {'accession': accession})

    if fields_list:
        fields = ','.join(fields_list)
        url = url + '&fields=%(fields)s' % {'fields': fields}

    req = get_ena_url_response(url)
    ena_info = [line.split('\t') for line in req.text.split('\n') if line]
    header_data = ena_info[0]

    ena_info_set = [{header_data[i]: row[i] for i in range(len(row))}
                    for row in ena_info[1:] if len(row) == len(header_data)]

    return ena_info_set, url


def fetch_metadata_xml(accession):
    """
    Fetch sample metadata
    """
    xml_url = ('http://www.ebi.ac.uk/ena/data/view/%(acc)s&display=xml' %
               {'acc': accession})

    md_fetched = get_ena_url_response(xml_url)

    soup = BeautifulSoup(md_fetched.text, 'xml')

    return {
        attr('TAG')[0].contents[0]: (attr('VALUE')[0].contents[0]
                                     if attr('VALUE')[0].contents else None)
        for attr in soup('SAMPLE_ATTRIBUTE')
    }, xml_url


def update_surveyid_to_sampleacc(storage_filepath,
                                 study_accessions=ENA_STUDY_ACCESSIONS,
                                 max_additions=100):
    """
    Script to build the correspondence of survey IDs to sample accessions.

    In ENA, survey IDs are only available through the metadata for a sample.
    To determine which sample accessions correspond to survey IDs, we need to
    query all samples. Once we've retrieved this, we store as a file so we
    don't need to do this again.
    """
    if os.path.exists(storage_filepath):
        with open(storage_filepath) as f:
            survey_to_samples = json.load(f)
    else:
        survey_to_samples = {}

    samples_present = set([i for sl in
                           [survey_to_samples[x] for x in survey_to_samples]
                           for i in sl])

    fields_list = ['sample_accession']

    additions = 0

    for study_acc in study_accessions:
        sample_set, _ = get_ena_info_set(accession=study_acc,
                                         fields_list=fields_list)

        for sample in sample_set:
            if not sample['sample_accession']:
                continue

            if sample['sample_accession'] in samples_present:
                continue

            metadata, _ = fetch_metadata_xml(
                accession=sample['sample_accession'])
            survey_id = metadata['survey_id']

            if survey_id == 'Unknown':
                continue

            if survey_id in survey_to_samples:
                survey_to_samples[survey_id].append(sample['sample_accession'])
            else:
                survey_to_samples[survey_id] = [sample['sample_accession']]

            additions += 1

            if additions >= max_additions:
                break

        if additions >= max_additions:
            break

    with open(storage_filepath, 'w') as f:
        json.dump(survey_to_samples, f, indent=2, sort_keys=True)

    return additions


def dict_list_as_tsv(list_of_dicts):
    header = sorted(list_of_dicts[0].keys())
    output = '\t'.join([re.sub('\t', '    ', x) for x in header]) + '\n'

    for dict_item in list_of_dicts:
        output += '\t'.join([re.sub('\t', '    ', dict_item[x]) for
                             x in header]) + '\n'

    return output


class AmericanGutSource(BaseSource):
    """
    Create a dataset from a set of American Gut survey IDs.

    Required arguments:
        survey_ids: List of survey IDs
    """

    source = 'american_gut'

    def handle_ena_info(self, ena_info, filename_base, source):
        tsv_filename = filename_base + '-ena-info.tsv'
        tsv_filepath = self.temp_join(tsv_filename)

        with open(tsv_filepath, 'w') as f:
            for line in dict_list_as_tsv(ena_info):
                f.write(line)

        json_filename = filename_base + '-ena-info.json'
        json_filepath = self.temp_join(json_filename)

        with open(json_filepath, 'w') as f:
            json.dump(ena_info, f, indent=2, sort_keys=True)

        self.temp_files.append({
            'temp_filename': tsv_filename,
            'metadata': {
                'description': ('American Gut sample accession data from the '
                                'European Nucleotide Archive, TSV format.'),
                'tags': ['metadata', 'American Gut', 'tsv'],
                'sourceURL': source,
                }
        })

        self.temp_files.append({
            'temp_filename': json_filename,
            'metadata': {
                'description': ('American Gut sample accession data from the '
                                'European Nucleotide Archive, JSON format.'),
                'tags': ['metadata', 'American Gut', 'json'],
                'sourceURL': source,
            }
        })

    def handle_ena_metadata(self, ena_metadata, filename_base, source):
        tsv_filename = filename_base + '-metadata.tsv'

        with open(self.temp_join(tsv_filename), 'w') as f:
            for line in dict_list_as_tsv([ena_metadata]):
                f.write(line)

        json_filename = filename_base + '-metadata.json'

        with open(self.temp_join(json_filename), 'w') as f:
            json.dump(ena_metadata, f, indent=2, sort_keys=True)

        self.temp_files.append({
            'temp_filename': tsv_filename,
            'metadata': {
                'description': ('American Gut sample survey data and '
                                'metadata, TSV format.'),
                'tags': ['metadata', 'survey', 'American Gut', 'tsv'],
                'sourceURL': source,
            }
        })

        self.temp_files.append({
            'temp_filename': json_filename,
            'metadata': {
                'description': ('American Gut sample survey data and '
                                'metadata, JSON format.'),
                'tags': ['metadata', 'survey', 'American Gut', 'json'],
                'sourceURL': source,
            }
        })

    def create_files(self):
        # For mapping survey IDs to sample accessions.
        with open(SURVEYID_TO_SAMPACC_FILE) as filedata:
            surveyid_to_sampacc = json.loads(''.join(filedata.readlines()))

        for survey_id in self.survey_ids:
            if survey_id not in surveyid_to_sampacc:
                # If we can't match the survey ID to sample accession, the data
                # isn't yet available in ENA. This situation might arise if the
                # sample hasn't been analyzed yet (but American Gut is still
                # offering the barcode to Open Humans). Conclusion by OH should
                # be "Data not available."
                self.sentry_log('No sample accession data for American '
                                'Gut survey ID: {}'.format(survey_id))

                continue

            for sampleacc in surveyid_to_sampacc[survey_id]:
                filename_base = 'American-Gut-{}'.format(sampleacc)

                # Get ENA information. Describes repository items and
                # accessions.
                ena_info, url = get_ena_info_set(accession=sampleacc)

                self.handle_ena_info(ena_info=ena_info,
                                     filename_base=filename_base,
                                     source=url)

                # Get and store metadata. Contains survey data.
                ena_metadata, url = fetch_metadata_xml(accession=sampleacc)

                self.handle_ena_metadata(ena_metadata=ena_metadata,
                                         filename_base=filename_base,
                                         source=url)

                # Process to get individual read files. A sample can have more
                # than one read file if it has more than one run, e.g. if the
                # first run had unsatisfactory quality.
                for ena_info_item in ena_info:
                    fastq_url = 'http://' + ena_info_item['fastq_ftp']

                    fastq_filename = '{}-run-{}.fastq'.format(
                        filename_base,
                        ena_info_item['run_accession'])

                    original_filename = self.get_remote_file(fastq_url)

                    if original_filename.endswith('.gz'):
                        new_filename = fastq_filename + '.gz'
                    elif original_filename.endswith('.bz2'):
                        new_filename = fastq_filename + '.bz2'
                    elif original_filename.endswith('.zip'):
                        new_filename = fastq_filename + '.zip'

                    shutil.move(self.temp_join(original_filename),
                                self.temp_join(new_filename))

                    self.temp_files.append({
                        'temp_filename': new_filename,
                        'metadata': {
                            'description': ('American Gut 16S FASTQ raw '
                                            'sequencing data.'),
                            'tags': ['fastq', 'American Gut', '16S'],
                            'sourceURL': fastq_url,
                            'originalFilename': original_filename,
                        }
                    })
