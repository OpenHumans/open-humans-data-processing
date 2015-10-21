"""
American Gut EBI metadata extraction.

Copyright (C) 2014 PersonalGenomes.org

This software is shared under the "MIT License" license (aka "Expat License"),
see LICENSE.TXT for full license text.

May be used on the command line from this project's base directory, e.g.

   python -m data_retrieval.american_gut 000007080 files

...will assemble a data set for the barcode 000007080 at:

   files/AmericanGut-000007080-dataset.tar.gz
"""

import json
import os
import re
import sys

from cStringIO import StringIO

import requests

from bs4 import BeautifulSoup

from .participant_data_set import format_filename, get_dataset, OHDataSource

SURVEYID_TO_SAMPACC_FILE = os.path.join(
    os.path.dirname(__file__),
    'american-gut',
    'survey_id_to_sample_accession.json')

EBI_STUDY_ACCESSIONS = ['ERP012803']

MAX_ATTEMPTS = 5


def get_ebi_url_response(url):
    """EBI sometimes unresponsive, use this to try multiple times."""
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


def get_ebi_info_set(accession, fields_list=None):
    """Get database information from EBI"""
    url = ('http://www.ebi.ac.uk/ena/data/warehouse/filereport?'
           'accession=%(accession)s&result=read_run' %
           {'accession': accession})

    if fields_list:
        fields = ','.join(fields_list)
        url = url + '&fields=%(fields)s' % {'fields': fields}

    req = get_ebi_url_response(url)
    ebi_data = [line.split('\t') for line in req.text.split('\n') if line]
    header_data = ebi_data[0]

    ebi_info_set = [{header_data[i]: row[i] for i in range(len(row))}
                    for row in ebi_data[1:] if len(row) == len(header_data)]

    return ebi_info_set


def fetch_metadata_xml(accession):
    """Fetch sample metadata"""
    xml_url = ('http://www.ebi.ac.uk/ena/data/view/%(acc)s&display=xml' %
               {'acc': accession})

    md_fetched = get_ebi_url_response(xml_url)

    soup = BeautifulSoup(md_fetched.text, 'xml')

    return {
        attr('TAG')[0].contents[0]: (attr('VALUE')[0].contents[0]
                                     if attr('VALUE')[0].contents else None)
        for attr in soup('SAMPLE_ATTRIBUTE')
    }


def update_surveyid_to_sampleacc(storage_filepath,
                                 study_accessions=EBI_STUDY_ACCESSIONS,
                                 max_additions=100):
    """
    Script to build the correspondence of survey IDs to sample accessions.

    On EBI, survey IDs are only available through the metadata for a sample.
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
        sample_set = get_ebi_info_set(accession=study_acc,
                                      fields_list=fields_list)
        for sample in sample_set:
            if not sample['sample_accession']:
                continue
            if sample['sample_accession'] in samples_present:
                continue
            metadata = fetch_metadata_xml(accession=sample['sample_accession'])
            survey_id = metadata['survey_id']
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
        json.dump(survey_to_samples, f, indent=2)

    return additions


def _get_all_barcodes(accessions=EBI_STUDY_ACCESSIONS):
    """
    Get barcodes for each sample accession in EBI data.

    Barcodes are used to look up American Gut samples and associated
    information for creating a dataset for Open Humans.

    Returns a dict where keys are barcodes, values are sample accessions.
    """
    acc_from_barcode = {}
    fields_list = ['sample_accession', 'library_name']

    for acc in accessions:
        ebi_info_set = get_ebi_info_set(accession=acc, fields_list=fields_list)

        for sample_info in ebi_info_set:
            # Notes on barcodes: The standard barcode seems to be 9 digits,
            # but many don't match this pattern. Most are probably blanks and
            # other controls. To be safe, we save information for all of them.
            barcode = sample_info['library_name'].split(':')[0]

            acc_from_barcode[barcode] = sample_info['sample_accession']

    return acc_from_barcode


def create_amgut_ohdataset(barcode,
                           source,
                           task_id=None,
                           update_url=None,
                           **kwargs):
    """
    Create a dataset from an American Gut barcode.
    """
    # For mapping survey IDs to sample accessions.
    with open(SURVEYID_TO_SAMPACC_FILE) as filedata:
        surveyid_to_sampacc = json.loads(''.join(filedata.readlines()))

    identifier = 'sample-{}'.format(barcode)
    filename = format_filename('american-gut', identifier, 'microbiome-16S')

    dataset = get_dataset(filename, source, **kwargs)

    # Pull data from EBI.
    print 'Adding ebi_information.json file'

    try:
        ebi_information = get_ebi_info_set(
            accession=surveyid_to_sampacc[barcode])[0]
    except KeyError:
        # If we can't match the survey ID to sample accession, the data isn't
        # yet available in EBI. This situation might arise if the sample hasn't
        # been analyzed yet (but American Gut is still offering the barcode to
        # Open Humans). Conclusion by OH should be "Data not available."
        return

    ebi_information_string = StringIO(json.dumps(ebi_information, indent=2,
                                                 sort_keys=True) + '\n')

    dataset.add_file(file=ebi_information_string, name='ebi_information.json')

    print 'Adding ebi_metadata.tsv file'

    ebi_metadata = fetch_metadata_xml(accession=barcode_to_sampacc[barcode])

    keys = sorted(ebi_metadata.keys())

    # Unclear if incoming data is clean, so pro-actively removing tabs.
    header = '#' + '\t'.join([re.sub('\t', '    ', k) for k in keys])
    values = '\t'.join([re.sub('\t', '    ', ebi_metadata[k]) for k in keys])

    ebi_metadata_string = StringIO(header + '\n' + values + '\n')

    dataset.add_file(file=ebi_metadata_string, name='ebi_metadata.tsv')

    print 'Adding ebi_metadata.json file'

    ebi_metadata_json = StringIO(json.dumps(ebi_metadata, indent=2,
                                            sort_keys=True) + '\n')

    dataset.add_file(file=ebi_metadata_json, name='ebi_metadata.json')

    fastq_url = 'http://' + ebi_information['submitted_ftp']

    print 'Adding remote file from ' + fastq_url

    dataset.add_remote_file(url=fastq_url)
    dataset.metadata['american_gut_sample_barcode'] = barcode
    dataset.close()

    dataset.update(update_url, task_id)

    return dataset


def create_amgut_ohdatasets(barcodes,
                            task_id=None,
                            update_url=None,
                            **kwargs):
    """
    Create Open Humans data sets from an American Gut sample barcode.

    Required arguments:
        barcodes: List of EBI sample barcode
        filepath OR (s3_bucket_name and s3_key_name): (see below)

    Optional arguments:
        filedir: Local filepath, folder in which to place the resulting file.
        s3_bucket_name: S3 bucket to write resulting file.
        s3_key_dir: S3 key "directory" to write resulting file. The full S3 key
                    name will add a filename to the end of s3_key_dir.

    Either 'filedir' (and no S3 arguments), or both S3 arguments (and no
    'filedir') must be specified.
    """
    source = OHDataSource(name='American Gut',
                          url='https://microbio.me/americangut/')

    return [
        create_amgut_ohdataset(barcode, source, task_id, update_url, **kwargs)
        for barcode in barcodes
    ]


if __name__ == '__main__':
    if len(sys.argv) != 3:
        print 'Please specify a barcode and directory.'

        sys.exit(1)

    create_amgut_ohdatasets(barcodes=[sys.argv[1]], filedir=sys.argv[2])
