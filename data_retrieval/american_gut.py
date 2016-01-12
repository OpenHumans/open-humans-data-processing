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
import shutil
import sys
import tempfile

import requests

from bs4 import BeautifulSoup

from .files import get_remote_file, mv_tempfile_to_output

SURVEYID_TO_SAMPACC_FILE = os.path.join(
    os.path.dirname(__file__),
    'american-gut',
    'survey_id_to_sample_accession.json')

EBI_STUDY_ACCESSIONS = ['ERP012803']

MAX_ATTEMPTS = 5


def get_ena_url_response(url):
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


def get_ena_info_set(accession, fields_list=None):
    """Get database information from EBI"""
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
    """Fetch sample metadata"""
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
        ebi_info_set, _ = get_ena_info_set(accession=acc,
                                           fields_list=fields_list)

        for sample_info in ebi_info_set:
            # Notes on barcodes: The standard barcode seems to be 9 digits,
            # but many don't match this pattern. Most are probably blanks and
            # other controls. To be safe, we save information for all of them.
            barcode = sample_info['library_name'].split('.')[1]

            acc_from_barcode[barcode] = sample_info['sample_accession']

    return acc_from_barcode


def dict_list_as_tsv(list_of_dicts):
    header = sorted(list_of_dicts[0].keys())
    output = '\t'.join([re.sub('\t', '    ', x) for x in header]) + '\n'
    for dict_item in list_of_dicts:
        output += '\t'.join([re.sub('\t', '    ', dict_item[x]) for
                            x in header]) + '\n'
    return output


def handle_ena_info(ena_info, tempdir, filename_base, source):
    tsv_filename = filename_base + '-ena-data.tsv'
    tsv_filepath = os.path.join(tempdir, tsv_filename)
    with open(tsv_filepath, 'w') as f:
        for line in dict_list_as_tsv(ena_info):
            f.write(line)
    json_filename = filename_base + '-ena-info.json'
    json_filepath = os.path.join(tempdir, json_filename)
    with open(json_filepath, 'w') as f:
        json.dump(ena_info, f, indent=2, sort_keys=True)
    temp_files = [{
        'temp_filename': tsv_filename,
        'tempdir': tempdir,
        'metadata': {
            'description': ('American Gut sample accession data from the '
                            'European Nucleotide Archive, TSV format.'),
            'tags': ['metadata', 'American Gut', 'tsv'],
            'sourceURL': source,
            }
    }, {
        'temp_filename': json_filename,
        'tempdir': tempdir,
        'metadata': {
            'description': ('American Gut sample accession data from the '
                            'European Nucleotide Archive, JSON format.'),
            'tags': ['metadata', 'American Gut', 'json'],
            'sourceURL': source,
        }
    }]
    return temp_files


def handle_ena_metadata(ena_metadata, tempdir, filename_base, source):
    tsv_filename = filename_base + '-ena-metadata.tsv'
    with open(os.path.join(tempdir, tsv_filename), 'w') as f:
        for line in dict_list_as_tsv([ena_metadata]):
            f.write(line)
    json_filename = filename_base + '-ena-metadata.json'
    with open(os.path.join(tempdir, json_filename), 'w') as f:
        json.dump(ena_metadata, f, indent=2, sort_keys=True)
    temp_files = [{
        'temp_filename': tsv_filename,
        'tempdir': tempdir,
        'metadata': {
            'description': ('American Gut sample survey data and metadata, '
                            'TSV format.'),
            'tags': ['metadata', 'survey', 'American Gut', 'tsv'],
            'sourceURL': source,
            }
    }, {
        'temp_filename': json_filename,
        'tempdir': tempdir,
        'metadata': {
            'description': ('American Gut sample survey data and metadata, '
                            'JSON format.'),
            'tags': ['metadata', 'survey', 'American Gut', 'json'],
            'sourceURL': source,
        }
    }]
    return temp_files


def create_amgut_datafiles(survey_ids,
                           task_id=None,
                           update_url=None,
                           sentry=None,
                           **kwargs):
    """
    Create a dataset from a set of American Gut survey IDs.

    Required arguments:
        survey_ids: List of survey IDs
        filepath OR (s3_bucket_name and s3_key_name): (see below)

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

    # For mapping survey IDs to sample accessions.
    with open(SURVEYID_TO_SAMPACC_FILE) as filedata:
        surveyid_to_sampacc = json.loads(''.join(filedata.readlines()))

    for survey_id in survey_ids:
        if survey_id not in surveyid_to_sampacc:
            # If we can't match the survey ID to sample accession, the data
            # isn't yet available in EBI. This situation might arise if the
            # sample hasn't been analyzed yet (but American Gut is still
            # offering the barcode to Open Humans). Conclusion by OH should be
            # "Data not available."
            if sentry:
                sentry.captureMessage('No sample accession data for American '
                                      'Gut survey ID: {}'.format(survey_id))
            continue

        for sampleacc in surveyid_to_sampacc[survey_id]:
            filename_base = 'American-Gut-{}'.format(sampleacc)

            # Get EBI information. Describes repository items and accessions.
            ena_info, url = get_ena_info_set(accession=sampleacc)
            temp_files += handle_ena_info(
                ena_info=ena_info,
                tempdir=tempdir,
                filename_base=filename_base,
                source=url)

            # Get and store metadata. Contains survey data.
            ena_metadata, url = fetch_metadata_xml(accession=sampleacc)
            temp_files += handle_ena_metadata(
                ena_metadata=ena_metadata,
                tempdir=tempdir,
                filename_base=filename_base,
                source=url)

            # Process to get individual read files.
            # A sample can have more than one read file if it has more than one
            # run, e.g. if the first run had unsatisfactory quality.
            for ebi_info_item in ena_info:
                fastq_url = 'http://' + ebi_info_item['fastq_ftp']
                print "Retrieving file from: {}".format(fastq_url)
                fastq_filename = filename_base + '-run-{}.fastq'.format(
                    ebi_info_item['run_accession'])
                orig_filename = get_remote_file(fastq_url, tempdir)
                if orig_filename.endswith('.gz'):
                    new_fn = fastq_filename + '.gz'
                elif orig_filename.endswith('.bz2'):
                    new_fn = fastq_filename + '.bz2'
                elif orig_filename.endswith('.zip'):
                    new_fn = fastq_filename + '.zip'
                new_fp = os.path.join(tempdir, new_fn)
                shutil.move(os.path.join(tempdir, orig_filename), new_fp)
                temp_files += [{
                    'temp_filename': new_fn,
                    'tempdir': tempdir,
                    'metadata': {
                        'description': ('American Gut 16S FASTQ sequencing '
                                        'sample survey data and metadata, '
                                        'JSON format.'),
                        'tags': ['fastq', 'American Gut', '16S'],
                        'sourceURL': fastq_url,
                        'originalFilename': orig_filename,
                    }
                }]

    print 'Finished creating all datasets locally.'

    for file_info in temp_files:
        print "File info: {}".format(str(file_info))
        filename = file_info['temp_filename']
        file_tempdir = file_info['tempdir']
        output_path = mv_tempfile_to_output(
            os.path.join(file_tempdir, filename), filename, **kwargs)
        if 's3_key_dir' in kwargs and 's3_bucket_name' in kwargs:
            data_files.append({
                's3_key': output_path,
                'metadata': file_info['metadata'],
            })
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
    if len(sys.argv) != 3:
        print 'Please specify a survey ID and directory.'
        sys.exit(1)

    create_amgut_datafiles(survey_ids=[sys.argv[1]], filedir=sys.argv[2])
