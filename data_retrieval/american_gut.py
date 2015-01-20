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
import re
import sys
import tempfile

import requests
from bs4 import BeautifulSoup

from .participant_data_set import OHDataSource, OHDataSet, S3OHDataSet

BARCODE_TO_SAMPACC_FILE = 'american_gut_barcode_to_sample_accession.json'

EBI_STUDY_ACCESSIONS = ['ERP003819',
                        'ERP003820',
                        'ERP003821',
                        'ERP003822',
                        'ERP005361',
                        'ERP005362',
                        'ERP005366',
                        'ERP005367',
                        ]

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
    url = ("http://www.ebi.ac.uk/ena/data/warehouse/filereport?" +
           "accession=%(accession)s&" % {'accession': accession} +
           "&result=read_run")
    if fields_list:
        fields = ','.join(fields_list)
        url = url + "&fields=%(fields)s" % {'fields': fields}
    req = get_ebi_url_response(url)
    ebi_data = [line.split('\t') for line in req.text.split('\n')]
    header_data = ebi_data[0]
    ebi_info_set = [{header_data[i]: row[i] for i in range(len(row))}
                    for row in ebi_data[1:] if len(row) == len(header_data)]
    return ebi_info_set


def fetch_metadata_xml(accession):
    """Fetch sample metadata"""
    xml_url = ("http://www.ebi.ac.uk/ena/data/view/%(acc)s&display=xml" %
               {'acc': accession})
    md_fetched = get_ebi_url_response(xml_url)
    soup = BeautifulSoup(md_fetched.text, 'xml')
    return {attr('TAG')[0].contents[0]: attr('VALUE')[0].contents[0] if
            attr('VALUE')[0].contents else None for
            attr in soup('SAMPLE_ATTRIBUTE')}


def _get_all_barcodes(accessions=EBI_STUDY_ACCESSIONS):
    """Get barcodes for each sample accession in EBI data.

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


def create_amgut_ohdataset(barcode, filepath=None, s3_bucket_name=None,
                           s3_key_name=None, **kwargs):
    """Create an Open Humans data set from an American Gut sample barcode.

    Required arguments:
        barcode: EBI sample barcode
        filepath OR (s3_bucket_name and s3_key_name): (see below)

    Optional arguments:
        filepath: Local filepath to write resulting file. Must end with ".tar",
                  ".tar.gz", or ".tar.bz2".
        s3_bucket_name: S3 bucket to write resulting file.
        s3_key_name: S3 key to write resulting file. Must end with ".tar",
                  ".tar.gz", or ".tar.bz2"

    Either 'filepath' (and no S3 arguments), or both S3 arguments (and no
    'filepath') must be specified.
    """

    filepath_used = filepath and not (s3_bucket_name or s3_key_name)
    s3_used = (s3_bucket_name and s3_key_name) and not filepath
    # This is an XOR assertion.
    assert filepath_used != s3_used, "Specific filepath OR s3 info, not both."

    source = OHDataSource(name='American Gut',
                          url='http://microbio.me/americangut/')
    if filepath_used:
        dataset = OHDataSet(mode='w', source=source, filepath=filepath)
    elif s3_used:
        dataset = S3OHDataSet(mode='w', source=source,
                              s3_bucket_name=s3_bucket_name,
                              s3_key_name=s3_key_name)
    # Use barcode to get sample accession.
    with open(BARCODE_TO_SAMPACC_FILE) as filedata:
        barcode_to_sampacc = json.loads(''.join(filedata.readlines()))

    # Pull data from EBI.
    print "Adding ebi_information.json file"
    ebi_information = get_ebi_info_set(accession=barcode_to_sampacc[barcode])
    with tempfile.TemporaryFile() as ebi_information_file:
        ebi_information_file.write(json.dumps(ebi_information[0],
                                   indent=2, sort_keys=True) + '\n')
        ebi_information_file.seek(0)
        dataset.add_file(file=ebi_information_file,
                         name='ebi_information.json')

    print "Adding ebi_metadata.tsv file"
    ebi_metadata = fetch_metadata_xml(accession=barcode_to_sampacc[barcode])
    with tempfile.TemporaryFile() as ebi_metadata_tsv_file:
        keys = sorted(ebi_metadata.keys())
        # Unclear if incoming data is clean, so pro-actively removing tabs.
        header = '#' + '\t'.join([re.sub('\t', '    ', k) for k in keys])
        ebi_metadata_tsv_file.write(header + '\n')
        values = '\t'.join([re.sub('\t', '    ', ebi_metadata[k]) for
                            k in keys])
        ebi_metadata_tsv_file.write(values + '\n')
        ebi_metadata_tsv_file.seek(0)
        dataset.add_file(file=ebi_metadata_tsv_file, name='ebi_metadata.tsv')

    print "Adding ebi_metadata.json file"
    with tempfile.TemporaryFile() as ebi_metadata_json_file:
        ebi_metadata_json_file.write(json.dumps(ebi_metadata,
                                     indent=2, sort_keys=True) + '\n')
        ebi_metadata_json_file.seek(0)
        dataset.add_file(file=ebi_metadata_json_file, name='ebi_metadata.json')

    fastq_url = 'http://' + ebi_information[0]['submitted_ftp']
    print "Adding remote file from " + fastq_url
    dataset.add_remote_file(url=fastq_url)

    dataset.close()

if __name__ == "__main__":
    create_amgut_ohdataset(barcode=sys.argv[1], filepath=sys.argv[2])
