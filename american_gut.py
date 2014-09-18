"""
American Gut EBI metadata extraction.

Copyright (C) 2014 Madeleine Price Ball

This software is shared under the "MIT License" license (aka "Expat License"),
see LICENSE.TXT for full license text.
"""
import bz2
import gzip
import json
import os
import re

import requests
from bs4 import BeautifulSoup

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
    fields_list = ['sample_accession', 'submitted_ftp']
    fastq_name_regex = re.compile(r'seqs_(?P<barcode>[0-9]+)' +
                                  r'\..*(|\.gz|\.bz2)$')
    for acc in accessions:
        ebi_info_set = get_ebi_info_set(accession=acc, fields_list=fields_list)
        for sample_info in ebi_info_set[1:]:
            filename = os.path.basename(sample_info['submitted_ftp'])
            try:
                barcode = fastq_name_regex.match(filename).group('barcode')
            except AttributeError:
                continue
            acc_from_barcode[barcode] = sample_info['sample_accession']
    return acc_from_barcode

if __name__ == "__main__":
    acc_from_barcode = _get_all_barcodes()
    print json.dumps(acc_from_barcode, indent=2)
