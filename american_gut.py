"""
American Gut participant data extraction.

This software is shared under the "MIT License" license (aka "Expat License"),
copied below.

Copyright (C) 2014 Madeleine Price Ball

Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
of the Software, and to permit persons to whom the Software is furnished to do
so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""

import json
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
        url = url + "fields=%(fields)s" % {'fields': fields}
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


def get_info_studies(study_accessions=EBI_STUDY_ACCESSIONS):
    """Get information from combined American Gut studies

    Returns a dict containing three key/values:

      'samples':         dict of dicts. Keys are sample_accession, values are
                         a dict containing EBI standard fields.

      'sample_metadata': dict of dicts. Keys are sample_accession, values are
                         SAMPLE_ATTRIBUTES parsed from EBI XML.

      'participants':    dict of dicts. Keys are host_subject_id (as seen in
                         sample_metadata), value is list of sample_accessions.
    """
    samples = dict()
    sample_metadata = dict()
    participants = dict()
    for acc in study_accessions:
        ebi_info_set = get_ebi_info_set(accession=acc)
        for sample_info in ebi_info_set:
            sample_acc = sample_info['sample_accession']
            samples[sample_acc] = sample_info
            sample_metadata[sample_acc] = fetch_metadata_xml(sample_acc)
            host_subject_id = sample_metadata[sample_acc]['host_subject_id']
            if host_subject_id in participants:
                participants[host_subject_id].append(sample_acc)
            else:
                participants[host_subject_id] = [sample_acc]
    output = {'samples': samples,
              'sample_metadata': sample_metadata,
              'participants': participants}
    return output

if __name__ == "__main__":
    output = get_info_studies()
    print json.dumps(output, sort_keys=True, indent=2)
