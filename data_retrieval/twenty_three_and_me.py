r"""
23andMe genotyping data extraction.

Copyright (C) 2014 PersonalGenomes.org

This software is shared under the "MIT License" license (aka "Expat License"),
see LICENSE.TXT for full license text.

May be used on the command line. For example, the following command:

    python -m data_retrieval.twentythree_and_me <23andme_token> \
        <23andme_profile_id> files

Will assemble a data set in files/
"""
import bz2
import gzip
import json
import os
import re
import requests
import shutil
import sys
import tempfile
import zipfile

from cStringIO import StringIO
from datetime import date, datetime

from boto.s3.connection import S3Connection

from .files import get_remote_file, mv_tempfile_to_output, now_string

REF_23ANDME_FILE = os.path.join(
    os.path.dirname(__file__), '23andme', 'reference_b37.txt')

# Was used to generate reference genotypes in the previous file.
REFERENCE_GENOME_URL = ('http://hgdownload-test.cse.ucsc.edu/' +
                        'goldenPath/hg19/bigZips/hg19.2bit')

VCF_FIELDS = ['CHROM', 'POS', 'ID', 'REF', 'ALT', 'QUAL', 'FILTER',
              'INFO', 'FORMAT', '23ANDME_DATA']


def s3_connection():
    """
    Get an S3 connection using environment variables.
    """
    key = os.getenv('AWS_ACCESS_KEY_ID')
    secret = os.getenv('AWS_SECRET_ACCESS_KEY')

    if not (key and secret):
        raise Exception('You must specify AWS credentials.')

    return S3Connection(key, secret)


def vcf_header(source=None, reference=None, format_info=None):
    """Generate a VCF header."""
    header = []
    today = date.today()
    header.append('##fileformat=VCFv4.1')
    header.append('##fileDate=%s%s%s' % (str(today.year),
                                         str(today.month).zfill(2),
                                         str(today.day).zfill(2)))
    if source:
        header.append('##source=' + source)
    if reference:
        header.append('##reference=%s' % reference)
    for item in format_info:
        header.append('##FORMAT=' + item)
    header.append('#' + '\t'.join(VCF_FIELDS))
    return header


def vcf_from_raw_23andme(raw_23andme):
    output = StringIO()
    reference = dict()
    with open(REF_23ANDME_FILE) as f:
        for line in f:
            data = line.rstrip().split('\t')
            if data[0] not in reference:
                reference[data[0]] = dict()
            reference[data[0]][data[1]] = data[2]
    header = vcf_header(
        source='open_humans_data_processing.twenty_three_and_me',
        reference=REFERENCE_GENOME_URL,
        format_info=['<ID=GT,Number=1,Type=String,Description="Genotype">']
    )
    for line in header:
        output.write(line + '\n')
    for line in raw_23andme:
        # Skip header
        if line.startswith('#'):
            continue

        data = line.rstrip().split('\t')

        # Skip uncalled and genotyping without explicit base calls
        if not re.match(r'^[ACGT]{1,2}$', data[3]):
            continue
        vcf_data = {x: '.' for x in VCF_FIELDS}

        # Chromosome, position, dbSNP ID, reference. Skip if we don't have ref.
        try:
            vcf_data['REF'] = reference[data[1]][data[2]]
        except KeyError:
            continue
        if data[1] == 'MT':
            vcf_data['CHROM'] = 'M'
        else:
            vcf_data['CHROM'] = data[1]
        vcf_data['POS'] = data[2]
        if data[0].startswith('rs'):
            vcf_data['ID'] = data[0]

        # Figure out the alternate alleles.
        alt_alleles = []
        for alle in data[3]:
            if not alle == vcf_data['REF'] and alle not in alt_alleles:
                alt_alleles.append(alle)
        if alt_alleles:
            vcf_data['ALT'] = ','.join(alt_alleles)
        else:
            vcf_data['ALT'] = '.'
            vcf_data['INFO'] = 'END=' + vcf_data['POS']

        # Get allele-indexed genotype.
        vcf_data['FORMAT'] = 'GT'
        all_alleles = [vcf_data['REF']] + alt_alleles
        genotype_indexed = '/'.join([str(all_alleles.index(x))
                                     for x in data[3]])
        vcf_data['23ANDME_DATA'] = genotype_indexed
        output_line = '\t'.join([vcf_data[x] for x in VCF_FIELDS])
        output.write(output_line + '\n')

    return output


def clean_raw_23andme(input_filepath, sentry=None, username=None):
    """
    Create clean file in 23andme format from downloaded version

    Obsessively careful processing that ensures 23andMe file format changes
    won't inadvertantly result in unexpected information, e.g. names.
    """
    error_message = ("Input file is expected to be either '.txt', '.txt.gz', "
                     "'.txt.bz2', or a single '.txt' file in a '.zip' ZIP "
                     'archive.')
    if input_filepath.endswith('.zip'):
        zip23andme = zipfile.ZipFile(input_filepath)
        zipfilelist = zip23andme.namelist()
        if len(zipfilelist) != 1:
            raise ValueError(error_message)
        inputfile = zip23andme.open(zipfilelist[0])
    elif input_filepath.endswith('.txt.gz'):
        inputfile = gzip.open(input_filepath)
    elif input_filepath.endswith('.txt.bz2'):
        inputfile = bz2.BZ2File(input_filepath)
    elif input_filepath.endswith('.txt'):
        inputfile = open(input_filepath)
    else:
        raise ValueError(error_message)

    output = StringIO()

    dateline = inputfile.next()
    re_datetime_string = (r'([A-Z][a-z]{2} [A-Z][a-z]{2} [ 1-9][0-9] ' +
                          r'[0-9][0-9]:[0-9][0-9]:[0-9][0-9] 2[0-9]{3})')
    if re.search(re_datetime_string, dateline):
        datetime_string = re.search(re_datetime_string, dateline).groups()[0]
        re_norm_day = r'(?<=[a-z])  ([1-9])(?= [0-9][0-9]:[0-9][0-9])'
        datetime_norm = re.sub(re_norm_day, r' 0\1', datetime_string)
        datetime_23andme = datetime.strptime(datetime_norm, '%a %b %d %H:%M:%S %Y')
        output.write("# This data file generated by 23andMe at: {}\r\n".format(
                     datetime_23andme.strftime('%a %b %d %H:%M:%S %Y')))

    expected_header_p1 = ["#\r\n",]
    expected_header_p2 = [
        "# This file contains raw genotype data, including data that is not used in 23andMe reports.\r\n",
        "# This data has undergone a general quality review however only a subset of markers have been \r\n",
        "# individually validated for accuracy. As such, this data is suitable only for research, \r\n",
        "# educational, and informational use and not for medical or other use.\r\n",
        "# \r\n",]
    expected_header_p3 = [
        "# Below is a text version of your data.  Fields are TAB-separated\r\n",
        "# Each line corresponds to a single SNP.  For each SNP, we provide its identifier \r\n",
        "# (an rsid or an internal id), its location on the reference human genome, and the \r\n",
        "# genotype call oriented with respect to the plus strand on the human reference sequence.\r\n",
        "# We are using reference human assembly build 37 (also known as Annotation Release 104).\r\n",
        "# Note that it is possible that data downloaded at different times may be different due to ongoing \r\n",
        "# improvements in our ability to call genotypes. More information about these changes can be found at:\r\n",
        "# https://www.23andme.com/you/download/revisions/\r\n",
        "# \r\n",
        ]
    expected_header_p4 = [
        "# More information on reference human assembly build 37 (aka Annotation Release 104):\r\n",
        "# http://www.ncbi.nlm.nih.gov/mapview/map_search.cgi?taxid=9606\r\n",
        "#\r\n",
        "# rsid\tchromosome\tposition\tgenotype\r\n",
        ]
    expected_header_v1 = (expected_header_p1 + expected_header_p3 +
                          expected_header_p4)
    expected_header_v2 = (expected_header_p1 + expected_header_p2 +
                          expected_header_p3 + expected_header_p4)

    next_line = inputfile.next()
    header_lines = []
    while next_line.startswith('#'):
        header_lines.append(next_line)
        next_line = inputfile.next()
    if len(header_lines) == len(expected_header_v2):
        if all([expected_header_v2[i] == header_lines[i] for i in
                 range(len(expected_header_v2))]):
            for line in expected_header_v2:
                output.write(line)
    elif len(header_lines) == len(expected_header_v1):
        if all([expected_header_v1[i] == header_lines[i] for i in
                 range(len(expected_header_v1))]):
            for line in expected_header_v1:
                output.write(line)
    else:
        if sentry:
            sentry_msg = '23andMe header did not conform to expected format.'
            if username:
                sentry_msg = sentry_msg + " Username: {}".format(username)
            sentry.captureMessage(sentry_msg)

    bad_format = False

    while next_line:
        if re.match(r'(rs|i)[0-9]+\t[1-9XYM][0-9T]?\t[0-9]+\t[ACGT\-ID][ACGT\-ID]?', next_line):
            output.write(next_line)
        else:
            bad_format = True
            print "BAD FORMAT:\n{}".format(next_line)
        try:
            next_line = inputfile.next()
        except StopIteration:
            next_line = None

    if bad_format and sentry:
        sentry_msg = '23andMe body did not conform to expected format.'
        if username:
            sentry_msg = sentry_msg + " Username: {}".format(username)
        sentry.captureMessage(sentry_msg)

    return output


def create_23andme_datafiles(username,
                             input_file=None,
                             file_url=None,
                             task_id=None,
                             update_url=None,
                             sentry=None,
                             **kwargs):
    """Create Open Humans Dataset from uploaded 23andme full genotyping data

    Optional arguments:
        input_file: path to a local copy of the uploaded file
        file_url: path to an online copy of the input file
        filedir: Local filepath, folder in which to place the resulting file.
        s3_bucket_name: S3 bucket to write resulting file.
        s3_key_dir: S3 key "directory" to write resulting file. The full S3 key
                    name will add a filename to the end of s3_key_dir.

    For input: either 'input_file' or 'file_url' must be specified.
    (The first is a path to a local file, the second is a URL to a remote one.)

    For output: iither 'filedir' (and no S3 arguments), or both
    's3_bucket_name' and 's3_key_dir' (and no 'filedir') must be specified.
    """
    tempdir = tempfile.mkdtemp()
    temp_files = []
    data_files = []

    if file_url and not input_file:
        filename = get_remote_file(file_url, tempdir)
        input_file = os.path.join(tempdir, filename)
    elif input_file and not file_url:
        pass
    else:
        raise 'Run with either input_file, or file_url'

    filename_base = '23andMe-genotyping'

    raw_23andme = clean_raw_23andme(input_file, sentry, username)
    raw_23andme.seek(0)
    vcf_23andme = vcf_from_raw_23andme(raw_23andme)

    # Save raw 23andMe genotyping to temp file.
    raw_filename = filename_base + '.txt'
    with open(os.path.join(tempdir, raw_filename), 'w') as raw_file:
        raw_23andme.seek(0)
        shutil.copyfileobj(raw_23andme, raw_file)
        temp_files.append({
            'temp_filename': raw_filename,
            'tempdir': tempdir,
            'metadata': {
                'description': "23andMe full genotyping data, original format",
                'tags': ['23andMe', 'genotyping'],
            },
        })

    # Save VCF 23andMe genotyping to temp file.
    vcf_filename = filename_base + '.vcf'
    with open(os.path.join(tempdir, vcf_filename), 'w') as vcf_file:
        vcf_23andme.seek(0)
        shutil.copyfileobj(vcf_23andme, vcf_file)
        temp_files.append({
            'temp_filename': vcf_filename,
            'tempdir': tempdir,
            'metadata': {
                'description': "23andMe full genotyping data, VCF format",
                'tags': ['23andMe', 'genotyping', 'vcf'],
            },
        })

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
    if file_url:
        os.remove(input_file)
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
    if len(sys.argv) != 4:
        print 'Please specify a remote file URL, target local directory, and username.'
        sys.exit(1)

    create_23andme_datafiles(input_file=sys.argv[1], filedir=sys.argv[2], username=sys.argv[3])
