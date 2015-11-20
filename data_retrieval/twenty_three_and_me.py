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
import os
import re
import requests
import shutil
import sys
import tempfile
from urlparse import urlparse
import zipfile

from cStringIO import StringIO
from datetime import date, datetime

from boto.s3.connection import S3Connection

from .participant_data_set import format_filename, get_dataset, OHDataSource

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
            vcf_data['ALT'] = '<NON_REF>'
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


def clean_raw_23andme(input_filepath, sentry=None):
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

    expected_header = [
        "#\r\n",
        "# This file contains raw genotype data, including data that is not used in 23andMe reports.\r\n",
        "# This data has undergone a general quality review however only a subset of markers have been \r\n",
        "# individually validated for accuracy. As such, this data is suitable only for research, \r\n",
        "# educational, and informational use and not for medical or other use.\r\n",
        "# \r\n",
        "# Below is a text version of your data.  Fields are TAB-separated\r\n",
        "# Each line corresponds to a single SNP.  For each SNP, we provide its identifier \r\n",
        "# (an rsid or an internal id), its location on the reference human genome, and the \r\n",
        "# genotype call oriented with respect to the plus strand on the human reference sequence.\r\n",
        "# We are using reference human assembly build 37 (also known as Annotation Release 104).\r\n",
        "# Note that it is possible that data downloaded at different times may be different due to ongoing \r\n",
        "# improvements in our ability to call genotypes. More information about these changes can be found at:\r\n",
        "# https://www.23andme.com/you/download/revisions/\r\n",
        "# \r\n",
        "# More information on reference human assembly build 37 (aka Annotation Release 104):\r\n",
        "# http://www.ncbi.nlm.nih.gov/mapview/map_search.cgi?taxid=9606\r\n",
        "#\r\n",
        "# rsid\tchromosome\tposition\tgenotype\r\n"
        ]
    next_line = inputfile.next()
    header_lines = []
    while next_line.startswith('#'):
        header_lines.append(next_line)
        next_line = inputfile.next()
    if not (len(header_lines) == len(expected_header) and
            all([expected_header[i] == header_lines[i] for i in
                 range(len(expected_header))])):
        if sentry:
            sentry.captureMessage('23andMe header did not conform to expected format')
    for line in expected_header:
        output.write(line)

    bad_format = False

    while next_line:
        if re.match(r'(rs|i)[0-9]+\t[1-9XYM][0-9T]?\t[0-9]+\t[ACGT\-ID][ACGT\-ID]?', next_line):
            output.write(next_line)
        else:
            bad_format = True
        try:
            next_line = inputfile.next()
        except StopIteration:
            next_line = None

    if bad_format and sentry:
        sentry.captureMessage('23andMe body did not conform to expected format')

    return output


def create_23andme_ohdataset(input_file=None,
                             file_url=None,
                             task_id=None,
                             update_url=None,
                             sentry=None,
                             **kwargs):
    """Create Open Humans Dataset from uploaded 23andme full genotyping data

    Required arguments:
        access_token: 23andme access token
        profile_id: 23andme profile ID

    Optional arguments:
        input_file: path to a local copy of the uploaded file
        file_url: path to an online copy of the input file
        filedir: Local filepath, folder in which to place the resulting file.
        s3_bucket_name: S3 bucket to write resulting file.
        s3_key_dir: S3 key "directory" to write resulting file. The full S3 key
                    name will add a filename to the end of s3_key_dir.

    Either 'input_file', or both 'input_s3_bucket' and 'input_s3_key' (and no
    'input_file') must be specified.

    Either 'filedir' (and no S3 arguments), or both s3_bucket_name and
    s3_key_dir (and no 'filedir') must be specified.
    """
    filename = format_filename(source='twenty-three-and-me',
                               data_type='genotyping')

    if file_url and not input_file:
        # Create a local temp dir to work with this file, copy file to it.
        tempdir = tempfile.mkdtemp()
        r = requests.get(file_url, stream=True)
        req_url = r.url
        basename = os.path.basename(urlparse(req_url).path)
        input_file = os.path.join(tempdir, basename)
        with open(input_file, 'wb') as fd:
            for chunk in r.iter_content(chunk_size=1024):
                fd.write(chunk)
    elif input_file and not file_url:
        pass
    else:
        raise 'Run with either input_file, or file_url'

    raw_23andme = clean_raw_23andme(input_file, sentry)
    raw_23andme.seek(0)

    vcf_23andme = vcf_from_raw_23andme(raw_23andme)
    raw_23andme.seek(0)
    vcf_23andme.seek(0)

    # Set up output data set file.
    source = OHDataSource(name='23andMe User Download',
                          url='https://www.23andme.com/you/download/')
    dataset = get_dataset(filename, source, **kwargs)

    dataset.add_file(file=raw_23andme, name='23andme-full-genotyping.txt')
    dataset.add_file(file=vcf_23andme, name='23andme-full-genotyping.vcf')
    dataset.close()

    if file_url:
        os.remove(input_file)
        shutil.rmtree(tempdir)

    if update_url and task_id:
        dataset.update(update_url, task_id, subtype='genotyping')

    return dataset


if __name__ == '__main__':
    if len(sys.argv) != 3:
        print 'Please specify a remote file URL, and target local directory.'
        sys.exit(1)

    create_23andme_ohdataset(file_url=sys.argv[1], filedir=sys.argv[2])
