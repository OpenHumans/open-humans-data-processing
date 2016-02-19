r"""
AncestryDNA genotyping data extraction.

Copyright (C) 2016 PersonalGenomes.org

This software is shared under the "MIT License" license (aka "Expat License"),
see LICENSE.TXT for full license text.

May be used on the command line. For example, the following command:

    python -m data_retrieval.ancestry_dna ~/Downloads/AncestryDNA.zip files

Will assemble processed data sets in files/
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

from .files import get_remote_file, mv_tempfile_to_output
from .sort_vcf import sort_vcf

REF_ANCESTRYDNA_FILE = os.path.join(
    os.path.dirname(__file__), 'ancestry-dna', 'reference_b37.txt')

# Was used to generate reference genotypes in the previous file.
REFERENCE_GENOME_URL = ('http://hgdownload-test.cse.ucsc.edu/' +
                        'goldenPath/hg19/bigZips/hg19.2bit')

VCF_FIELDS = ['CHROM', 'POS', 'ID', 'REF', 'ALT', 'QUAL', 'FILTER',
              'INFO', 'FORMAT', 'ANCESTRYDNA_DATA']

# The only non-commented-out header line. We want to ignore it.
EXPECTED_COLUMNS_HEADER = 'rsid\tchromosome\tposition\tallele1\tallele2\r\n'

CHROM_MAP = {
    '1': '1',
    '2': '2',
    '3': '3',
    '4': '4',
    '5': '5',
    '6': '6',
    '7': '7',
    '8': '8',
    '9': '9',
    '10': '10',
    '11': '11',
    '12': '12',
    '13': '13',
    '14': '14',
    '15': '15',
    '16': '16',
    '17': '17',
    '18': '18',
    '19': '19',
    '20': '20',
    '21': '21',
    '22': '22',
    '23': 'X',
    '24': 'Y',
    '25': 'X',
}

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


def vcf_from_raw_ancestrydna(raw_ancestrydna, genome_sex):
    output = StringIO()
    reference = dict()
    with open(REF_ANCESTRYDNA_FILE) as f:
        for line in f:
            data = line.rstrip().split('\t')
            if data[0] not in reference:
                reference[data[0]] = dict()
            reference[data[0]][data[1]] = data[2]
    header = vcf_header(
        source='open_humans_data_processing.ancestry_dna',
        reference=REFERENCE_GENOME_URL,
        format_info=['<ID=GT,Number=1,Type=String,Description="Genotype">']
    )
    for line in header:
        output.write(line + '\n')
    for line in raw_ancestrydna:
        # Skip header
        if line.startswith('#'):
            continue
        if line == EXPECTED_COLUMNS_HEADER:
            continue

        data = line.rstrip().split('\t')

        # Skip uncalled and genotyping without explicit base calls
        if not re.match(r'^[ACGT]$', data[3]):
            continue
        if not re.match(r'^[ACGT]$', data[4]):
            continue
        vcf_data = {x: '.' for x in VCF_FIELDS}

        # Chromosome. Determine correct reporting according to genome_sex.
        try:
            vcf_data['REF'] = reference[data[1]][data[2]]
        except KeyError:
            continue
        vcf_data['CHROM'] = CHROM_MAP[data[1]]
        if data[1] == '24' and genome_sex == 'Female':
            continue
        if data[1] in ['23', '24'] and genome_sex == 'Male':
            alleles = data[3]
        else:
            alleles = data[3] + data[4]

        # Position, dbSNP ID, reference. Skip if we don't have ref.
        vcf_data['POS'] = data[2]
        if data[0].startswith('rs'):
            vcf_data['ID'] = data[0]

        # Figure out the alternate alleles.
        alt_alleles = []
        for alle in alleles:
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
                                     for x in alleles])
        vcf_data['ANCESTRYDNA_DATA'] = genotype_indexed
        output_line = '\t'.join([vcf_data[x] for x in VCF_FIELDS])
        output.write(output_line + '\n')

    return output


def clean_raw_ancestrydna(input_filepath, sentry=None, username=None):
    """
    Create clean file in AncestryDNA format from downloaded version

    Obsessively careful processing that ensures AncestryDNA file format changes
    won't inadvertantly result in unexpected information, e.g. names.
    """
    error_message = ("Input file is expected to be either '.txt', '.txt.gz', "
                     "'.txt.bz2', or a single '.txt' file in a '.zip' ZIP "
                     'archive.')
    if input_filepath.endswith('.zip'):
        zipancestrydna = zipfile.ZipFile(input_filepath)
        zipfilelist = [f for f in zipancestrydna.namelist() if not
                       f.startswith('__MACOSX/')]
        if len(zipfilelist) != 1:
            raise ValueError(error_message)
        inputfile = zipancestrydna.open(zipfilelist[0])
    elif input_filepath.endswith('.txt.gz'):
        inputfile = gzip.open(input_filepath)
    elif input_filepath.endswith('.txt.bz2'):
        inputfile = bz2.BZ2File(input_filepath)
    elif input_filepath.endswith('.txt'):
        inputfile = open(input_filepath)
    else:
        raise ValueError(error_message)

    output = StringIO()

    header_l1 = inputfile.next()
    expected_header_l1 = '#AncestryDNA raw data download\r\n'
    if header_l1 == expected_header_l1:
        output.write(header_l1)
    dateline = inputfile.next()
    re_datetime_string = (r'([0-1][0-9]/[0-3][0-9]/20[1-9][0-9] ' +
                          r'[0-9][0-9]:[0-9][0-9]:[0-9][0-9]) MDT')
    if re.search(re_datetime_string, dateline):
        datetime_string = re.search(re_datetime_string, dateline).groups()[0]
        datetime_ancestrydna = datetime.strptime(datetime_string, '%m/%d/%Y %H:%M:%S')
        output.write("#This file was generated by AncestryDNA at: {}\r\n".format(
                      datetime_ancestrydna.strftime('%a %b %d %H:%M:%S %Y MDT')))

    re_array_version = r"#Data was collected using AncestryDNA array version: V\d\.\d\r\n"
    header_array_version = inputfile.next()
    if re.match(re_array_version, header_array_version):
        output.write(header_array_version)

    re_converter_version = r"#Data is formatted using AncestryDNA converter version: V\d\.\d\r\n"
    header_converter_version = inputfile.next()
    if re.match(re_converter_version, header_converter_version):
        output.write(header_converter_version)

    expected_header_p = [
        "#Below is a text version of your DNA file from Ancestry.com DNA, LLC.  THIS \r\n",
        "#INFORMATION IS FOR YOUR PERSONAL USE AND IS INTENDED FOR GENEALOGICAL RESEARCH \r\n",
        "#ONLY.  IT IS NOT INTENDED FOR MEDICAL OR HEALTH PURPOSES.  THE EXPORTED DATA IS \r\n",
        "#SUBJECT TO THE AncestryDNA TERMS AND CONDITIONS, BUT PLEASE BE AWARE THAT THE \r\n",
        "#DOWNLOADED DATA WILL NO LONGER BE PROTECTED BY OUR SECURITY MEASURES.\r\n",
        "#\r\n",
        "#Genetic data is provided below as five TAB delimited columns.  Each line \r\n",
        "#corresponds to a SNP.  Column one provides the SNP identifier (rsID where \r\n",
        "#possible).  Columns two and three contain the chromosome and basepair position \r\n",
        "#of the SNP using human reference build 37.1 coordinates.  Columns four and five \r\n",
        "#contain the two alleles observed at this SNP (genotype).  The genotype is reported \r\n",
        "#on the forward (+) strand with respect to the human reference.\r\n",
        ]

    next_line = inputfile.next()
    header_p_lines = []
    while next_line.startswith('#'):
        header_p_lines.append(next_line)
        next_line = inputfile.next()
    if len(header_p_lines) == len(expected_header_p):
        if all([expected_header_p[i] == header_p_lines[i] for i in
                range(len(expected_header_p))]):
            for line in expected_header_p:
                output.write(line)
    else:
        if sentry:
            sentry_msg = 'AncestryDNA header did not conform to expected format.'
            if username:
                sentry_msg = sentry_msg + " Username: {}".format(username)
            sentry.captureMessage(sentry_msg)

    data_header = next_line
    if data_header == EXPECTED_COLUMNS_HEADER:
        output.write(EXPECTED_COLUMNS_HEADER)

    next_line = inputfile.next()
    bad_format = False
    # AncestryDNA always reports two alleles for all X and Y positions.
    # For XY individuals, haplozygous positions are redundantly reported.
    # For XX individuals this means Y positions are "0".
    # Note the above two statements are not ALWAYS true! The raw data
    # ocassionally reports 'heterozygous' calls for X and Y in XY individuals,
    # and Y calls in XX individuals. So our test is forgiving of these.
    genome_sex = 'Female'
    called_Y = 0
    reported_Y = 0
    while next_line:
        if re.match(r'(rs|VGXS)[0-9]+\t[1-9][0-9]?\t[0-9]+\t[ACGT0]\t[ACGT0]', next_line):
            if re.match(r'(rs|VGXS)[0-9]+\t24\t[0-9]+\t[ACGT0]\t[ACGT0]', next_line):
                reported_Y += 1
                if re.match(r'(rs|VGXS)[0-9]+\t24\t[0-9]+\t[ACGT]\t[ACGT]', next_line):
                    called_Y += 1
            output.write(next_line)
        else:
            bad_format = True
            print "BAD FORMAT:\n{}".format(next_line)
        try:
            next_line = inputfile.next()
        except StopIteration:
            next_line = None

    if bad_format and sentry:
        sentry_msg = 'AncestryDNA body did not conform to expected format.'
        if username:
            sentry_msg = sentry_msg + " Username: {}".format(username)
        sentry.captureMessage(sentry_msg)

    if called_Y * 1.0 / reported_Y > 0.5:
        genome_sex = 'Male'
    return output, genome_sex


def create_ancestrydna_datafiles(username,
                                 input_file=None,
                                 file_url=None,
                                 task_id=None,
                                 update_url=None,
                                 sentry=None,
                                 **kwargs):
    """Create Open Humans Dataset from uploaded AncestryDNA genotyping data

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
        raise Exception('Run with either input_file, or file_url')

    filename_base = 'AncestryDNA-genotyping'

    raw_ancestrydna, genome_sex = clean_raw_ancestrydna(
        input_file, sentry, username)
    raw_ancestrydna.seek(0)
    vcf_ancestrydna_unsorted = vcf_from_raw_ancestrydna(
        raw_ancestrydna, genome_sex)

    # Save raw AncestryDNA genotyping to temp file.
    raw_filename = filename_base + '.txt'
    with open(os.path.join(tempdir, raw_filename), 'w') as raw_file:
        raw_ancestrydna.seek(0)
        shutil.copyfileobj(raw_ancestrydna, raw_file)
        temp_files.append({
            'temp_filename': raw_filename,
            'tempdir': tempdir,
            'metadata': {
                'description': "AncestryDNA full genotyping data, original format",
                'tags': ['AncestryDNA', 'genotyping'],
            },
        })

    # Save VCF AncestryDNA genotyping to temp file.
    vcf_ancestrydna_unsorted.seek(0)
    vcf_ancestrydna_sorted = sort_vcf(vcf_ancestrydna_unsorted)
    vcf_filename = filename_base + '.vcf'
    with open(os.path.join(tempdir, vcf_filename), 'w') as vcf_file:
        vcf_ancestrydna_sorted.seek(0)
        shutil.copyfileobj(vcf_ancestrydna_sorted, vcf_file)
        temp_files.append({
            'temp_filename': vcf_filename,
            'tempdir': tempdir,
            'metadata': {
                'description': "AncestryDNA full genotyping data, VCF format",
                'tags': ['AncestryDNA', 'genotyping', 'vcf'],
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

    create_ancestrydna_datafiles(input_file=sys.argv[1], filedir=sys.argv[2], username=sys.argv[3])
