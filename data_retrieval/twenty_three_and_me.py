"""
23andme genotyping data extraction.

Copyright (C) 2014 Madeleine Price Ball

This software is shared under the "MIT License" license (aka "Expat License"),
see LICENSE.TXT for full license text.

"""
from datetime import date, datetime
import json
import os
import re
import requests
import subprocess
from subprocess import call, check_output
from tempfile import NamedTemporaryFile, TemporaryFile
import time

from .participant_data_set import OHDataSource, OHDataSet

SNP_DATA_23ANDME_FILE = os.path.join(
    os.path.dirname(__file__),
    '23andme_API_snps_data_with_ref_sorted.txt')

# Was used to generate reference genotypes in the previous file.
REFERENCE_GENOME_URL = ("http://hgdownload-test.cse.ucsc.edu/" +
                        "goldenPath/hg19/bigZips/hg19.2bit")

API23ANDME_Y_REGIONS_JSON = 'data_retrieval/23andme_y_chrom_regions.json'

VCF_FIELDS = ['CHROM', 'POS', 'ID', 'REF', 'ALT', 'QUAL', 'FILTER',
              'INFO', 'FORMAT', '23ANDME_DATA']


def snp_data_23andme():
    """Generator, returns SNP info sorted by chrom and position."""
    snp_data_file = open(SNP_DATA_23ANDME_FILE)
    next_line = snp_data_file.next()
    while next_line.startswith('#'):
        next_line = snp_data_file.next()
    expected_header = ['index', 'snp', 'chromosome',
                       'chromosome_position', 'reference_allele']
    assert next_line == '\t'.join(expected_header) + '\n'
    for line in snp_data_file:
        data = line.rstrip('\n').split('\t')
        yield data


def api23andme_full_gen_data(access_token, profile_id):
    """Get full genotype data from 23andme API."""
    headers = {'Authorization': 'Bearer %s' % access_token}
    genome_data_url = "http://api.23andme.com/1/genomes/%s" % profile_id
    genome_data_response = requests.get(genome_data_url, headers=headers)
    genome_data = genome_data_response.json()['genome']
    return genome_data


def api23andme_full_gen_infer_sex(genetic_data):
    """Check some known Y genotype calls to infer sex."""
    y_regions = json.load(open(API23ANDME_Y_REGIONS_JSON))
    y_seqs = ''.join([genetic_data[x[0]*2:x[0]*2+x[1]*2] for x in y_regions])
    if re.search(r'[ACGT]', y_seqs):
        return "Male"
    else:
        return "Female"


def vcf_header(source=None, reference=None, format_info=None):
    """Generate a VCF header."""
    header = []
    header.append("##fileformat=VCFv4.1")
    header.append("##fileDate=%s%s%s" %
                  (str(date.today().year),
                   str(date.today().month).zfill(2),
                   str(date.today().day).zfill(2)))
    if source:
        header.append("##source=" + source)
    if reference:
        header.append("##reference=%s" % reference)
    for item in format_info:
        header.append("##FORMAT=" + item)
    header.append('#' + '\t'.join(VCF_FIELDS))
    return header


def get_genotype(genetic_data, snp_info, sex):
    """Get genotype, collapsing hemizygous locations."""
    raw_genotype = genetic_data[int(snp_info[0])*2:int(snp_info[0])*2+2]
    if snp_info[2] in ['MT', 'M', 'Y', 'chrM', 'chrMT', 'chrY']:
        try:
            assert raw_genotype[0] == raw_genotype[1]
        except AssertionError:
            print raw_genotype
            print snp_info
            print sex
            raise SystemError
        return raw_genotype[0]
    if sex == 'Male' and snp_info[2] in ['X', 'chrX']:
        # PAR X coordinates for hg19 according to UCSC are:
        # chrX:60001-2699520 and chrX:154931044-155260560
        if (60001 <= int(snp_info[3]) <= 2699520 or
            154931044 <= int(snp_info[3]) <= 155260560):
            return raw_genotype
        else:
            try:
                assert raw_genotype[0] == raw_genotype[1]
            except AssertionError:
                print raw_genotype
                print snp_info
                print sex
                raise SystemError
            return raw_genotype[0]
    return raw_genotype


def api23andme_to_vcf_rows(genetic_data, sex):
    """Convert 23andme locations to unsorted VCF lines."""
    snp_info_data = snp_data_23andme()
    for snp_info in snp_info_data:
        genotype = get_genotype(genetic_data, snp_info, sex)
        if snp_info[4] == '_' or genotype == '__' or genotype == '--':
            continue
        if not re.match(r'^[ACGT]{1,2}$', genotype):
            continue
        vcf_data = {x:'.' for x in VCF_FIELDS}
        vcf_data['CHROM'] = snp_info[2]
        vcf_data['POS'] = snp_info[3]
        if snp_info[1].startswith('rs'):
            vcf_data['ID'] = snp_info[1]
        vcf_data['REF'] = snp_info[4]
        alt_alleles = []
        for alle in genotype:
            if not alle == vcf_data['REF'] and alle not in alt_alleles:
                alt_alleles.append(alle)
        if alt_alleles:
            vcf_data['ALT'] = ','.join(alt_alleles)
        vcf_data['FORMAT'] = 'GT'
        all_alleles = [vcf_data['REF']] + alt_alleles
        genotype_indexed = '/'.join([str(all_alleles.index(x)) for
                                     x in genotype])
        vcf_data['23ANDME_DATA'] = genotype_indexed
        yield '\t'.join([vcf_data[x] for x in VCF_FIELDS])


def api23andme_to_vcf(genetic_data, sex):
    commit = check_output(["git", "rev-parse", "HEAD"]).rstrip('\n')
    source = ("open_humans_data_extraction.twenty_three_and_me," +
              "commit:%s" % commit)
    reference = REFERENCE_GENOME_URL
    format_info = ['<ID=GT,Number=1,Type=String,Description="Genotype">']
    vcf_header_lines = vcf_header(source=source,
                                  reference=reference,
                                  format_info=format_info)
    for line in vcf_header_lines:
        yield line + '\n'
    vcf_rows = api23andme_to_vcf_rows(genetic_data, sex)
    for line in vcf_rows:
        yield line + '\n'


def api23andme_to_23andmeraw(genetic_data, sex):
    snp_info_data = snp_data_23andme()
    date_string = datetime.now().strftime("%a %b %d %H:%M:%S %Y")
    if date_string[8] == '0':
        date_string = date_string[0:8] + ' ' + date_string[9:]
    header = ("# This data file generated by Open Humans at: " + date_string)
    header += """
#
# Below is a text version of your data, received by us using the 23andme API
# and reformatted to resemble the 23andme raw data file format.
#
# Fields are TAB-separated. Each line corresponds to a single SNP. For each SNP,
# we provide its identifier (an rsid or a 23andme internal id) and its location
# on the reference human genome (if available), as provided by the 23andeme API
# key. The genotype call is oriented with respect to the plus strand on the
# human reference sequence.
#
# 23andme's key is using the reference human assembly build 37 (also known as
# Annotation Release 104). Note that it is possible that data downloaded at
# different times may be different due to ongoing improvements in 23andme's
# ability to call genotypes. More information about these changes can be found
# at: https://www.23andme.com/you/download/revisions/
#
# More information on reference human assembly build 37 (aka Annotation Release 104):
# http://www.ncbi.nlm.nih.gov/mapview/map_search.cgi?taxid=9606
#
# rsid\tchromosome\tposition\tgenotype
"""
    yield header
    for snp_info in snp_info_data:
        index = int(snp_info[0]) * 2
        genotype = get_genotype(genetic_data, snp_info, sex)
        if not re.match(r'[ACGT-]', genotype):
            continue
        data = [snp_info[1], snp_info[2], snp_info[3], genotype]
        yield '\t'.join(data) + '\n'


def create_23andme_OHDataSet(access_token, profile_id, file_id):
    source = OHDataSource(name='23andme API',
                          url='http://api.23andme.com/')
    dataset_filename = '23andme-' + file_id + '-dataset.tar.gz'
    dataset = OHDataSet(filename=dataset_filename, mode='w', source=source)
    print "Fetching 23andme full genotyping data."
    data_23andme = api23andme_full_gen_data(access_token, profile_id)
    sex_inferred = api23andme_full_gen_infer_sex(data_23andme)
    print "Generating and adding VCF file."
    with TemporaryFile() as vcffile:
        data_vcf = api23andme_to_vcf(data_23andme, sex_inferred)
        for line in data_vcf:
            vcffile.write(line)
        vcffile.seek(0)
        dataset.add_file(file=vcffile,
                         name='23andme-full-genotyping-%s.vcf' % file_id)
    print "Generating and adding 23andme 'raw data' file."
    with TemporaryFile() as rawfile:
        data_23andmeraw = api23andme_to_23andmeraw(data_23andme, sex_inferred)
        for line in data_23andmeraw:
            rawfile.write(line)
        rawfile.seek(0)
        dataset.add_file(file=rawfile,
                         name='23andme-full-genotyping-%s.txt' % file_id)
    dataset.close()


if __name__ == "__main__":
    from secret_test_config import TOKEN_23ANDME, PROFILE_ID_23ANDME
    file_id = "abc123"
    create_23andme_OHDataSet(access_token=TOKEN_23ANDME,
                             profile_id=PROFILE_ID_23ANDME,
                             file_id=file_id)
