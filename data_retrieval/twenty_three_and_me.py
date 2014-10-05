"""
23andme genotyping data extraction.

Copyright (C) 2014 Madeleine Price Ball

This software is shared under the "MIT License" license (aka "Expat License"),
see LICENSE.TXT for full license text.

"""
from datetime import date
import json
import os
import re
import requests
import subprocess
from subprocess import call, check_output
from tempfile import NamedTemporaryFile
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

CHROM_INDEX = {"1": 1, "2": 2, "3": 3, "4": 4, "5": 5,
               "6": 6, "7": 7, "8": 8, "9": 9, "10": 10,
               "11": 11, "12": 12, "13": 13, "14": 14, "15": 15,
               "16": 16, "17": 17, "18": 18, "19": 19, "20": 20,
               "21": 21, "22": 22, "X": 23, "Y": 24, "M": 25, "MT": 25,
               }


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
    y_regions = json.load(open(API23ANDME_Y_REGIONS_JSON))
    y_seqs = ''.join([data[x[0]*2:x[0]*2+x[1]*2] for x in y_regions])
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


def api23andme_to_vcf_rows(genetic_data):
    """Convert 23andme locations to unsorted VCF lines"""
    snp_info_data = snp_data_23andme()
    for snp_info in snp_info_data:
        index = int(snp_info[0]) * 2
        genotype = genetic_data[index:index+2]
        if snp_info[4] == '_' or genotype == '__':
            continue
        if not re.match(r'^[ACGT]{2}$', genotype):
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


def api23andme_to_vcf(genetic_data):
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
    vcf_rows = api23andme_to_vcf_rows(genetic_data)
    for line in vcf_rows:
        yield line + '\n'


def api23andme_to_23andmeraw(genetic_data):
    snp_info_data = snp_data_23andme()
    for snp_info in snp_info_data:
        index = int(snp_info[0]) * 2
        genotype = genetic_data[index:index+2]
        data = [snp_info[1], snp_info[2], snp_info[3], genotype]
        yield '\t'.join(data) + '\n'


def create_23andme_OHDataSet(access_token, profile_id, file_id):
    data_23andme = api23andme_full_gen_data(access_token, profile_id)
    sex_inferred = api23andme_full_gen_infer_sex(data_23andme)
    data_vcf = api23andme_to_vcf(data_23andme, sex_inferred)
    output = open('temp.vcf', 'w')
    for line in data_vcf:
        output.write(line)
    data_23andmeraw = api23andme_to_23andmeraw(data_23andme, sex_inferred)
    output2 = open('temp.txt', 'w')
    for line in data_23andmeraw:
        output2.write(line)

if __name__ == "__main__":
    from secret_test_config import TOKEN_23ANDME, PROFILE_ID_23ANDME
    file_id = "Test_23andme_data.tar.gz"
    create_23andme_OHDataSet(access_token=TOKEN_23ANDME,
                             profile_id=PROFILE_ID_23ANDME,
                             file_id=file_id)
