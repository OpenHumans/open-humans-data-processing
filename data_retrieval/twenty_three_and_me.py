"""
23andme genotyping data extraction.

Copyright (C) 2014 Madeleine Price Ball

This software is shared under the "MIT License" license (aka "Expat License"),
see LICENSE.TXT for full license text.

"""
from datetime import date
import json
import os
import requests
import subprocess
from subprocess import call, check_output
from tempfile import NamedTemporaryFile
import time

from .participant_data_set import OHDataSource, OHDataSet

SNP_DATA_23ANDME_FILE = os.path.join(os.path.dirname(__file__),
                                     '23andme_API_snps_data_with_ref.txt')

CHROM_SORT_ORDER_FILE = os.path.join(os.path.dirname(__file__),
                                     'chrom_sort_order.txt')

VCF_FIELDS = ['CHROM', 'POS', 'ID', 'REF', 'ALT', 'QUAL', 'FILTER',
              'INFO', 'FORMAT', '23ANDME_DATA']

CHROM_INDEX = {"1": 1, "2": 2, "3": 3, "4": 4, "5": 5,
               "6": 6, "7": 7, "8": 8, "9": 9, "10": 10,
               "11": 11, "12": 12, "13": 13, "14": 14, "15": 15,
               "16": 16, "17": 17, "18": 18, "19": 19, "20": 20,
               "21": 21, "22": 22, "X": 23, "Y": 24, "M": 25, "MT": 25,
               }


def snp_data_23andme():
    """Generator, returns SNP data for each position"""
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
    """Get full genotype data from 23andme API"""
    headers = {'Authorization': 'Bearer %s' % access_token}
    genome_data_url = "http://api.23andme.com/1/genomes/%s" % profile_id
    genome_data_response = requests.get(genome_data_url, headers=headers)
    genome_data = genome_data_response.json()['genome']
    return genome_data


def vcf_header():
    header = []
    header.append("##fileformat=VCFv4.2")
    header.append("##fileDate=%s%s%s" %
                  (str(date.today().year),
                   str(date.today().month).zfill(2),
                   str(date.today().day).zfill(2)))
    commit = check_output(["git", "rev-parse", "HEAD"]).rstrip('\n')
    header.append("##source=open_humans_data_extraction." +
                  "twenty_three_and_me, commit:%s" % commit)
    ref_url = ("http://hgdownload-test.cse.ucsc.edu/" +
               "goldenPath/hg19/bigZips/hg19.2bit")
    header.append("##reference=%s" % ref_url)
    header.append('##FORMAT=<ID=GT,Number=1,Type=String,' +
                  'Description="Genotype">')
    header.append('#' + '\t'.join(VCF_FIELDS))
    return header


def api23andme_to_vcf_rows(genetic_data):
    """Convert 23andme locations to unsorted VCF lines"""
    snp_info_data = snp_data_23andme()
    for i in range(0, len(genetic_data), 2):
        genotype = genetic_data[i:i+2]
        snp_info = snp_info_data.next()
        if snp_info[4] == '_' or genotype == '__':
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


def sort_vcf_rows(vcf_rows):
    os.environ['LANG']='en_EN'
    vcf_rows_unsorted_file = NamedTemporaryFile()
    sorted_chrom_sort_order_file = NamedTemporaryFile()
    vcf_rows_sorted_file = NamedTemporaryFile()
    for line in vcf_rows:
        vcf_rows_unsorted_file.write(line + '\n')

    # Preparation for later join.
    # LANG=en_EN specified to solve possible inconsistent sort
    # algorithms used by 'sort' and 'join'.
    sortcall = ['sort', '-k1', CHROM_SORT_ORDER_FILE]
    subprocess.Popen(sortcall, stdout=sorted_chrom_sort_order_file)

    # Initial VCF sort, preparation for later join.
    sort_vcf_init = ['sort', '-k1', '-k2',
                     vcf_rows_unsorted_file.name]
    call(sort_vcf_init, stdout=vcf_rows_sorted_file)
    # Join VCF rows with the sort order key.
    join_command = ['join', '-t', "'\t'", '-11', '-11',
                    sorted_chrom_sort_order_file.name, '-']
    # Sort according to the sort order key (2) and position (3)
    sort_vcf = ['sort', '-k2n', '-k3n']
    # Cut out the key and output.
    cut_key = ['cut', '-d', "'\t'", '-f', '1,3-11']

    full_command = sort_vcf_init + ['|'] + join_command + ['|'] + sort_vcf + ['|'] + cut_key + ['>', vcf_rows_sorted_file.name]
    full_command_string = ' '.join(full_command)
    print full_command_string
    call(full_command_string, shell=True)

    vcf_rows_sorted_file.seek(0)
    for line in vcf_rows_sorted_file:
        yield line


def api23andme_to_vcf(genetic_data):
    unsorted_vcf_rows = api23andme_to_vcf_rows(genetic_data)
    sorted_vcf_rows = sort_vcf_rows(unsorted_vcf_rows)
    for line in vcf_header():
        yield line + '\n'
    for line in sorted_vcf_rows:
        yield line


def api23andme_to_23andmeraw(genetic_data):
    snp_info_data = snp_data_23andme()
    for i in range(0, len(genetic_data), 2):
        genotype = genetic_data[i:i+2]
        snp_info = snp_info_data.next()
        data = [snp_info[1], snp_info[2], snp_info[3], genotype]
        yield '\t'.join(data) + '\n'


def create_23andme_OHDataSet(access_token, profile_id, file_id):
    data_23andme = api23andme_full_gen_data(access_token, profile_id)
    data_vcf = api23andme_to_vcf(data_23andme)
    output = open('temp.vcf', 'w')
    for line in data_vcf:
        output.write(line)
    data_23andmeraw = api23andme_to_23andmeraw(data_23andme)
    output2 = open('temp.txt', 'w')
    for line in data_23andmeraw:
        output2.write(line)

if __name__ == "__main__":
    from secret_test_config import TOKEN_23ANDME, PROFILE_ID_23ANDME
    file_id = "Test_23andme_data.tar.gz"
    create_23andme_OHDataSet(access_token=TOKEN_23ANDME,
                             profile_id=PROFILE_ID_23ANDME,
                             file_id=file_id)
