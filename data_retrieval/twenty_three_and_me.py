"""
23andme genotyping data extraction.

Copyright (C) 2014 Madeleine Price Ball

This software is shared under the "MIT License" license (aka "Expat License"),
see LICENSE.TXT for full license text.

"""
import bz2
import json
import os
import requests

from .participant_data_set import OHDataSource, OHDataSet

SNP_DATA_23ANDME_FILE = os.path.join(os.path.dirname(__file__),
                                     '23andme_API_snps_data.txt.bz2')

CHROM_INDEX = {"1": 1, "2": 2, "3": 3, "4": 4, "5": 5,
               "6": 6, "7": 7, "8": 8, "9": 9, "10": 10,
               "11": 11, "12": 12, "13": 13, "14": 14, "15": 15,
               "16": 16, "17": 17, "18": 18, "19": 19, "20": 20,
               "21": 21, "22": 22, "X": 23, "Y": 24, "M": 25, "MT": 25,
               }


def snp_data_23andme():
    """Generator function, returns array of SNP data for each position/row"""
    snp_data_file = bz2.BZ2File(SNP_DATA_23ANDME_FILE)
    next_line = snp_data_file.next()
    while next_line.startswith('#'):
        next_line = snp_data_file.next()
    assert next_line == 'index\tsnp\tchromosome\tchromosome_position\n', \
        '23andme SNP data: Expected header not found'
    for line in snp_data_file:
        data = line.rstrip('\n').split('\t')
        yield data

def create_23andme_OHDataSet(access_token, profile_id, file_id):
    headers = {'Authorization': 'Bearer %s' % access_token}
    genome_data_url = "http://api.23andme.com/1/genomes/%s" % profile_id
    genome_data_response = requests.get(genome_data_url, headers=headers)
    genome_data = genome_data_response.json()['genome']
    print len(genome_data)

if __name__ == "__main__":
    from secret_test_config import TOKEN_23ANDME, PROFILE_ID_23ANDME
    file_id = "Test_23andme_data.tar.gz"
    testout = snp_data_23andme()
    create_23andme_OHDataSet(access_token=TOKEN_23ANDME,
                             profile_id=PROFILE_ID_23ANDME,
                             file_id=file_id)
