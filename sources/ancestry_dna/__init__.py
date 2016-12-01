"""
AncestryDNA genotyping data extraction.

Copyright (C) 2016 PersonalGenomes.org

This software is shared under the "MIT License" license (aka "Expat License"),
see LICENSE.TXT for full license text.
"""

import bz2
from cStringIO import StringIO
from datetime import date, datetime
import logging
import os
import re
import shutil
import urlparse

import arrow
import bcrypt

from base_source import BaseSource
from data_retrieval.sort_vcf import sort_vcf

logger = logging.getLogger(__name__)

REF_ANCESTRYDNA_FILE = os.path.join(
    os.path.dirname(__file__), 'reference_b37.txt')

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
            if alle != vcf_data['REF'] and alle not in alt_alleles:
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


class AncestryDNASource(BaseSource):
    """
    Create clean file in AncestryDNA format from downloaded version

    Obsessively careful processing to minimize risk that AncestryDNA file
    format changes inadvertantly result in unexpected leaks, e.g. names.
    """
    source = 'ancestry_dna'

    def clean_raw_ancestrydna(self):
        """
        Create clean file in AncestryDNA format from downloaded version

        Obsessively careful processing that ensures AncestryDNA file format changes
        won't inadvertantly result in unexpected information, e.g. names.
        """
        inputfile = self.open_archive()

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

            datetime_ancestrydna = datetime.strptime(datetime_string,
                                                     '%m/%d/%Y %H:%M:%S')

            output.write(
                '#This file was generated by AncestryDNA at: {}\r\n'.format(
                    datetime_ancestrydna.strftime('%a %b %d %H:%M:%S %Y MDT')))

        re_array_version = (
            r'#Data was collected using AncestryDNA array version: V\d\.\d\r\n')

        header_array_version = inputfile.next()

        if re.match(re_array_version, header_array_version):
            output.write(header_array_version)

        re_converter_version = (
            r'#Data is formatted using AncestryDNA converter version: V\d\.\d\r\n')

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
            self.sentry_log("AncestryDNA header didn't match expected format.")

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

        LINE_RE = re.compile(
            r'(rs|VGXS)[0-9]+\t[1-9][0-9]?\t[0-9]+\t[ACGT0]\t[ACGT0]')
        REPORTED_Y = re.compile(r'(rs|VGXS)[0-9]+\t24\t[0-9]+\t[ACGT0]\t[ACGT0]')
        CALLED_Y = re.compile(r'(rs|VGXS)[0-9]+\t24\t[0-9]+\t[ACGT]\t[ACGT]')

        while next_line:
            if LINE_RE.match(next_line):
                if REPORTED_Y.match(next_line):
                    reported_Y += 1

                    if CALLED_Y.match(next_line):
                        called_Y += 1

                output.write(next_line)
            else:
                # Only report this type of format issue once.
                if not bad_format:
                    bad_format = True
                    self.sentry_log('AncestryDNA body did not conform to expected format.')
                    logger.warn('Bad format: "%s"', next_line)

            try:
                next_line = inputfile.next()
            except StopIteration:
                next_line = None

        if called_Y * 1.0 / reported_Y > 0.5:
            genome_sex = 'Male'

        return output, genome_sex

    def should_update(self, files):
        """
        Reprocess only if source file has changed.

        We store a hash of the original filepath as metadata and check this.
        Update is deemed unnecessary if (a) processed files exist, (b) they
        have recorded orig_file_hash, (c) we verify these all match a hash of
        the source file path for this task (from self.file_url).
        """
        if not files:
            logger.info(
                'Update needed for user "{}", source "{}": no current '
                'files available.'.format(self.oh_username, self.source))
            return True
        for file_data in files:
            try:
                orig_file_hash = file_data['metadata']['orig_file_hash']
            except KeyError:
                logger.info(
                    'Update needed for user "{}", source "{}": no hash stored '
                    'for original file.'.format(self.oh_username, self.source))
                return True
            if not self.same_orig_file(orig_file_hash):
                logger.info(
                    'Update needed for user "{}", source "{}": hash mismatch '
                    'for original file.'.format(self.oh_username, self.source))
                return True
        logger.info('Update unnecessary for user "{}", source "{}".'.format(
            self.oh_username, self.source))
        return False

    def same_orig_file(self, orig_file_hash):
        """
        Check hashed self.file_url path against stored orig_file_hash.

        The path in an original source file URL are expected to be unique, as
        we store them with a UUID.
        """
        if not self.file_url:
            return False
        url_path = str(urlparse.urlparse(self.file_url).path)
        new_hash = bcrypt.hashpw(url_path, str(orig_file_hash))
        return orig_file_hash == new_hash

    def create_files(self, input_file=None, file_url=None):
        """
        Create Open Humans Dataset from uploaded AncestryDNA genotyping data

        Optional arguments:
            input_file: path to a local copy of the uploaded file
            file_url: path to an online copy of the input file
        """
        if not self.input_file:
            raise Exception('Run with either input_file or file_url')

        new_hash = ''
        if self.file_url:
            orig_path = urlparse.urlparse(self.file_url).path
            new_hash = bcrypt.hashpw(str(orig_path), bcrypt.gensalt())

        filename_base = 'AncestryDNA-genotyping'

        raw_ancestrydna, genome_sex = self.clean_raw_ancestrydna()
        raw_ancestrydna.seek(0)
        vcf_ancestrydna_unsorted = vcf_from_raw_ancestrydna(
            raw_ancestrydna, genome_sex)

        # Save raw AncestryDNA genotyping to temp file.
        raw_filename = filename_base + '.txt'

        with open(self.temp_join(raw_filename), 'w') as raw_file:
            raw_ancestrydna.seek(0)

            shutil.copyfileobj(raw_ancestrydna, raw_file)

            self.temp_files.append({
                'temp_filename': raw_filename,
                'metadata': {
                    'description':
                        'AncestryDNA full genotyping data, original format',
                    'tags': ['AncestryDNA', 'genotyping'],
                    'orig_file_hash': new_hash,
                    'creation_date': arrow.get().format(),
                },
            })

        # Save VCF AncestryDNA genotyping to temp file.
        vcf_ancestrydna_unsorted.seek(0)
        vcf_ancestrydna_sorted = sort_vcf(vcf_ancestrydna_unsorted)
        vcf_filename = filename_base + '.vcf.bz2'

        with bz2.BZ2File(self.temp_join(vcf_filename), 'w') as vcf_file:
            vcf_ancestrydna_sorted.seek(0)

            shutil.copyfileobj(vcf_ancestrydna_sorted, vcf_file)

            self.temp_files.append({
                'temp_filename': vcf_filename,
                'metadata': {
                    'description': 'AncestryDNA full genotyping data, VCF format',
                    'tags': ['AncestryDNA', 'genotyping', 'vcf'],
                    'orig_file_hash': new_hash,
                    'creation_date': arrow.get().format(),
                },
            })
