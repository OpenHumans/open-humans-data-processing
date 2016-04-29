"""
Use UNIX commands to sort a VCF file.

Uses tempfiles to store intermediate products. Useful if you don't trust the
file to be sorted. Doesn't check, just sorts.

Sorts by chromosome 1st: Chr1, Chr2... Chr9, Chr10... Chr22, ChrX, ChrY, ChrM
Then numerically by position column.
Works with these chromosome name variations: "1", "Chr1", and "chr1".
"""
import bz2
import gzip
import tempfile
import subprocess
import sys

CHROM_ORDER = {
    'chr1': '1',
    'chr2': '2',
    'chr3': '3',
    'chr4': '4',
    'chr5': '5',
    'chr6': '6',
    'chr7': '7',
    'chr8': '8',
    'chr9': '9',
    'chr10': '10',
    'chr11': '11',
    'chr12': '12',
    'chr13': '13',
    'chr14': '14',
    'chr15': '15',
    'chr16': '16',
    'chr17': '17',
    'chr18': '18',
    'chr19': '19',
    'chr20': '20',
    'chr21': '21',
    'chr22': '22',
    'chrX': '23',
    'chrY': '24',
    'chrM': '25',
    'chrMT': '25',
    'Chr1': '1',
    'Chr2': '2',
    'Chr3': '3',
    'Chr4': '4',
    'Chr5': '5',
    'Chr6': '6',
    'Chr7': '7',
    'Chr8': '8',
    'Chr9': '9',
    'Chr10': '10',
    'Chr11': '11',
    'Chr12': '12',
    'Chr13': '13',
    'Chr14': '14',
    'Chr15': '15',
    'Chr16': '16',
    'Chr17': '17',
    'Chr18': '18',
    'Chr19': '19',
    'Chr20': '20',
    'Chr21': '21',
    'Chr22': '22',
    'ChrX': '23',
    'ChrY': '24',
    'ChrM': '25',
    'ChrMT': '25',
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
    'X': '23',
    'Y': '24',
    'M': '25',
    'MT': '25',
}


def sort_vcf(input_file):
    outputfile = tempfile.TemporaryFile()
    sortingfile = tempfile.TemporaryFile()
    next_line = input_file.next()
    while next_line and next_line.startswith('#'):
        outputfile.write(next_line)
        try:
            next_line = input_file.next()
        except StopIteration:
            next_line = None
            break
    while next_line:
        for key in CHROM_ORDER:
            if next_line.startswith(key + '\t'):
                sortingfile.write(CHROM_ORDER[key] + '\t' + next_line)
                break
        try:
            next_line = input_file.next()
        except StopIteration:
            next_line = None
            break
    sortingfile.seek(0)
    sort_proc = subprocess.Popen(['sort', '-k', '1n,1', '-k', '3n,3'],
                                 stdin=sortingfile,
                                 stdout=subprocess.PIPE)
    cut_proc = subprocess.Popen(['cut', '-f', '2-'],
                                stdin=sort_proc.stdout,
                                stdout=subprocess.PIPE)
    for line in cut_proc.stdout:
        outputfile.write(line)
    outputfile.seek(0)
    return outputfile


def sort_vcf_file(input_filepath):
    error_message = ("Input file is expected to be either '.vcf', '.vcf.gz', "
                     "'.vcf.bz2'.")
    if input_filepath.endswith('.vcf.gz'):
        input_file = gzip.open(input_filepath)
    elif input_filepath.endswith('.vcf.bz2'):
        input_file = bz2.BZ2File(input_filepath)
    elif input_filepath.endswith('.vcf'):
        input_file = open(input_filepath)
    else:
        raise ValueError(error_message)
    return sort_vcf(input_file)


if __name__ == '__main__':
    output = sort_vcf_file(sys.argv[1])
    for line in output:
        print line.rstrip()
