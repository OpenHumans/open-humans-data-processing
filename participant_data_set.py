"""
Create and manage participant-specific research dataset TarFile.
"""
import bz2
from datetime import datetime
import json
import gzip
import re
import os
import requests
import shutil
import tarfile
import tempfile

METADATA_SUFFIX = '.metadata.json'


class OHDataSet(object):
    """Create and manage a participant dataset, managed as a tarfile.

    _Notes_

    An OHDataSet is just a tarfile, with some constraints:
      - All data is contained within a single directory in the tar, with the
        same name as the basename for the file. (i.e. typical tar etiquette)
      - The first file in the tarfile is a JSON-format file containing metadata
        about the OHDataSet. It is named '[basename].metadata.json'.

    For any modes that perform writing or editing, an uncompressed version of
    the data is created in a temporary directory. The .close() method MUST be
    called to create the new OHDataSet tarfile and clean the tempdir.

    _Attributes_

    basename   The base filename for the dataset, derived from the filename
               provided on init. 'sample-q9f.tar.gz' has basename 'sample-q9f'.

    filetype   Compression type ('', 'gz', or 'bz2'), from filename suffix.
               e.g. 'sample-q9fe.tar.bz2' has compression type 'bz2'

    mode       File read/edit mode ('r', 'r+', 'a', or 'w'), specified on init.
               'r': read-only. 'w': write - will overwrite if filename exists.
               'r+' or 'a': amend existing file, create one if none exists.

    metadata   Information about data in the OHDataSet.

    tempdir    Location of temporary directory used for write/edit in 'r+',
               'a', and 'w' modes.
    """

    def __init__(self, *args, **kwargs):
        """Open or create an OHDataset.

        _Required arguments_
        mode      'r', 'r+', 'a', or 'w'
        filename   Must end in .tar, .tar.gz, or .tar.bz2.
        """
        self.filename = kwargs['filename']
        (self.basename,
         self.filetype) = self._parse_tar_filename(kwargs['filename'])
        self.mode = kwargs['mode']
        assert self.mode in ['r', 'r+', 'a', 'w'], "Mode not valid"
        self.tempdir = None
        self.metadata = {}

        if self.mode == 'r':
            try:
                self.tarfile = tarfile.open(self.filename)
                self.metadata = self._extract_metadata
            except:
                raise ValueError("Not available for reading: " + self.filename)
        else:
            self.tempdir = os.path.join(tempfile.mkdtemp(), self.basename)
            os.mkdir(self.tempdir)
            if self.mode == 'a' or self.mode == 'r+':
                old = tarfile.open(self.filename, 'r')
                self._copy_into_tempdir(old)
                old.close()

    @staticmethod
    def _parse_tar_filename(filename_str):
        """Parse filename for basename and filetype."""
        filename = os.path.basename(filename_str)
        filename_split = re.split(r'\.tar', filename)
        basename = filename_split[0]
        filetype = filename_split[1].lstrip('.')
        if filetype not in ['', 'bz2', 'gz']:
            raise ValueError("Not an expected filename: should end with " +
                             "'.tar', 'tar.gz', or 'tar.bz2'.")
        return basename, filetype

    @classmethod
    def extract_metadata(cls, target_tarfile):
        """Get metadata content from an OHDataSet tarfile."""
        if not target_tarfile or not target_tarfile.name:
            raise ValueError("Expects TarFile with associated filename.")
        basename, _ = cls._parse_tar_filename(target_tarfile.name)
        metadata_fp = os.path.join(basename, basename + METADATA_SUFFIX)
        metadata_content = target_tarfile.extractfile(metadata_fp).readlines()
        return json.loads(metadata_content)

    def _update_metadata_file(self):
        """Update metadata file with current object metadata."""
        assert self.mode in ['r+', 'a', 'w']
        metadata_fp = os.path.join(self.tempdir, self.basename,
                                   self.basename + METADATA_SUFFIX)
        metadata_fh = open(metadata_fp, 'w')
        metadata_fh.write(json.dumps(self.metadata, indent=2) + '\n')
        metadata_fh.close()

    def _copy_into_tempdir(self, old):
        """Copy existing dataset into tempdir for editing"""
        extraction_tempdir = tempfile.mkdtemp()
        old.extractall(extraction_tempdir)
        old_filesdir = os.path.join(extraction_tempdir, self.basename)
        for item in os.listdir(old_filesdir):
            shutil.move(os.path.join(old_filesdir, item), self.tempdir)
        shutil.rmtree(extraction_tempdir)

    def add_remote_file(self, url):
        """Fetch remote file, add to tempdir. Uncompress if gz or bz2."""
        assert self.mode in ['r+', 'a', 'w']
        local_filename = url.split('/')[-1]
        req = requests.get(url)
        tempf = tempfile.NamedTemporaryFile()
        for chunk in req.iter_content(chunk_size=512 * 1024):
            if chunk:
                tempf.write(chunk)
        tempf.flush()
        if local_filename.endswith('.gz'):
            out = gzip.open(tempf.name, mode='r')
            basename = local_filename[0:-3]
        elif local_filename.endswith('.bz2'):
            out = bz2.BZ2File(tempf.name, mode='r')
            basename = local_filename[0:-4]
        else:
            tempf.seek(0)
            out = tempf
            basename = local_filename
        rettime = datetime.isoformat(datetime.utcnow().replace(microsecond=0))
        self.metadata[local_filename] = {'retrieved_from': url,
                                         'retrieval_time': rettime}
        local_filepath = os.path.join(self.tempdir, basename)
        file_out = open(local_filepath, 'wb')
        file_out.writelines(out)
        file_out.close()
        out.close()
        tempf.close()

    def close(self):
        """Close the dataset and create a compressed read-only tarfile"""
        assert self.mode in ['r+', 'a', 'w']
        self.tarfile = tarfile.open(self.filename, mode='w:' + self.filetype)
        # Add the metadata file first so it's at the beginning of the tarfile.
        md_filename = self.basename + METADATA_SUFFIX
        with open(os.path.join(self.tempdir, md_filename), 'w') as mdfile:
            mdfile.write(json.dumps(self.metadata,
                                    indent=2, sort_keys=True) + '\n')
        self.tarfile.add(os.path.join(self.tempdir, md_filename),
                         arcname=os.path.join(self.basename, md_filename))
        os.remove(os.path.join(self.tempdir, md_filename))
        # Now add the rest.
        for item in os.listdir(self.tempdir):
            self.tarfile.add(os.path.join(self.tempdir, item),
                             arcname=os.path.join(self.basename, item))
        self.tarfile.close()
        print "Removing temporary directory: " + self.tempdir
        shutil.rmtree(self.tempdir)
        self.tarfile = tarfile.open(self.filename, mode='r')
        self.mode = 'r'
