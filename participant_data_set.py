"""
Create and manage participant-specific research dataset TarFile.
"""
import bz2
import json
import gzip
import re
import os
import requests
import shutil
import tarfile
import tempfile

METADATA_SUFFIX = '.metadata.json'


class MyDataSet(object):
    """Create and manage a participant data set tarfile."""

    def __init__(self, *args, **kwargs):
        """Open or create a MyDataSet tarfile."""
        self.filename = kwargs['filename']
        self.mode = kwargs['mode']
        assert self.mode in ['r', 'r+', 'a', 'w']
        self.basename, self.filetype = self._parse_filename(self.filename)
        self.md_name = self.basename + METADATA_SUFFIX
        self.tempdir = None
        self.metadata = None

        if self.mode == 'r':
            try:
                self.tarfile = tarfile.open(self.filename)
            except:
                raise ValueError("Not available for reading: " + self.filename)
        elif self.mode == 'w':
            self._create_new_dataset(*args, **kwargs)
        else:
            self._copy_into_tempdir()

    def _parse_filename(self, filename):
        """Parse filename for basename and filetype."""
        filename_split = re.split(r'\.tar', filename)
        basename = filename_split[0]
        filetype = filename_split[1].lstrip('.')
        if filetype not in ['', 'bz2', 'gz']:
            raise ValueError("Not an expected filename: should end with " +
                             "'.tar', 'tar.gz', or 'tar.bz2'.")
        return basename, filetype

    def _copy_into_tempdir(self):
        """Copy existing dataset into tempdir for editing"""
        extraction_tempdir = tempfile.mkdtemp()
        print "Created temporary directory: " + extraction_tempdir
        tfile = tarfile.open(self.filename, 'r')
        tfile.extractall(extraction_tempdir)
        tfile.close()
        self.tempdir = tempfile.mkdtemp()
        print "Created temporary directory: " + self.tempdir
        files_dir = os.path.join(extraction_tempdir, self.basename)
        print "Copying " + files_dir + " to " + self.tempdir
        for item in os.listdir(files_dir):
            shutil.move(os.path.join(files_dir, item), self.tempdir)
        print "Removing temporary directory: " + extraction_tempdir
        shutil.rmtree(extraction_tempdir)

    def _create_new_dataset(self, **kwargs):
        """Initialize the dataset with an optional metadata file"""
        self.tempdir = tempfile.mkdtemp()
        print "Created temporary directory: " + self.tempdir

        # Initialize metadata
        if 'metadata' in kwargs:
            self.metadata = kwargs['metadata']
        else:
            self.metadata = {}
        metadata_file = open(os.path.join(self.tempdir, self.md_name), 'w')
        metadata_file.write(json.dumps(self.metadata, indent=2) + '\n')
        metadata_file.close()

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
        self.tarfile.add(os.path.join(self.tempdir, self.md_name),
                         arcname=os.path.join(self.basename, self.md_name))
        os.remove(os.path.join(self.tempdir, self.md_name))
        # Now add the rest.
        for item in os.listdir(self.tempdir):
            self.tarfile.add(os.path.join(self.tempdir, item),
                             arcname=os.path.join(self.basename, item))
        self.tarfile.close()
        print "Removing temporary directory: " + self.tempdir
        shutil.rmtree(self.tempdir)
        self.tarfile = tarfile.open(self.filename, mode='r')
        self.mode = 'r'
