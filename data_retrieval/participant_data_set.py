"""
Create and manage user-specific research dataset TarFile.
"""
import bz2
import json
import gzip
import re
import os
import shutil
import tarfile
import tempfile

from datetime import datetime

import boto
import requests

SOURCE_INFO_ITEMS = ['name', 'url', 'citation',
                     'contact_email', 'contact_email_name',
                     'contact_phone', 'contact_phone_name']

METADATA_SUFFIX = '.metadata.json'


def get_dataset(filename,
                source=None,
                filedir=None,
                s3_bucket_name=None,
                s3_key_dir=None,
                **kwargs):
    """
    Get an S3- or a file-based dataset depending on the arguments passed in.
    """
    filedir_used = bool(filedir and not (s3_bucket_name or s3_key_dir))
    s3_used = bool((s3_bucket_name and s3_key_dir) and not filedir)

    # This is an XOR assertion
    assert filedir_used != s3_used, 'Specify filedir or S3 info, not both.'

    if filedir_used:
        filepath = os.path.join(filedir, filename)

        return OHDataSet(mode='w', source=source, filepath=filepath)

    s3_key_name = os.path.join(s3_key_dir, filename)

    return S3OHDataSet(mode='w',
                       source=source,
                       s3_bucket_name=s3_bucket_name,
                       s3_key_name=s3_key_name)


class OHDataSource(object):
    """Create and manage information about an OHDataSet source.

    _Attributes_
    info  (dict) Information about the study.

    Possible items in self.info are:
      name                (required) Name of the study or activity that
                          generated this data set.
      url                 The general URL for the study or activity.
      citation            Requested citation.
      contact_email       Email contact for more information.
      contact_email_name
      contact_phone       Phone number contact for more information.
      contact_phone_name

    """
    def __init__(self, *args, **kwargs):
        assert 'name' in kwargs, 'OHDataSource requires a "name".'

        self.info = {
            item: kwargs[item] for item in SOURCE_INFO_ITEMS if item in kwargs
        }


class OHDataSet(object):
    """
    Create and manage a user dataset, managed as a tarfile.

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

    source     OHDataSource, information about the OHDataSet source.

    metadata   Information about data in the OHDataSet.

    tempdir    Location of temporary directory used for write/edit in 'r+',
               'a', and 'w' modes.
    """
    def __init__(self, *args, **kwargs):
        """Open or create an OHDataSet.

        _Required arguments_
        mode      'r', 'r+', 'a', or 'w'
        filepath   Must end in .tar, .tar.gz, or .tar.bz2.
        """
        self.filepath = kwargs['filepath']
        (self.basename,
         self.filetype) = self._parse_tar_filename(self.filepath)
        self.mode = kwargs['mode']
        assert self.mode in ['r', 'r+', 'a', 'w'], 'Mode not valid'
        self.tempdir = None
        self.metadata = {'files': {}}

        if self.mode == 'r':
            try:
                self.tarfile = tarfile.open(self.filepath)
                self.metadata = self.extract_metadata(self.tarfile)
                self.source = self.extract_source(self.tarfile)
            except:
                raise ValueError('Not available for reading: ' + self.filepath)
        else:
            self.source = kwargs['source']
            self.tempdir = os.path.join(tempfile.mkdtemp(), self.basename)

            os.mkdir(self.tempdir)

            if self.mode == 'a' or self.mode == 'r+':
                old = tarfile.open(self.filepath, 'r')
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
            raise ValueError('Not an expected filename: should end with ' +
                             "'.tar', 'tar.gz', or 'tar.bz2'.")
        return basename, filetype

    @classmethod
    def extract_metadata(cls, target_tarfile):
        """Get metadata content from an OHDataSet tarfile."""
        if not target_tarfile or not target_tarfile.name:
            raise ValueError('Expects TarFile with associated filename.')
        basename, _ = cls._parse_tar_filename(target_tarfile.name)
        metadata_fp = os.path.join(basename, basename + METADATA_SUFFIX)
        metadata_content = target_tarfile.extractfile(metadata_fp).readlines()
        return json.loads(metadata_content)

    @classmethod
    def extract_source(cls, target_tarfile):
        metadata = cls.extract_metadata(target_tarfile)
        return OHDataSource(**metadata['source'])

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

    def add_file(self, filepath=None, file=None, name=None, file_meta=None):
        """Add local file

        _Input_
        filepath  Path to local file. If not given, must provide file and name.
        file      (file or file-like object)
        name      filename to use in archive
        """
        assert filepath or (file and name), 'Filepath or file and name missing'
        if filepath:
            filehandle = open(filepath)
            basename = os.path.basename(filepath)
        else:
            filehandle = file
            basename = name
        filepath_out = os.path.join(self.tempdir, basename)
        file_out = open(filepath_out, 'w')
        maketime = datetime.isoformat(datetime.utcnow().replace(microsecond=0))
        self.metadata['files'][basename] = {'creation_time': maketime}
        if file_meta:
            self.metadata['files'][basename].update(file_meta)
        file_out.writelines(filehandle)
        file_out.close()
        filehandle.close()

    def add_remote_file(self, url, file_meta=None):
        """Fetch remote file, add to tempdir. Uncompress if gz or bz2.

        _Input_
        url      (str) URL of target file.
        """
        assert self.mode in ['r+', 'a', 'w']

        # Parse url for filename information.
        local_filename = url.split('/')[-1]
        basename = re.search(r'(?P<basename>.*?)(|\.gz|\.bz2)$',
                             local_filename).group('basename')
        local_filepath = os.path.join(self.tempdir, basename)

        # Get the file.
        req = requests.get(url, stream=True)
        tempf = tempfile.NamedTemporaryFile()

        for chunk in req.iter_content(chunk_size=512 * 1024):
            if chunk:
                tempf.write(chunk)

        tempf.flush()

        # Set up decompression if appropriate.
        if local_filename.endswith('.gz'):
            out = gzip.open(tempf.name, mode='r')
        elif local_filename.endswith('.bz2'):
            out = bz2.BZ2File(tempf.name, mode='r')
        else:
            tempf.seek(0)
            out = tempf

        # Update metadata.
        rettime = datetime.isoformat(datetime.utcnow().replace(microsecond=0))
        self.metadata['files'][local_filename] = {'retrieved_from': url,
                                                  'retrieval_time': rettime}
        if file_meta:
            self.metadata['files'][local_filename].update(file_meta)
        # Copy uncompressed to file, clean up.
        file_out = open(local_filepath, 'wb')
        file_out.writelines(out)
        file_out.close()
        out.close()
        tempf.close()

    def close(self):
        """Save and close the dataset."""
        assert self.mode in ['r+', 'a', 'w']
        self.tarfile = tarfile.open(self.filepath, mode='w:' + self.filetype)
        # Generate and add metadata file first so it's at the beginning.
        self.metadata['source'] = self.source.info
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
        print 'Removing temporary directory: ' + self.tempdir
        shutil.rmtree(self.tempdir)

    def update(self, *args, **kwargs):
        pass


class S3OHDataSet(OHDataSet):
    """OHDataSet where input and/or output are in S3."""

    def __init__(self, *args, **kwargs):
        """Open S3-based OHDataSet."""
        self.mode = kwargs['mode']
        self.s3_key_name = kwargs['s3_key_name']
        self.s3_bucket_name = kwargs['s3_bucket_name']

        assert 'filepath' not in kwargs, '"filepath" argument not allowed'

        filename = os.path.basename(kwargs['s3_key_name'])
        filepath_tmp = tempfile.mkstemp()[1]

        # OHDataSet checks that filepaths end with '.tar[|.gz|.bz2]'.
        filepath = filepath_tmp + filename
        shutil.move(filepath_tmp, filepath)

        # Check S3 connection. Copy S3 to local temp file if reading.
        try:
            s3 = boto.connect_s3()
            bucket = s3.get_bucket(self.s3_bucket_name)
        except boto.exception.S3ResponseError:
            raise ValueError('S3 bucket not found: ' + self.s3_bucket_name)

        if self.mode in ['a', 'r+', 'r']:
            try:
                key = bucket.get_key(self.s3_key_name)
                key.get_contents_to_filename(filename)
            except AttributeError:
                raise ValueError('S3 key not found: ' + self.s3_key_name)

            key.close()

        s3.close()

        kwargs['filepath'] = filepath

        # Now we can treat this as an OHDataSet.
        super(S3OHDataSet, self).__init__(*args, **kwargs)

    def close(self, *args, **kwargs):
        """Close S3-based OHDataSet, clean up local temp files."""
        super(S3OHDataSet, self).close(*args, **kwargs)

        if self.mode not in ['a', 'r+', 'w']:
            return

        # Copy to S3 and clean up the temp local filepath.
        s3 = boto.connect_s3()
        bucket = s3.get_bucket(self.s3_bucket_name)
        key = bucket.new_key(self.s3_key_name)

        print 'Setting bucket %s and key %s to contents from %s' % (
            self.s3_bucket_name, self.s3_key_name, self.filepath
        )

        key.set_contents_from_filename(self.filepath)
        key.close()

        s3.close()

        print 'Done copying to S3, removing temp file %s' % self.filepath

        os.remove(self.filepath)

    def update(self, update_url, task_id):
        if not task_id or not update_url:
            return

        print ('Updating main site (%s) with completed files for task_id=%s.' %
               (update_url, task_id))

        requests.post(update_url, data={
            'task_data': json.dumps({
                'task_id': task_id,
                's3_keys': [self.s3_key_name],
            })
        })

