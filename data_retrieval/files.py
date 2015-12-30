from datetime import datetime
import os
import re
import shutil
import urlparse

from boto.s3.connection import S3Connection
import requests


def now_string():
    """
    Return the current date and time in a format suitable for a filename.

    This is ISO 8601 without the optional date dashes and time colons.
    """
    return datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')


def s3_connection():
    """
    Get an S3 connection using environment variables.
    """
    key = os.getenv('AWS_ACCESS_KEY_ID')
    secret = os.getenv('AWS_SECRET_ACCESS_KEY')
    if not (key and secret):
        raise Exception('You must specify AWS credentials.')
    return S3Connection(key, secret)


def copy_file_to_s3(bucket, keypath, filepath):
    """
    Copy a local file to S3.
    """
    s3 = s3_connection()
    bucket = s3.get_bucket(bucket)
    key = bucket.new_key(keypath)
    # Override MIME type for compressed files, which AWS sets automatically.
    # These can be erroneous and, even if correct, ome browsers try to "help",
    # for example:
    # - 'foo.csv.bz2' is automatically assigned 'text/csv', browser renames the
    #    downloaded file to 'foo.csv'.
    #  - 'foo.csv.bz2' set to type 'application/x-bzip2', browser renames the
    #    downloaded file to 'foo.bz2'.
    if keypath.endswith('.gz') or keypath.endswith('.bz2'):
        key.set_metadata('Content-Type', 'application/octet-stream')

    key.set_contents_from_filename(filepath)
    print 'Setting bucket {} and key {} to contents from {}'.format(
        bucket, keypath, filepath)
    key.close()
    s3.close()


def get_remote_file(url, tempdir):
    """
    Get and save a remote file to temporary directory. Return filename used.
    """
    req = requests.get(url, stream=True)
    orig_filename = ''
    if 'Content-Disposition' in req.headers:
        regex = re.match(r'attachment; filename="(.*)"$',
                         req.headers['Content-Disposition'])
        if regex:
            orig_filename = regex.groups()[0]
    if not orig_filename:
        orig_filename = urlparse.urlsplit(req.url)[2].split('/')[-1]
    print orig_filename
    tempf = open(os.path.join(tempdir, orig_filename), 'wb')
    for chunk in req.iter_content(chunk_size=512 * 1024):
        if chunk:
            tempf.write(chunk)
    tempf.close()
    return orig_filename


def mv_tempfile_to_output(filepath, filename, **kwargs):
    """
    Copy a temp file to S3 or local permanent directory, then delete temp copy.
    """
    if 's3_key_dir' in kwargs and 's3_bucket_name' in kwargs:
        keypath = os.path.join(kwargs['s3_key_dir'], filename)
        copy_file_to_s3(
            bucket=kwargs['s3_bucket_name'],
            keypath=keypath,
            filepath=filepath)
        output_path = keypath
    elif 'filedir' in kwargs:
        output_path = os.path.join(kwargs['filedir'], filename)
        shutil.copy(filepath, output_path)
    else:
        raise ValueError("Must specify S3 key & bucket, or local filedir.")
    os.remove(filepath)
    return output_path
