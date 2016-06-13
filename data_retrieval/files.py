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
    # - 'foo.csv.bz2' set to type 'application/x-bzip2', browser renames the
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
    assert os.path.exists(tempdir), 'get_remote_file: path not found: "{}"'

    print 'get_remote_file: retrieving "{}"'.format(url)
    print 'get_remote_file: using temporary directory "{}"'.format(url)

    # start a GET request but don't retrieve the data; we'll start retrieval
    # below
    request = requests.get(url, stream=True)

    if request.status_code != 200:
        raise Exception('File URL not working! Data processing aborted: {}'
                        .format(url))

    specified_filename = ''

    # try to retrieve the filename via the 'Content-Disposition' filename
    # header
    if 'Content-Disposition' in request.headers:
        filename = re.match(r'attachment; filename="(.*)"$',
                            request.headers['Content-Disposition'])

        if filename:
            specified_filename = filename.groups()[0]

    # if that header isn't sent then use the last portion of the URL as the
    # filename ('https://test.com/hello/world.zip' becomes 'world.zip')
    if not specified_filename:
        specified_filename = urlparse.urlsplit(request.url)[2].split('/')[-1]

    print 'get_remote_file: filename "{}"'.format(specified_filename)

    temp_file = open(os.path.join(tempdir, specified_filename), 'wb')

    # write each streamed chunk to the temporary file
    for chunk in request.iter_content(chunk_size=512 * 1024):
        if chunk:
            temp_file.write(chunk)

    temp_file.close()

    return specified_filename


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
        raise ValueError('Must specify S3 key & bucket, or local filedir.')
    os.remove(filepath)
    return output_path
