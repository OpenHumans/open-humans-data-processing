import os

from boto.s3.connection import S3Connection


def copy_file_to_s3(bucket, keypath, filepath):
    """
    Copy a local file to S3.
    """
    key = os.getenv('AWS_ACCESS_KEY_ID')
    secret = os.getenv('AWS_SECRET_ACCESS_KEY')

    if not (key and secret):
        raise Exception('You must specify AWS credentials.')

    s3 = S3Connection(key, secret)

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
