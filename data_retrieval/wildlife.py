"""
Create data set for Wild Life of Our Homes

Copyright (C) 2015 PersonalGenomes.org

This software is shared under the "MIT License" license (aka "Expat License"),
see LICENSE.TXT for full license text.
"""

from .participant_data_set import format_filename, get_dataset, OHDataSource


def create_wildlife_ohdataset(files,
                              task_id=None,
                              update_url=None,
                              **kwargs):
    """
    Create a dataset from a set of Wild Life of Our Homes file links.

    Required arguments:
        files: dict containing filenames and URLs to the files
        filepath OR (s3_bucket_name and s3_key_name): (see below)

    Optional arguments:
        filedir: Local filepath, folder in which to place the resulting file.
        s3_bucket_name: S3 bucket to write resulting file.
        s3_key_dir: S3 key "directory" to write resulting file. The full S3 key
                    name will add a filename to the end of s3_key_dir.

    Either 'filedir' (and no S3 arguments), or both S3 arguments (and no
    'filedir') must be specified.
    """
    # Set up for constructing the OH dataset file.
    source = OHDataSource(
        name='Wild Life of Our Homes',
        url='http://robdunnlab.com/projects/wild-life-of-our-homes/')
    filename = format_filename(source='wildlife',
                               data_type='bacterial-and-fungal-profiling')
    dataset = get_dataset(filename, source, **kwargs)

    for filename in files:
        url = files[filename]
        new_filename = filename[:-4] if filename.endswith('.bz2') else filename
        new_filename = new_filename[:-3] if new_filename.endswith('.gz') else new_filename
        dataset.add_remote_file(
            url=url,
            filename=new_filename)

    dataset.close()
    if update_url and task_id:
        dataset.update(update_url, task_id,
                       subtype='bacterial-and-fungal-profiling')
    return dataset
