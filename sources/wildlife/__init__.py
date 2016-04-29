"""
Create data set for Wild Life of Our Homes

Copyright (C) 2016 PersonalGenomes.org

This software is shared under the "MIT License" license (aka "Expat License"),
see LICENSE.TXT for full license text.
"""

import json
import os
import re
import tempfile

import requests

from data_retrieval.files import get_remote_file, mv_tempfile_to_output
from . import visualization


def create_datafiles(files, task_id=None, update_url=None, **kwargs):
    """
    Create datafiles from a set of Wild Life of Our Homes file links.

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
    tempdir = tempfile.mkdtemp()
    temp_files = []
    data_files = []

    for filename in files:
        url = files[filename]
        filename = get_remote_file(url, tempdir)
        filepath = os.path.join(tempdir, filename)
        base_tags = ['Wild Life of Our Homes']
        if re.search('home-data-', filename):
            temp_files += [{
                'temp_filename': filename,
                'tempdir': tempdir,
                'metadata': {
                    'description': ('Geographical and architectural '
                                    'information about residence'),
                    'tags': ['survey', 'location'] + base_tags,
                }
            }]
        elif (re.search('fungi-kit-', filename) or
              re.search('bacteria-kit-', filename)):
            data_tags = ['OTU'] + base_tags
            vis_tags = ['visualization'] + base_tags
            if re.search('bacteria-kit-', filename):
                data_descr = ('Bacteria 16S-based OTU counts and taxonomic '
                              'classifications')
                data_tags = ['bacteria', '16S'] + data_tags
                vis_descr = ('Visualization of Wild Life of Our Homes '
                             'bacteria data')
                vis_tags = ['bacteria'] + vis_tags
            else:
                data_descr = ('Fungi ITS-based OTU counts and taxonomic '
                              'classifications')
                data_tags = ['fungi', 'ITS'] + data_tags
                vis_descr = ('Visualization of Wild Life of Our Homes fungi '
                             'data')
                vis_tags = ['fungi'] + vis_tags
            counts = visualization.get_counts(filepath=filepath)
            vis_filename = filename.split('.')[0] + '-graphs.png'
            vis_filepath = os.path.join(tempdir, vis_filename)
            visualization.make_pie_charts(counts, vis_filepath)
            temp_files += [
                {
                    'temp_filename': filename,
                    'tempdir': tempdir,
                    'metadata': {
                        'description': data_descr,
                        'tags': data_tags,
                    }
                },
                {
                    'temp_filename': vis_filename,
                    'tempdir': tempdir,
                    'metadata': {
                        'description': vis_descr,
                        'tags': vis_tags,
                    }
                }]

    print 'Finished creating all datasets locally.'

    for file_info in temp_files:
        print 'File info: {}'.format(str(file_info))
        filename = file_info['temp_filename']
        file_tempdir = file_info['tempdir']
        output_path = mv_tempfile_to_output(
            os.path.join(file_tempdir, filename), filename, **kwargs)
        if 's3_key_dir' in kwargs and 's3_bucket_name' in kwargs:
            data_files.append({
                's3_key': output_path,
                'metadata': file_info['metadata'],
            })
    os.rmdir(tempdir)

    print 'Finished moving all datasets to permanent storage.'

    if not (task_id and update_url):
        return

    task_data = {'task_id': task_id,
                 's3_keys': [df['s3_key'] for df in data_files],
                 'data_files': data_files}
    status_msg = ('Updating main site ({}) with completed files for task_id={}'
                  ' with task_data:\n{}'.format(
                      update_url, task_id, json.dumps(task_data)))
    print status_msg
    requests.post(update_url, json={'task_data': task_data})
