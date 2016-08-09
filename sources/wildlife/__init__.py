"""
Create data set for Wild Life of Our Homes

Copyright (C) 2016 PersonalGenomes.org

This software is shared under the "MIT License" license (aka "Expat License"),
see LICENSE.TXT for full license text.
"""

import os
import re

from base_source import BaseSource

from . import visualization


class WildlifeSource(BaseSource):
    """
    Create datafiles from a set of Wild Life of Our Homes file links.

    Required arguments:
        files: dict containing filenames and URLs to the files
    """

    def __init__(self, files, **kwargs):
        self.files = files

        super(WildlifeSource, self).__init__(**kwargs)

    def create_datafiles(self):
        for filename in self.files:
            url = self.files[filename]
            filename = self.get_remote_file(url)
            filepath = os.path.join(self.temp_directory, filename)
            base_tags = ['Wild Life of Our Homes']

            if re.search('home-data-', filename):
                self.temp_files.append({
                    'temp_filename': filename,
                    'metadata': {
                        'description': ('Geographical and architectural '
                                        'information about residence'),
                        'tags': ['survey', 'location'] + base_tags,
                    }
                })
            elif (re.search('fungi-kit-', filename) or
                  re.search('bacteria-kit-', filename)):
                data_tags = ['OTU'] + base_tags
                vis_tags = ['visualization'] + base_tags

                if re.search('bacteria-kit-', filename):
                    data_descr = ('Bacteria 16S-based OTU counts and '
                                  'taxonomic classifications')
                    data_tags = ['bacteria', '16S'] + data_tags
                    vis_descr = ('Visualization of Wild Life of Our Homes '
                                 'bacteria data')
                    vis_tags = ['bacteria'] + vis_tags
                else:
                    data_descr = ('Fungi ITS-based OTU counts and taxonomic '
                                  'classifications')
                    data_tags = ['fungi', 'ITS'] + data_tags
                    vis_descr = ('Visualization of Wild Life of Our Homes '
                                 'fungi data')
                    vis_tags = ['fungi'] + vis_tags

                counts = visualization.get_counts(filepath=filepath)

                vis_filename = filename.split('.')[0] + '-graphs.png'
                vis_filepath = os.path.join(self.temp_directory, vis_filename)

                visualization.make_pie_charts(counts, vis_filepath)

                self.temp_files.append({
                    'temp_filename': filename,
                    'metadata': {
                        'description': data_descr,
                        'tags': data_tags,
                    }
                })

                self.temp_files.append({
                    'temp_filename': vis_filename,
                    'metadata': {
                        'description': vis_descr,
                        'tags': vis_tags,
                    }
                })
