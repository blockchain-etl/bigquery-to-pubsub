# MIT License
#
# Copyright (c) 2019 Evgeny Medvedev, evge.medvedev@gmail.com
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import logging
import os
import subprocess


class SortJsonLinesFileJob:
    """Requires jq tool to be installed on the system https://github.com/stedolan/jq/wiki/Installation.
    Can sort files a few GB in size without much RAM. sort_field values must not contain tabs.
    """

    def __init__(
            self,
            filename,
            sort_field,
            sort_timeout=300):
        self.filename = filename
        self.sort_field = sort_field
        self.sort_timeout = sort_timeout

    def run(self):
        logging.info('Sorting jsonlines file {} by {} field.'.format(self.filename, self.sort_field))

        output_filename = self.filename + '.sorted.json'

        sort_command = '''jq -cr '"\\(.{sort_field})\\t\\(.)"' {filename} | sort | cut -f 2 > {output_filename}'''.format(
            sort_field=self.sort_field, filename=self.filename, output_filename=output_filename)

        return_value = subprocess.call(['/bin/bash', '-o', 'pipefail', '-c', sort_command], timeout=self.sort_timeout)

        if return_value != 0:
            raise ValueError('jq command returned non-0 value: ' + str(return_value))

        if not os.path.exists(output_filename):
            raise ValueError('jq command didn\'t produce any output. Check its output above.')

        logging.info('Sorting jsonlines file {} by {} field finished.'.format(self.filename, self.sort_field))

        return output_filename



