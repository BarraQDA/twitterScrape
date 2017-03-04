#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2016 Jonathan Schultz
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

from __future__ import print_function
import argparse
import sys
import unicodecsv
import datetime
from dateutil import parser as dateparser
import matplotlib.pyplot as plt

parser = argparse.ArgumentParser(description='Twitter frequency matrix plotter.')

parser.add_argument('-v', '--verbosity',  type=int, default=1)

parser.add_argument('infile', type=str, nargs='?', help='Input CSV file, otherwise use stdin.')

args = parser.parse_args()

if args.infile is None:
    infile = sys.stdin
else:
    infile = file(args.infile, 'r')

# Copy comments at start of infile to outfile. Avoid using tell/seek since
# we want to be able to process stdin.
while True:
    line = infile.readline()
    if line[:1] != '#':
        fieldnames = next(unicodecsv.reader([line]))
        break

inreader=unicodecsv.reader(infile)

dates = []
values = []
for valueidx in range(len(fieldnames) - 1):
    values.append([])
while True:
    try:
        row = next(inreader)
    except StopIteration:
        break

    dates.append(dateparser.parse(row[0]))
    for valueidx in range(len(fieldnames) - 1):
        values[valueidx].append(row[valueidx+1])

args = ()
for valueidx in range(len(fieldnames) - 1):
    args += (dates, values[valueidx], '')

plt.plot(*args)
plt.gcf().autofmt_xdate()
plt.show()
