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
from textblob import TextBlob
import string
import unicodedata
import re

parser = argparse.ArgumentParser(description='Filter twitter CSV file on text column.')

parser.add_argument('-v', '--verbosity', type=int, default=1)

parser.add_argument('-s', '--search', type=str, help='Search string to filter twitter content')
parser.add_argument('-i', '--ignorecase', action='store_true', help='Ignore case')
parser.add_argument('-r', '--regexp', action='store_true', help='Use regular expression matching')
parser.add_argument('-n', '--no',     action='store_true', help='Keep tweets that do not match pattern')

parser.add_argument('-o', '--outfile', type=str, help='Output file name, otherwise use stdout')

parser.add_argument('infile', type=str, help='Input CSV file, or "-" to use stdin')

args = parser.parse_args()

ignorecase = args.ignorecase
no         = args.no
regexp     = args.regexp

if regexp:
    if ignorecase:
        regexp = re.compile(args.search, re.IGNORECASE)
    else:
        regexp = re.compile(args.search)
else:
    if ignorecase:
        search = args.search.lower()
    else:
        search = args.search

if args.infile == '-':
    infile = sys.stdin
else:
    infile = file(args.infile, 'r')

# See https://bytes.com/topic/python/answers/513222-csv-comments#post1997980
def CommentStripper (iterator):
    for line in iterator:
        if line [:1] == '#':
            continue
        if not line.strip ():
            continue
        yield line

inreader=unicodecsv.DictReader(CommentStripper(infile))
fieldnames = inreader.fieldnames

# Open output file already so we catch file error before doing all the hard work
if args.outfile is None:
    outfile = sys.stdout
else:
    outfile = file(args.outfile, 'w')

csvwriter=unicodecsv.DictWriter(outfile, fieldnames=fieldnames, extrasaction='ignore')
csvwriter.writeheader()

keptcount    = 0
droppedcount = 0

for row in inreader:
    if regexp:
        keep = (regexp.search(row['text']) != None) != no
    else:
        if ignorecase:
            keep = (search in row['text'].lower()) != no
        else:
            keep = (search in row['text']) != no

    if keep:
        csvwriter.writerow(row)
        keptcount += 1
    else:
        droppedcount += 1

print(str(keptcount) + " rows kept, " + str(droppedcount) + " rows dropped.", file=sys.stderr)

outfile.close()
