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

parser.add_argument('-f', '--filter',    type=str, required=True, help='Python expression evaluated to determine whether tweet is included')
parser.add_argument('-i', '--ignorecase', action='store_true', help='Convert tweet text to lower case before applying filter')
parser.add_argument('-l', '--limit',      type=int, help='Limit number of tweets to process')

parser.add_argument('-o', '--outfile', type=str, help='Output CSV file, otherwise use stdout')
parser.add_argument('-n', '--number',   type=int, default=0, help='Maximum number of results to output')
parser.add_argument('--no-comments',    action='store_true', help='Do not output descriptive comments')

parser.add_argument('infile', type=str, help='Input CSV file, otherwise use stdin')

args = parser.parse_args()

filter = compile(args.filter, 'filter argument', 'eval')
def evalfilter(user, date, retweets, favorites, text, lang, geo, mentions, hashtags, id, permalink):
    if args.ignorecase:
        text = text.lower()
    return eval(filter)

if args.outfile is None:
    outfile = sys.stdout
else:
    outfile = file(args.outfile, 'w')

if args.infile is None:
    infile = sys.stdin
else:
    infile = file(args.infile, 'r')

# Copy comments at start of infile to outfile. Avoid using tell/seek since
# we want to be able to process stdin.
while True:
    line = infile.readline()
    if line[:1] == '#':
        if not args.no_comments:
            outfile.write(line)
    else:
        fieldnames = next(unicodecsv.reader([line]))
        break

if not args.no_comments:
    outfile.write('# twitterFilter\n')
    outfile.write('#     outfile=' + (args.outfile or '<stdout>') + '\n')
    outfile.write('#     infile=' + (args.infile or '<stdin>') + '\n')
    outfile.write('#     filter=' + args.filter + '\n')
    if args.ignorecase:
        outfile.write('#     ignorecase\n')
    if args.limit:
        outfile.write('#     limit=' + str(args.limit) + '\n')
    if args.number:
        outfile.write('#     number=' + str(args.number) + '\n')

if args.verbosity > 1:
    print("Loading twitter data.", file=sys.stderr)

inreader=unicodecsv.DictReader(infile, fieldnames=fieldnames)

csvwriter=unicodecsv.DictWriter(outfile, fieldnames=fieldnames, extrasaction='ignore')
csvwriter.writeheader()

keptcount    = 0
droppedcount = 0
tweetcount   = 0
for row in inreader:
    try:
        row['retweets'] = int(row['retweets'])
    except ValueError:
        row['retweets'] = 0
    try:
        row['favorites'] = int(row['favorites'])
    except ValueError:
        row['favorites'] = 0

    keep = evalfilter(**row)

    tweetcount += 1
    if keep:
        csvwriter.writerow(row)
        keptcount += 1
    else:
        droppedcount += 1

    if args.limit and tweetcount == args.limit:
        break
    if args.number and keptcount == args.number:
        break

outfile.close()

if args.verbosity > 1:
    print(str(keptcount) + " rows kept, " + str(droppedcount) + " rows dropped.", file=sys.stderr)

