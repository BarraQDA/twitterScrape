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
import string
import unicodedata
import pymp
import re
from dateutil import parser as dateparser
from pytimeparse.timeparse import timeparse

parser = argparse.ArgumentParser(description='Twitter feed regular expression analysis.')

parser.add_argument('-v', '--verbosity',  type=int, default=1)

parser.add_argument('-f', '--filter',     type=str, nargs='+', help='Python expression evaluated to determine whether tweet is included')
parser.add_argument('-t', '--title',      type=str, nargs='+', help='Title of column corresponding to filter')
parser.add_argument(      '--since',    type=str, help='Lower bound tweet date.')
parser.add_argument(      '--until',    type=str, help='Upper bound tweet date.')
parser.add_argument('-l', '--limit',      type=int, help='Limit number of tweets to process')

parser.add_argument('-s', '--score',      type=str, default='1', help='Python expression to evaluate tweet score, for example "1 + retweets + favorites"')

parser.add_argument('-p', '--period',     type=str, default='1 day', help='Time period to measure frequency, for example "1 day".')

parser.add_argument('-o', '--outfile',    type=str, help='Output CSV file, otherwise use stdout.')
parser.add_argument('--no-comments',    action='store_true', help='Do not output descriptive comments')

parser.add_argument('infile', type=str, nargs='?', help='Input CSV file, otherwise use stdin.')

args = parser.parse_args()

filteridx = 0
compiledfilters = []
for filteritem in args.filter:
    compiledfilters.append(compile(filteritem, 'filter ' + str(filteridx), 'eval'))

def evalfilter(filteridx, user, date, retweets, favorites, text, lang, geo, mentions, hashtags, id, permalink, **extra):
    ret = eval(compiledfilters[filteridx])
    return ret

# Parse since and until dates
if args.until:
    args.until = dateparser.parse(args.until).date().isoformat()
if args.since:
    args.since = dateparser.parse(args.since).date().isoformat()

score = compile(args.score, 'score argument', 'eval')
def evalscore(user, date, retweets, favorites, text, lang, geo, mentions, hashtags, id, permalink, **extra):
    return eval(score)

period = timeparse(args.period)
if period is None:
    raise RuntimeError("Period: " + args.period + " not recognised.")

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
    outfile.write('# twitterFrequency\n')
    outfile.write('#     outfile=' + (args.outfile or '<stdout>') + '\n')
    outfile.write('#     infile=' + (args.infile or '<stdin>') + '\n')
    if args.limit:
        outfile.write('#     limit=' + str(args.limit) + '\n')
    for filter in args.filter:
        outfile.write('#     filter=' + filter + '\n')
    if args.title:
        for title in args.title:
            outfile.write('#     title=' + title + '\n')
    if args.since:
        outfile.write('#     since=' + args.since+ '\n')
    if args.until:
        outfile.write('#     until=' + args.until + '\n')
    outfile.write('#     score=' + args.score + '\n')
    if args.period:
        outfile.write('#     period=' + str(args.period) + '\n')

inreader=unicodecsv.DictReader(infile, fieldnames=fieldnames)

outunicodecsv=unicodecsv.writer(outfile)
outunicodecsv.writerow(['date'] + args.title)

if args.verbosity > 1:
    print("Loading twitter data.", file=sys.stderr)

rows=[]
tweetcount = 0
runningscore = [0] * len(args.filter)
while True:
    try:
        while True:
            row = next(inreader)
            row['datesecs'] = int(dateparser.parse(row['date']).strftime('%s'))
            row['score'] = evalscore(**row)

            if not args.since or row['date'] >= args.since:
                break
            if not args.until or row['date'] < args.since:
                break

    except StopIteration:
        break

    filters = []
    for filteridx in range(len(args.filter)):
        thisfilter = evalfilter(filteridx, **row)
        filters.append(thisfilter)
        if thisfilter:
            runningscore[filteridx] += row['score']

    if not any(filters):
        continue

    row['filters'] = filters[:]
    rows.append(row)
    firstrow = rows[0]
    filters = []
    while firstrow and firstrow['datesecs'] - row['datesecs'] > period:
        filters = firstrow['filters']
        for filteridx in range(len(args.filter)):
            if filters[filteridx]:
                runningscore[filteridx] -= firstrow['score']

        del rows[0]
        firstrow = rows[0] if len(rows) else None

    outunicodecsv.writerow([row['date']] + runningscore)

    tweetcount += 1
    if args.limit and tweetcount == args.limit:
        break

outfile.close()
