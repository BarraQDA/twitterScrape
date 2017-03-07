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
from TwitterFeed import TwitterRead
import unicodecsv
import string
import unicodedata
import pymp
import re
import datetime
from dateutil import parser as dateparser
from pytimeparse.timeparse import timeparse
import calendar

parser = argparse.ArgumentParser(description='Twitter feed frequency matrix producer.')

parser.add_argument('-v', '--verbosity',  type=int, default=1)

parser.add_argument('-p', '--prelude',    type=str, nargs="*", help='Python code to execute before processing')
parser.add_argument('-f', '--filter',     type=str, nargs='+', help='Python expression evaluated to determine whether tweet is included')
parser.add_argument('-t', '--title',      type=str, nargs='+', help='Title of column corresponding to filter')
parser.add_argument(      '--since',      type=str, help='Lower bound tweet date.')
parser.add_argument(      '--until',      type=str, help='Upper bound tweet date.')
parser.add_argument('-l', '--limit',      type=int, help='Limit number of tweets to process')

parser.add_argument('-s', '--score',      type=str, default='1', help='Python expression to evaluate tweet score, for example "1 + retweets + favorites"')

parser.add_argument(      '--interval',   type=str, default='1 day', help='Interval for measuring frequency, for example "1 day".')

parser.add_argument('-o', '--outfile',    type=str, help='Output CSV file, otherwise use stdout.')
parser.add_argument('--no-comments',    action='store_true', help='Do not output descriptive comments')

parser.add_argument('infile', type=str, nargs='?', help='Input CSV file, otherwise use stdin.')

args = parser.parse_args()

if args.prelude:
    if args.verbosity > 1:
        print("Executing prelude code.", file=sys.stderr)

    for line in args.prelude:
        exec(line)

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

interval = timeparse(args.interval)
if interval is None:
    raise RuntimeError("Interval: " + args.interval + " not recognised.")

if args.outfile is None:
    outfile = sys.stdout
else:
    outfile = file(args.outfile, 'w')

twitterread  = TwitterRead(args.infile, since=args.since, until=args.until, limit=args.limit)
if not args.no_comments:
    comments = ''

    comments += '# twitterFrequency\n'
    comments += '#     outfile=' + (args.outfile or '<stdout>') + '\n'
    comments += '#     infile=' + (args.infile or '<stdin>') + '\n'
    if args.limit:
        comments += '#     limit=' + str(args.limit) + '\n'
    for filter in args.filter:
        comments += '#     filter=' + filter + '\n'
    if args.title:
        for title in args.title:
            comments += '#     title=' + title + '\n'
    if args.since:
        comments += '#     since=' + args.since+ '\n'
    if args.until:
        comments += '#     until=' + args.until + '\n'
    comments += '#     score=' + args.score + '\n'
    if args.interval:
        comments += '#     interval=' + str(args.interval) + '\n'

    comments += twitterread.comments

    outfile.write(comments)

outunicodecsv=unicodecsv.writer(outfile)
outunicodecsv.writerow(['date'] + args.title)

if args.verbosity > 1:
    print("Loading twitter data.", file=sys.stderr)

rows=[]
runningscore = [0] * len(args.filter)
while True:
    try:
        row = next(twitterread)
    except StopIteration:
        break

    row['datesecs']  = calendar.timegm(dateparser.parse(row['date']).timetuple())
    row['score']     = evalscore(**row)

    firstrow = rows[0] if len(rows) > 0 else None
    while firstrow and firstrow['datesecs'] - row['datesecs'] > interval:
        filters = firstrow['filters']
        for filteridx in range(len(args.filter)):
            if filters[filteridx]:
                runningscore[filteridx] -= firstrow['score']

        if any(filters):
            outunicodecsv.writerow([datetime.datetime.utcfromtimestamp(firstrow['datesecs'] - interval).isoformat()] + runningscore)

        del rows[0]
        firstrow = rows[0] if len(rows) else None

    filters = []
    for filteridx in range(len(args.filter)):
        thisfilter = evalfilter(filteridx, **row)
        filters.append(thisfilter)
        if thisfilter:
            runningscore[filteridx] += row['score']

    if args.limit and twitterread.count == args.limit:
        break

    if not any(filters):
        continue

    row['filters'] = filters[:]
    rows.append(row)

    outunicodecsv.writerow([row['date']] + runningscore)


outfile.close()
