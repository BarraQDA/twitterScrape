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

parser = argparse.ArgumentParser(description='Twitter feed regular expression processing.')

parser.add_argument('-v', '--verbosity',  type=int, default=1)
parser.add_argument('-j', '--jobs',       type=int, help='Number of parallel tasks, default is number of CPUs')
parser.add_argument('-b', '--batch',      type=int, default=1000000, help='Number of tweets to process per batch, or zero for unlimited. May affect performance but not results.')
parser.add_argument('-l', '--limit',     type=int, help='Limit number of tweets to process')

parser.add_argument('-r', '--regexp',     type=str, help='Regular expression.')
parser.add_argument('-i', '--ignorecase', action='store_true', help='Ignore case in regular expression')
parser.add_argument('-t', '--threshold',  type=float, help='Threshold value for word to be output')
parser.add_argument('-n', '--number',     type=int, default=0, help='Maximum number of words to output')
parser.add_argument('-o', '--outfile',    type=str, help='Output file name, otherwise use stdout.')

parser.add_argument('infile', type=str, nargs='?', help='Input CSV file, if missing use stdin.')

args = parser.parse_args()

if args.jobs is None:
    import multiprocessing
    args.jobs = multiprocessing.cpu_count()

if args.regexp is None:
    raise RuntimeError("Regular expression must be provided.")

if args.verbosity > 1:
    print("Using " + str(args.jobs) + " jobs.", file=sys.stderr)

if args.infile is None:
    infile = sys.stdin
else:
    infile = file(args.infile, 'r')

# Open output file already so we catch file error before doing all the hard work
if args.outfile is None:
    outfile = sys.stdout
else:
    outfile = file(args.outfile, 'w')

if args.ignorecase:
    regexp = re.compile(args.regexp, re.IGNORECASE)
else:
    regexp = re.compile(args.regexp)

fields = list(regexp.groupindex)

if args.batch == 0:
    args.batch = sys.maxint

# See https://bytes.com/topic/python/answers/513222-csv-comments#post1997980
def CommentStripper (iterator):
    for line in iterator:
        if line [:1] == '#':
            continue
        if not line.strip ():
            continue
        yield line

inreader=unicodecsv.DictReader(CommentStripper(infile))

if args.verbosity > 1:
    print("Loading twitter data.", file=sys.stderr)

mergedresult = {}
tweetcount = 0
while (tweetcount < args.limit) if args.limit is not None else True:
    if args.verbosity > 2:
        print("Loading twitter batch.", file=sys.stderr)

    rows = []
    batchtotal = min(args.batch, args.limit - tweetcount) if args.limit is not None else args.batch
    batchcount = 0
    while batchcount < batchtotal:
        try:
            rows += [next(inreader)]
            batchcount += 1
        except StopIteration:
            break

    if batchcount == 0:
        break

    if args.verbosity > 2:
        print("Processing twitter batch.", file=sys.stderr)

    tweetcount += batchcount
    rowcount = len(rows)

    results = pymp.shared.list()
    with pymp.Parallel(args.jobs) as p:
        result = {}
        for rowindex in p.range(0, rowcount):
            row = rows[rowindex]
            matches = regexp.finditer(row['text'])
            for match in matches:
                if args.ignorecase:
                    index = tuple(value.lower() for value in match.groupdict().values())
                else:
                    index = tuple(match.groupdict().values())

                score = 1 + int(row['retweets']) + int(row['favorites'])
                result[index] = result.get(index, 0) + score

        if args.verbosity > 3:
            print("Thread " + str(p.thread_num) + " found " + str(len(result)) + " results.", file=sys.stderr)

        with p.lock:
            results += [result]

    for result in results:
        for index in result:
            mergedresult[index] = mergedresult.get(index, 0) + result[index]

if args.verbosity > 1:
    print("Sorting " + str(len(mergedresult)) + " results.", file=sys.stderr)

sortedresult = sorted([{'match': match, 'score':mergedresult[match]}
                                for match in mergedresult.keys()
                                if mergedresult[match] >= args.threshold or 0],
                           key=lambda item: item['score'],
                           reverse=True)

if args.number != 0:
    sortedresult = sortedresult[0:args.number]

for result in sortedresult:
    for idx in range(len(fields)):
        result[fields[idx]] = result['match'][idx]

outfile.write('# ' + args.regexp)
if args.ignorecase:
    outfile.write(', re.IGNORECASE')
outfile.write('\n')

outunicodecsv=unicodecsv.DictWriter(outfile, fieldnames=fields + ['score'], extrasaction='ignore')
outunicodecsv.writeheader()
if len(sortedresult) > 0:
    outunicodecsv.writerows(sortedresult)
outfile.close()
