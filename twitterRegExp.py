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
parser.add_argument('-b', '--batch',      type=int, help='Number of tweets to process per batch. Use to limit memory usage with very large files. May affect performance but not results.')
parser.add_argument('-l', '--limit',      type=int, help='Limit number of tweets to process')
parser.add_argument('-f', '--filter',     type=str, help='Python expression evaluated to determine whether tweet is included')

parser.add_argument('-c', '--column',     type=str, default='text', help='Column to apply regular expression')
parser.add_argument('-r', '--regexp',     type=str, required=True, help='Regular expression applied to tweet text to create output columns.')
parser.add_argument('-i', '--ignorecase', action='store_true', help='Ignore case in regular expression')
parser.add_argument('-s', '--score',      type=str, default='1', help='Python expression to evaluate tweet score, for example "1 + retweets + favorites"')
parser.add_argument('-t', '--threshold',  type=float, help='Threshold value for word to be output')
parser.add_argument('-n', '--number',     type=int, default=0, help='Maximum number of results to output')

parser.add_argument('-o', '--outfile',    type=str, help='Output CSV file, otherwise use stdout.')

parser.add_argument('infile', type=str, nargs='?', help='Input CSV file, otherwise use stdin.')

args = parser.parse_args()

if args.jobs is None:
    import multiprocessing
    args.jobs = multiprocessing.cpu_count()

if args.verbosity > 1:
    print("Using " + str(args.jobs) + " jobs.", file=sys.stderr)

if args.batch is None:
    args.batch = sys.maxint

if args.filter:
    filter = compile(args.filter, 'filter argument', 'eval')
    def evalfilter(user, date, retweets, favorites, text, lang, geo, mentions, hashtags, id, permalink):
        return eval(filter)

if args.ignorecase:
    regexp = re.compile(args.regexp, re.IGNORECASE)
else:
    regexp = re.compile(args.regexp)

fields = list(regexp.groupindex)

score = compile(args.score, 'score argument', 'eval')
def evalscore(user, date, retweets, favorites, text, lang, geo, mentions, hashtags, id, permalink):
    return eval(score)

if args.outfile is None:
    outfile = sys.stdout
else:
    outfile = file(args.outfile, 'w')

if args.infile is None:
    infile = sys.stdin
else:
    infile = file(args.infile, 'r')

# Copy comments at start of infile to outfile
while True:
    pos = infile.tell()
    line = infile.readline()
    if line[:1] == '#':
        outfile.write(line)
    else:
        infile.seek(pos)
        break

outfile.write('# twitterRegExp\n')
outfile.write('#     outfile=' + (args.outfile or '<stdout>') + '\n')
outfile.write('#     infile=' + (args.infile or '<stdin>') + '\n')
if args.limit:
    outfile.write('#     limit=' + str(args.limit) + '\n')
if args.filter:
    outfile.write('#     filter=' + args.filter + '\n')
if args.column != 'text':
    outfile.write('#     column=' + args.column+ '\n')
outfile.write('#     regexp=' + args.regexp + '\n')
if args.ignorecase:
    outfile.write('#     ignorecase\n')
outfile.write('#     score=' + args.score + '\n')
if args.threshold:
    outfile.write('#     threshold=' + str(args.threshold) + '\n')
if args.number:
    outfile.write('#     number=' + str(args.number) + '\n')

inreader=unicodecsv.DictReader(infile)

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
            while batchcount < batchtotal:
                row = next(inreader)
                try:
                    row['retweets'] = int(row['retweets'])
                except ValueError:
                    row['retweets'] = 0
                try:
                    row['favorites'] = int(row['favorites'])
                except ValueError:
                    row['favorites'] = 0
                batchcount += 1
                if (not args.filter) or evalfilter(**row):
                    break

            rows.append(row)
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
            matches = regexp.finditer(row[args.column])
            for match in matches:
                if args.ignorecase:
                    index = tuple(value.lower() for value in match.groupdict().values())
                else:
                    index = tuple(match.groupdict().values())

                result[index] = result.get(index, 0) + evalscore(**row)

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

outunicodecsv=unicodecsv.DictWriter(outfile, fieldnames=fields + ['score'], extrasaction='ignore')
outunicodecsv.writeheader()
if len(sortedresult) > 0:
    outunicodecsv.writerows(sortedresult)
outfile.close()
