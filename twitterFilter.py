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
import pymp
import operator

parser = argparse.ArgumentParser(description='Filter twitter CSV file on text column.')

parser.add_argument('-v', '--verbosity', type=int, default=1)
parser.add_argument('-j', '--jobs',       type=int, help='Number of parallel tasks, default is number of CPUs. May affect performance but not results.')
parser.add_argument('-b', '--batch',      type=int, help='Number of tweets to process per batch. Use to limit memory usage with very large files. May affect performance but not results.')

parser.add_argument('-p', '--prelude',    type=str, nargs="*", help='Python code to execute before processing')
parser.add_argument('-f', '--filter',     type=str, required=True, help='Python expression evaluated to determine whether tweet is included')
parser.add_argument(      '--invert',     action='store_true', help='Invert filter, that is, output those tweets that do not match filter')

parser.add_argument('-i', '--ignorecase', action='store_true', help='Convert tweet text to lower case before applying filter')
parser.add_argument(      '--since',      type=str, help='Lower bound tweet date.')
parser.add_argument(      '--until',      type=str, help='Upper bound tweet date.')
parser.add_argument('-l', '--limit',      type=int, help='Limit number of tweets to process')

parser.add_argument('-o', '--outfile', type=str, help='Output CSV file, otherwise use stdout')
parser.add_argument('-n', '--number',  type=int, default=0, help='Maximum number of results to output')
parser.add_argument('--no-comments',   action='store_true', help='Do not output descriptive comments')

parser.add_argument('infile', type=str, help='Input CSV file, otherwise use stdin')

args = parser.parse_args()

# Multiprocessing is not possible when doing time period processing
if args.jobs is None:
    import multiprocessing
    args.jobs = multiprocessing.cpu_count()

if args.verbosity > 1:
    print("Using " + str(args.jobs) + " jobs.", file=sys.stderr)

if args.batch is None:
    args.batch = sys.maxint

if args.prelude:
    if args.verbosity > 1:
        print("Executing prelude code.", file=sys.stderr)

    for line in args.prelude:
        exec(line)

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
    if args.prelude:
        for line in args.prelude:
            outfile.write('#     prelude=' + line + '\n')
    outfile.write('#     filter=' + args.filter + '\n')
    if args.invert:
        outfile.write('#     invert\n')
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
tweetcount   = 0
if args.jobs == 1:
    for row in inreader:
        if row['id'] == '':
            continue
        if args.until and row['date'] >= args.until:
            continue
        if args.since and row['date'] < args.since:
            break

        #  Calculate fields because they may be used in filter
        row['retweets']  = int(row['retweets'])
        row['favorites'] = int(row['favorites'])

        keep = evalfilter(**row) or False

        tweetcount += 1
        if keep != args.invert:
            csvwriter.writerow(row)
            keptcount += 1

        if args.limit and tweetcount == args.limit:
            break
        if args.number and keptcount == args.number:
            break
else:
    while (tweetcount < args.limit) if args.limit is not None else True:
        if args.verbosity > 2:
            print("Loading twitter batch.", file=sys.stderr)

        rows = []
        batchtotal = min(args.batch, args.limit - tweetcount) if args.limit is not None else args.batch
        batchcount = 0
        while batchcount < batchtotal:
            try:
                row = next(inreader)
                if row['id'] == '':
                    continue

                if args.until and row['date'] >= args.until:
                    continue
                if args.since and row['date'] < args.since:
                    break

                batchcount += 1
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
            result = []
            thiskeptcount    = 0
            for rowindex in p.range(0, rowcount):
                row = rows[rowindex]
                row['retweets']  = int(row['retweets'])
                row['favorites'] = int(row['favorites'])

                keep = evalfilter(**row) or False

                if keep != args.invert:
                    result.append(row)
                    thiskeptcount += 1

            with p.lock:
                results.append(result)

        resultidx = [0] * args.jobs
        resultlen = [len(result) for result in results]
        firstid   = [(results[idx][resultidx[idx]]['id'] if resultidx[idx] < resultlen[idx] else 0) for idx in range(args.jobs) ]
        resultcnt = len([idx for idx in resultidx if resultidx[idx] < resultlen[idx]])
        while True:
            maxidx, _ = max(enumerate(firstid), key=lambda p: p[1])

            csvwriter.writerow(results[maxidx][resultidx[maxidx]])
            keptcount += 1
            if args.number and keptcount == args.number:
                break

            resultidx[maxidx] += 1
            if resultidx[maxidx] < resultlen[maxidx]:
                firstid[maxidx] = results[maxidx][resultidx[maxidx]]['id']
            else:
                firstid[maxidx] = 0
                resultcnt -= 1
                if resultcnt == 0:
                    break

        if args.number and keptcount == args.number:
            break

outfile.close()

if args.verbosity > 1:
    print(str(keptcount) + " rows kept, " + str(tweetcount - keptcount) + " rows dropped.", file=sys.stderr)

