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
from TwitterFeed import TwitterRead, TwitterWrite
import sys
from textblob import TextBlob
import string
import pymp
import operator

parser = argparse.ArgumentParser(description='Filter twitter CSV file on text column.')

parser.add_argument('-v', '--verbosity', type=int, default=1)
parser.add_argument('-j', '--jobs',       type=int, help='Number of parallel tasks, default is number of CPUs. May affect performance but not results.')
parser.add_argument('-b', '--batch',      type=int, default=100000, help='Number of tweets to process per batch. Use to limit memory usage with very large files. May affect performance but not results.')

parser.add_argument('-p', '--prelude',    type=str, nargs="*", help='Python code to execute before processing')
parser.add_argument('-f', '--filter',     type=str, required=True, help='Python expression evaluated to determine whether tweet is included')
parser.add_argument(      '--invert',     action='store_true', help='Invert filter, that is, output those tweets that do not match filter')

parser.add_argument('-i', '--ignorecase', action='store_true', help='Convert tweet text to lower case before applying filter')
parser.add_argument(      '--since',      type=str, help='Lower bound tweet date.')
parser.add_argument(      '--until',      type=str, help='Upper bound tweet date.')
parser.add_argument('-l', '--limit',      type=int, help='Limit number of tweets to process')

parser.add_argument('-o', '--outfile', type=str, help='Output CSV file, otherwise use stdout')
parser.add_argument('-r', '--reject',  type=str, help='Output CSV file for rejected tweets')
parser.add_argument('-n', '--number',  type=int, default=0, help='Maximum number of results to output')
parser.add_argument('--no-comments',   action='store_true', help='Do not output descriptive comments')

parser.add_argument('infile', type=str, help='Input CSV file, otherwise use stdin')

args = parser.parse_args()

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

# Parse since and until dates
if args.until:
    args.until = dateparser.parse(args.until).date().isoformat()
if args.since:
    args.since = dateparser.parse(args.since).date().isoformat()

twitterread  = TwitterRead(args.infile, since=args.since, until=args.until, limit=args.limit)
if args.no_comments:
    comments = None
else:
    comments = ''
    if args.outfile:
        comments += (' ' + args.outfile + ' ').center(80, '#') + '\n'
    else:
        comments += '#' * 80 + '\n'

    comments += '# twitterFilter\n'
    comments += '#     outfile=' + (args.outfile or '<stdout>') + '\n'
    if args.reject:
        comments += '#     reject=' + args.reject + '\n'
    comments += '#     infile=' + (args.infile or '<stdin>') + '\n'
    if args.prelude:
        for line in args.prelude:
            comments += '#     prelude=' + line + '\n'
    comments += '#     filter=' + args.filter + '\n'
    if args.invert:
        comments += '#     invert\n'
    if args.ignorecase:
        comments += '#     ignorecase\n'
    if args.since:
        comments += '#     since=' + args.since+ '\n'
    if args.until:
        comments += '#     until=' + args.until + '\n'
    if args.limit:
        comments += '#     limit=' + str(args.limit) + '\n'
    if args.number:
        comments += '#     number=' + str(args.number) + '\n'

    comments += twitterread.comments

twitterwrite = TwitterWrite(args.outfile, comments=comments, fieldnames=twitterread.fieldnames)
if args.reject:
    rejectwrite = TwitterWrite(args.reject, comments=comments, fieldnames=twitterread.fieldnames)

if args.verbosity > 1:
    print("Loading twitter data.", file=sys.stderr)

if args.jobs == 1:
    for row in twitterread:
        keep = evalfilter(**row) or False

        if keep != args.invert:
            twitterwrite.write(row)
        elif args.reject:
            rejectwrite.write(row)

        if args.number and twitterwrite.count == args.number:
            break
else:
    while True:
        if args.verbosity > 2:
            print("Loading twitter batch.", file=sys.stderr)

        rows = []
        batchcount = 0
        while batchcount < args.batch:
            try:
                row = next(twitterread)
                batchcount += 1
                rows.append(row)

            except StopIteration:
                break

        if batchcount == 0:
            break

        if args.verbosity > 2:
            print("Processing twitter batch.", file=sys.stderr)

        rowcount = len(rows)
        results = pymp.shared.list()
        if args.reject:
            rejects = pymp.shared.list()
        with pymp.Parallel(args.jobs) as p:
            result = []
            if args.reject:
                reject = []
            for rowindex in p.range(0, rowcount):
                row = rows[rowindex]
                keep = evalfilter(**row) or False
                row['keep'] = keep
                if keep != args.invert:
                    result.append(row)
                elif args.reject:
                    reject.append(row)

            with p.lock:
                results.append(result)
                if args.reject:
                    rejects.append(reject)

        if args.verbosity > 2:
            print("Merging twitter batch.", file=sys.stderr)

        mergedresult = []
        for result in results:
            mergedresult += result

        sortedresult = sorted(mergedresult,
                              key=lambda item: item['id'],
                              reverse=True)
        for row in sortedresult:
            twitterwrite.write(row)
            if args.number and twitterwrite.count == args.number:
                break

        if args.reject:
            mergedreject = []
            for reject in rejects:
                mergedreject += reject

            sortedreject = sorted(mergedreject,
                                key=lambda item: item['id'],
                                reverse=True)
            for row in sortedreject:
                rejectwrite.write(row)

        if args.number and twitterwrite.count == args.number:
            break

if args.verbosity > 1:
    print(str(twitterwrite.count) + " rows kept, " + str(twitterread.count - twitterwrite.count) + " rows dropped.", file=sys.stderr)

del twitterwrite
