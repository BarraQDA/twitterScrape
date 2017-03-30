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
import string
import pymp
from dateutil import parser as dateparser
import re

def twitterFilter(arglist):

    parser = argparse.ArgumentParser(description='Filter twitter CSV file using regular or python expression.',
                                     fromfile_prefix_chars='@')

    parser.add_argument('-v', '--verbosity', type=int, default=1)
    parser.add_argument('-j', '--jobs',       type=int, help='Number of parallel tasks, default is number of CPUs. May affect performance but not results.')
    parser.add_argument('-b', '--batch',      type=int, default=100000, help='Number of tweets to process per batch. Use to limit memory usage with very large files. May affect performance but not results.')

    parser.add_argument('-p', '--prelude',    type=str, nargs="*", help='Python code to execute before processing')
    parser.add_argument('-f', '--filter',     type=str, help='Python expression evaluated to determine whether tweet is included')
    parser.add_argument('-c', '--column',     type=str, default='text', help='Column to apply regular expression')
    parser.add_argument('-r', '--regexp',     type=str, help='Regular expression applied to tweet text to create output columns.')
    parser.add_argument('-i', '--ignorecase', action='store_true', help='Ignore case in regular expression')
    parser.add_argument(      '--invert',     action='store_true', help='Invert filter, that is, output those tweets that do not pass filter and/or regular expression')

    parser.add_argument(      '--since',      type=str, help='Lower bound tweet date.')
    parser.add_argument(      '--until',      type=str, help='Upper bound tweet date.')
    parser.add_argument('-l', '--limit',      type=int, help='Limit number of tweets to process')

    parser.add_argument('-o', '--outfile', type=str, help='Output CSV file, otherwise use stdout')
    parser.add_argument(      '--rejfile',  type=str, help='Output CSV file for rejected tweets')
    parser.add_argument('-n', '--number',  type=int, default=0, help='Maximum number of results to output')
    parser.add_argument('--no-comments',   action='store_true', help='Do not output descriptive comments')

    parser.add_argument('infile', type=str, nargs='?', help='Input CSV file, otherwise use stdin')

    args = parser.parse_args(arglist)

    if args.jobs is None:
        import multiprocessing
        args.jobs = multiprocessing.cpu_count()

    if args.verbosity >= 1:
        print("Using " + str(args.jobs) + " jobs.", file=sys.stderr)

    if args.batch is None:
        args.batch = sys.maxint

    if args.prelude:
        if args.verbosity >= 1:
            print("Executing prelude code.", file=sys.stderr)

        for line in args.prelude:
            exec(line) in globals()

    if args.regexp:
        if args.ignorecase:
            regexp = re.compile(args.regexp, re.IGNORECASE | re.UNICODE)
        else:
            regexp = re.compile(args.regexp, re.UNICODE)

    # Parse since and until dates
    if args.until:
        args.until = dateparser.parse(args.until).date().isoformat()
    if args.since:
        args.since = dateparser.parse(args.since).date().isoformat()

    twitterread  = TwitterRead(args.infile, since=args.since, until=args.until, limit=args.limit)
    if args.no_comments:
        comments = None
    else:
        if args.outfile:
            outcomments = (' ' + args.outfile + ' ').center(80, '#') + '\n'
        else:
            outcomments = '#' * 80 + '\n'

        if args.rejfile:
            rejcomments = (' ' + args.rejfile + ' ').center(80, '#') + '\n'

        comments = ''

        comments += '# twitterFilter\n'
        comments += '#     outfile=' + (args.outfile or '<stdout>') + '\n'
        if args.rejfile:
            comments += '#     rejfile=' + args.rejfile + '\n'
        comments += '#     infile=' + (args.infile or '<stdin>') + '\n'
        if args.prelude:
            for line in args.prelude:
                comments += '#     prelude=' + line + '\n'
        if args.filter:
            comments += '#     filter=' + args.filter + '\n'
        if args.invert:
            comments += '#     invert\n'
        if args.since:
            comments += '#     since=' + args.since+ '\n'
        if args.until:
            comments += '#     until=' + args.until + '\n'
        if args.regexp:
            comments += '#     column=' + args.column+ '\n'
            comments += '#     regexp=' + args.regexp + '\n'
        if args.ignorecase:
            comments += '#     ignorecase\n'
        if args.limit:
            comments += '#     limit=' + str(args.limit) + '\n'
        if args.number:
            comments += '#     number=' + str(args.number) + '\n'

        comments += twitterread.comments

    if args.filter:
        exec "\n\
def evalfilter(" + ','.join(twitterread.fieldnames).replace('-','_') + "):\n\
    return " + args.filter + "\n"

    twitterwrite = TwitterWrite(args.outfile, comments=outcomments+comments, fieldnames=twitterread.fieldnames)
    if args.rejfile:
        rejectwrite = TwitterWrite(args.rejfile, comments=rejcomments+comments, fieldnames=twitterread.fieldnames)

    if args.verbosity >= 1:
        print("Loading twitter data.", file=sys.stderr)

    if args.jobs == 1:
        for row in twitterread:
            keep = True
            if args.filter:
                rowargs = {key.replace('-','_'): value for key, value in row.iteritems()}
                keep = (evalfilter(**rowargs) or False) and keep
            if args.regexp:
                keep = (regexp.search(str(row[args.column])) or False) and keep

            if keep != args.invert:
                twitterwrite.write(row)
            elif args.rejfile:
                rejectwrite.write(row)

            if args.number and twitterwrite.count == args.number:
                break
    else:
        while True:
            if args.verbosity >= 2:
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

            if args.verbosity >= 2:
                print("Processing twitter batch.", file=sys.stderr)

            rowcount = len(rows)
            results = pymp.shared.list()
            if args.rejfile:
                rejects = pymp.shared.list()
            with pymp.Parallel(args.jobs) as p:
                result = []
                if args.rejfile:
                    reject = []
                for rowindex in p.range(0, rowcount):
                    row = rows[rowindex]
                    keep = True
                    if args.filter:
                        rowargs = {key.replace('-','_'): value for key, value in row.iteritems()}
                        keep = (evalfilter(**rowargs) or False) and keep
                    if args.regexp:
                        keep = (regexp.search(str(row[args.column])) or False) and keep

                    row['keep'] = keep
                    if keep != args.invert:
                        result.append(row)
                    elif args.rejfile:
                        reject.append(row)

                with p.lock:
                    results.append(result)
                    if args.rejfile:
                        rejects.append(reject)

            if args.verbosity >= 2:
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

            if args.rejfile:
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

    if args.verbosity >= 1:
        print(str(twitterwrite.count) + " rows kept, " + str(twitterread.count - twitterwrite.count) + " rows dropped.", file=sys.stderr)

    del twitterwrite

if __name__ == '__main__':
    twitterFilter(None)
