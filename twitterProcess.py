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
import os
import shutil
import unicodecsv
import string
import unicodedata
import multiprocessing
import pymp
import re
from dateutil import parser as dateparser
from collections import OrderedDict

def twitterProcess(arglist):

    parser = argparse.ArgumentParser(description='Twitter CSV file processor.',
                                     fromfile_prefix_chars='@')

    parser.add_argument('-v', '--verbosity',  type=int, default=1)
    parser.add_argument('-j', '--jobs',       type=int, help='Number of parallel tasks, default is number of CPUs. May affect performance but not results.')
    parser.add_argument('-b', '--batch',      type=int, default=100000, help='Number of tweets to process per batch. Use to limit memory usage with very large files. May affect performance but not results.')

    parser.add_argument('-p', '--prelude',    type=str, nargs="*", help='Python code to execute before processing')
    parser.add_argument('-f', '--filter',     type=str, help='Python expression evaluated to determine whether tweet is included')
    parser.add_argument(      '--since',      type=str, help='Lower bound tweet date.')
    parser.add_argument(      '--until',      type=str, help='Upper bound tweet date.')
    parser.add_argument('-l', '--limit',      type=int, help='Limit number of tweets to process')

    parser.add_argument('-c', '--column',     type=str, help='Comma-separated list of column names.')
    parser.add_argument('-d', '--data',     type=str, required=True, help='Python code to produce list of lists to output as columns.')

    parser.add_argument('-o', '--outfile',    type=str, help='Output CSV file, otherwise use stdout.')
    parser.add_argument('-n', '--number',     type=int, default=0, help='Maximum number of results to output')
    parser.add_argument('--no-comments',    action='store_true', help='Do not output descriptive comments')

    parser.add_argument('infile', type=str, nargs='?', help='Input CSV file, otherwise use stdin.')

    args = parser.parse_args(arglist)

    if args.jobs is None:
        args.jobs = multiprocessing.cpu_count()

    if args.verbosity >= 1:
        print("Using " + str(args.jobs) + " jobs.", file=sys.stderr)

    if args.batch is None:
        args.batch = sys.maxint

    if args.prelude:
        if args.verbosity >= 1:
            print("Executing prelude code.", file=sys.stderr)

        exec(os.linesep.join(args.prelude)) in globals()
        #for line in args.prelude:
            #exec(line) in globals()

    if args.until:
        args.until = dateparser.parse(args.until).date().isoformat()
    if args.since:
        args.since = dateparser.parse(args.since).date().isoformat()

    if args.outfile is None:
        outfile = sys.stdout
    else:
        if os.path.exists(args.outfile):
            shutil.move(args.outfile, args.outfile + '.bak')

        outfile = file(args.outfile, 'w')

    twitterread  = TwitterRead(args.infile, since=args.since, until=args.until, limit=args.limit)
    if not args.no_comments:
        comments = ''
        if args.outfile:
            comments += (' ' + args.outfile + ' ').center(80, '#') + '\n'
        else:
            comments += '#' * 80 + '\n'

        comments += '# twitterProcess\n'
        comments += '#     outfile=' + (args.outfile or '<stdout>') + '\n'
        comments += '#     infile=' + (args.infile or '<stdin>') + '\n'
        if args.limit:
            comments += '#     limit=' + str(args.limit) + '\n'
        if args.prelude:
            for line in args.prelude:
                comments += '#     prelude=' + line + '\n'
        if args.filter:
            comments += '#     filter=' + args.filter + '\n'
        if args.since:
            comments += '#     since=' + args.since+ '\n'
        if args.until:
            comments += '#     until=' + args.until + '\n'
        if args.column:
            comments += '#     column=' + args.column + '\n'
        comments += '#     data=' + args.data + '\n'
        if args.number:
            comments += '#     number=' + str(args.number) + '\n'

        outfile.write(comments + twitterread.comments)

    if args.column:
        outunicodecsv=unicodecsv.writer(outfile, lineterminator=os.linesep)
        outunicodecsv.writerow(args.column.split(','))

    if args.filter:
            exec "\
def evalfilter(" + ','.join(twitterread.fieldnames).replace('-','_') + ", **kwargs):\n\
    return " + args.filter

    exec "\
def evalcolumn(" + ','.join(twitterread.fieldnames).replace('-','_') + ", **kwargs):\n\
    return " + args.data

    if args.verbosity >= 1:
        print("Loading twitter data.", file=sys.stderr)

    outrowcount = 0
    # NB Code for single- and multi-threaded processing is separate
    if args.jobs == 1:
        while True:
            try:
                while True:
                    row = next(twitterread)
                    rowargs = {key.replace('-','_'): value for key, value in row.iteritems()}
                    if args.filter:
                        if evalfilter(**rowargs):
                            break
                    else:
                        break

            except StopIteration:
                break

            outrows = evalcolumn(**rowargs)
            for outrow in outrows:
                outunicodecsv.writerow(outrow)
                outrowcount += 1
                if args.number and outrowcount == args.number:
                    break

            if args.number and outrowcount == args.number:
                break

        outfile.close()
    else:
        while True:
            if args.verbosity >= 2:
                print("Loading batch.", file=sys.stderr)

            rows = []
            batchcount = 0
            while batchcount < args.batch:
                try:
                    rows.append(next(twitterread))
                    batchcount += 1
                except StopIteration:
                    break

            if batchcount == 0:
                break

            if args.verbosity >= 2:
                print("Processing batch.", file=sys.stderr)

            rowcount = len(rows)
            results = pymp.shared.list()
            with pymp.Parallel(args.jobs) as p:
                result = {}
                for rowindex in p.range(0, rowcount):
                    row = rows[rowindex]
                    rowargs = {key.replace('-','_'): value for key, value in row.iteritems()}
                    if args.filter:
                        if not evalfilter(**rowargs):
                            continue

                    outrows = evalcolumn(**rowargs)
                    result[row['id']] = outrows

                if args.verbosity >= 2:
                    print("Thread " + str(p.thread_num) + " returned " + str(len(result)) + " results.", file=sys.stderr)

                with p.lock:
                    results.append(result)

            if args.verbosity >= 2:
                print("Merging batch.", file=sys.stderr)

            mergedresult = {}
            for result in results:
                mergedresult.update(result)
                #for index in result:
                    #mergedresult[index] = result[index]

            if args.verbosity >= 2:
                print("Outputting batch.", file=sys.stderr)

            for index in sorted(mergedresult.keys(), reverse=True):
                outrows = mergedresult[index]

                for outrow in outrows:
                    outunicodecsv.writerow(outrow)
                    outrowcount += 1
                    if args.number and outrowcount == args.number:
                        break

                if args.number and outrowcount == args.number:
                    break

            if args.number and outrowcount == args.number:
                break

        outfile.close()

if __name__ == '__main__':
    twitterProcess(None)
