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

def twitterFilter(arglist):

    parser = argparse.ArgumentParser(description='Twitter CSV file processor.',
                                     fromfile_prefix_chars='@')

    parser.add_argument('-v', '--verbosity',  type=int, default=1)
    parser.add_argument('-j', '--jobs',       type=int, help='Number of parallel tasks, default is number of CPUs. May affect performance but not results.')
    parser.add_argument('-b', '--batch',      type=int, default=100000, help='Number of tweets to process per batch. Use to limit memory usage with very large files. May affect performance but not results.')

    parser.add_argument('-p', '--prelude',    type=str, nargs="*", help='Python code to execute before processing')
    parser.add_argument('-f', '--filter',     type=str, help='Python expression evaluated to determine whether tweet is included')
    parser.add_argument('-c', '--column',     type=str, default='text', help='Column to apply regular expression')
    parser.add_argument('-r', '--regexp',     type=str, help='Regular expression applied to tweet text to create output columns.')
    parser.add_argument('-i', '--ignorecase', action='store_true', help='Ignore case in regular expression')
    parser.add_argument(      '--invert',     action='store_true', help='Invert filter, that is, output those tweets that do not pass filter and/or regular expression')

    parser.add_argument(      '--since',      type=str, help='Lower bound tweet date/time in any sensible format')
    parser.add_argument(      '--until',      type=str, help='Upper bound tweet date/time in any sensible format')
    parser.add_argument('-l', '--limit',      type=int, help='Limit number of tweets to process')

    parser.add_argument('-C', '--copy',       action='store_true', help='If true, copy all columns from input file.')
    parser.add_argument('-H', '--header',     type=str, help='Comma-separated list of column names to create.')
    parser.add_argument('-d', '--data',       type=str, help='Python code to produce list of lists to output as columns.')

    parser.add_argument('-o', '--outfile',    type=str, help='Output CSV file, otherwise use stdout.')
    parser.add_argument(      '--rejfile',    type=str, help='Output CSV file for rejected tweets')
    parser.add_argument('-n', '--number',     type=int, default=0, help='Maximum number of results to output')
    parser.add_argument('--no-comments',      action='store_true', help='Do not output descriptive comments')
    parser.add_argument('--no-header',        action='store_true', help='Do not output CSV header with column names')

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

    if args.regexp:
        if args.ignorecase:
            regexp = re.compile(args.regexp, re.IGNORECASE | re.UNICODE)
        else:
            regexp = re.compile(args.regexp, re.UNICODE)

    until = dateparser.parse(args.until) if args.until else None
    since = dateparser.parse(args.since) if args.since else None

    twitterread  = TwitterRead(args.infile, since=since, until=until, limit=args.limit)

    if args.outfile is None:
        outfile = sys.stdout
    else:
        if os.path.exists(args.outfile):
            shutil.move(args.outfile, args.outfile + '.bak')

        outfile = file(args.outfile, 'w')

    if args.rejfile:
        if os.path.exists(args.rejfile):
            shutil.move(args.rejfile, args.rejfile + '.bak')

        rejfile = file(args.rejfile, 'w')

    if args.no_comments:
        outcomments = None
        rejcomments = None
    else:
        comments = ''
        comments += '# twitterFilter\n'
        comments += '#     outfile=' + (args.outfile or '<stdout>') + '\n'
        if args.rejfile:
            comments += '#     rejfile=' + args.rejfile + '\n'
        comments += '#     infile=' + (args.infile or '<stdin>') + '\n'
        if args.limit:
            comments += '#     limit=' + str(args.limit) + '\n'
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
        if args.header:
            comments += '#     header=' + args.header + '\n'
        if args.data:
            comments += '#     data=' + args.data + '\n'
        if args.number:
            comments += '#     number=' + str(args.number) + '\n'
        if args.no_header:
            comments += '#     no-header\n'

        if args.outfile:
            outcomments = (' ' + args.outfile + ' ').center(80, '#') + '\n'
        else:
            outcomments = '#' * 80 + '\n'

        outcomments += comments + twitterread.comments
        outfile.write(outcomments)

        if args.rejfile:
            rejcomments = (' ' + args.rejfile + ' ').center(80, '#') + '\n'
            rejcomments += comments + twitterread.comments
            rejfile.write(rejcomments)

    # Check either copy or header/data are specified
    if args.copy == bool(args.header) or bool(args.header) != bool(args.data):
        raise RuntimeError("You must specify either --copy or both --header and --data")

    outcsv=unicodecsv.writer(outfile, lineterminator=os.linesep)

    if not args.no_header:
        outcsv.writerow(args.header.split(',') if args.header else twitterread.fieldnames)
    if args.rejfile:
        rejcsv=unicodecsv.writer(rejfile, lineterminator=os.linesep)
        if not args.no_header:
            rejcsv.writerow(args.header.split(',') if args.header else twitterread.fieldnames)

    if args.filter:
        exec "\
def evalfilter(" + ','.join(twitterread.fieldnames).replace('-','_') + ", **kwargs):\n\
    return " + args.filter

    if args.data:
        exec "\
def evalcolumn(" + ','.join(twitterread.fieldnames).replace('-','_') + ", **kwargs):\n\
    return " + args.data

    if args.verbosity >= 1:
        print("Loading twitter data.", file=sys.stderr)

    outrowcount = 0
    rejrowcount = 0
    # NB Code for single- and multi-threaded processing is separate
    if args.jobs == 1:
        for row in twitterread:
            keep = True
            rowargs = {key.replace('-','_'): value for key, value in row.iteritems()}
            if args.filter:
                keep = (evalfilter(**rowargs) or False) and keep

            if args.regexp:
                keep = keep and (regexp.search(unicode(row[args.column])) or False)

            if keep == args.invert and not args.rejfile:
                continue

            if args.copy:
                outrows = [[row[key] for key in twitterread.fieldnames]]
            else:
                outrows = evalcolumn(**rowargs)

            for outrow in outrows:
                if keep != args.invert:
                    outcsv.writerow(outrow)
                    outrowcount += 1
                    if args.number and outrowcount == args.number:
                        break
                else:
                    rejcsv.writerow(outrow)
                    rejrowcount += 1

            if args.number and outrowcount == args.number:
                break

        outfile.close()
        if args.rejfile:
            rejfile.close()
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
            if args.rejfile:
                rejects = pymp.shared.list()
            with pymp.Parallel(args.jobs) as p:
                result = {}
                if args.rejfile:
                    reject = {}
                for rowindex in p.range(0, rowcount):
                    row = rows[rowindex]
                    rowargs = {key.replace('-','_'): value for key, value in row.iteritems()}
                    keep = True
                    if args.filter:
                        keep = (evalfilter(**rowargs) or False) and keep
                    if args.regexp:
                        keep = keep and (regexp.search(unicode(row[args.column])) or False)

                    if keep == args.invert and not args.rejfile:
                        continue

                    if args.copy:
                        outrows = [[row[key] for key in twitterread.fieldnames]]
                    else:
                        outrows = evalcolumn(**rowargs)

                    if keep != args.invert:
                        result[row['id']] = outrows
                    else:
                        reject[row['id']] = outrows

                if args.verbosity >= 2:
                    print("Thread " + str(p.thread_num) + " returned " + str(len(result)) + " results.", file=sys.stderr)

                with p.lock:
                    results.append(result)
                    if args.rejfile:
                        rejects.append(reject)

            if args.verbosity >= 2:
                print("Merging batch.", file=sys.stderr)

            mergedresult = {}
            for result in results:
                mergedresult.update(result)

            if args.rejfile:
                mergedreject = {}
                for reject in rejects:
                    mergedreject.update(reject)

            if args.verbosity >= 2:
                print("Outputting batch.", file=sys.stderr)

            endindex = None
            for index in sorted(mergedresult.keys(), reverse=True):
                for outrow in mergedresult[index]:
                    outcsv.writerow(outrow)
                    outrowcount += 1
                    if args.number and outrowcount == args.number:
                        break

                if args.number and outrowcount == args.number:
                    endindex = index
                    break

            if args.rejfile:
                for index in sorted(mergedreject.keys(), reverse=True):
                    if index < endindex:
                        break

                    for outrow in mergedreject[index]:
                        rejcsv.writerow(outrow)

            if args.number and outrowcount == args.number:
                break

        outfile.close()
        if args.rejfile:
            rejfile.close()

if __name__ == '__main__':
    twitterFilter(None)
