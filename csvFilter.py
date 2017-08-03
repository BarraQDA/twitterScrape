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
import os
import shutil
import unicodecsv
import string
import unicodedata
import multiprocessing
import pymp
import re
from dateutil import parser as dateparser

def csvFilter(arglist):

    parser = argparse.ArgumentParser(description='CSV file processor.',
                                     fromfile_prefix_chars='@')

    parser.add_argument('-v', '--verbosity',  type=int, default=1)
    parser.add_argument('-j', '--jobs',       type=int, help='Number of parallel tasks, default is number of CPUs. May affect performance but not results.')
    parser.add_argument('-b', '--batch',      type=int, default=100000, help='Number of rows to process per batch. Use to limit memory usage with very large files. May affect performance but not results.')

    parser.add_argument('-p', '--prelude',    type=str, nargs="*", help='Python code to execute before processing')
    parser.add_argument('-f', '--filter',     type=str, help='Python expression evaluated to determine whether row is included')
    parser.add_argument('-c', '--column',     type=str, default='text', help='Column to apply regular expression')
    parser.add_argument('-r', '--regexp',     type=str, help='Regular expression to create output columns.')
    parser.add_argument('-i', '--ignorecase', action='store_true', help='Ignore case in regular expression')
    parser.add_argument(      '--invert',     action='store_true', help='Invert filter, that is, output those tweets that do not pass filter and/or regular expression')

    parser.add_argument(      '--since',      type=str, help='Lower bound date/time in any sensible format')
    parser.add_argument(      '--until',      type=str, help='Upper bound date/time in any sensible format')
    parser.add_argument('-l', '--limit',      type=int, help='Limit number of rows to process')

    parser.add_argument('-C', '--copy',       action='store_true', help='If true, copy all columns from input file.')
    parser.add_argument('-x', '--exclude',    type=str, help='If specified, copy all columns from input file except those in this comma-separated list')
    parser.add_argument('-H', '--header',     type=str, help='Comma-separated list of column names to create.')
    parser.add_argument('-d', '--data',       type=str, help='Python code to produce dict to fill or overwrite columns.')

    parser.add_argument('-o', '--outfile',    type=str, help='Output CSV file, otherwise use stdout.')
    parser.add_argument(      '--rejfile',    type=str, help='Output CSV file for rejected rows')
    parser.add_argument('-n', '--number',     type=int, help='Maximum number of results to output')
    parser.add_argument('--no-comments',      action='store_true', help='Do not output descriptive comments')
    parser.add_argument('--no-header',        action='store_true', help='Do not output CSV header with column names')

    parser.add_argument('infile', type=str, nargs='?', help='Input CSV file, otherwise use stdin.')

    args = parser.parse_args(arglist)
    hiddenargs = ['verbosity', 'jobs', 'batch', 'no_comments']

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
        regexp = re.compile(args.regexp, re.UNICODE | (re.IGNORECASE if args.ignorecase else 0))
        regexpfields = list(regexp.groupindex)
    else:
        regexpfields = None

    until = dateparser.parse(args.until) if args.until else None
    since = dateparser.parse(args.since) if args.since else None

    if args.infile is None:
        infile = sys.stdin
    else:
        infile = file(args.infile, 'rU')

    # Skip comments at start of infile.
    incomments = ''
    while True:
        line = infile.readline()
        if line[:1] == '#':
            incomments += line
        else:
            infieldnames = next(unicodecsv.reader([line]))
            break

    inreader=unicodecsv.DictReader(infile, fieldnames=infieldnames)

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
        comments = '# ' + os.path.basename(sys.argv[0]) + '\n'
        arglist = args.__dict__.keys()
        for arg in arglist:
            if arg not in hiddenargs:
                val = getattr(args, arg)
                if type(val) == str or type(val) == unicode:
                    comments += '#     --' + arg + '="' + val + '"\n'
                elif type(val) == bool:
                    if val:
                        comments += '#     --' + arg + '\n'
                elif type(val) == list:
                    for valitem in val:
                        if type(valitem) == str:
                            comments += '#     --' + arg + '="' + valitem + '"\n'
                        else:
                            comments += '#     --' + arg + '=' + str(valitem) + '\n'
                elif val is not None:
                    comments += '#     --' + arg + '=' + str(val) + '\n'

        if args.outfile:
            outcomments = (' ' + args.outfile + ' ').center(80, '#') + '\n'
        else:
            outcomments = '#' * 80 + '\n'

        outcomments += comments + incomments
        outfile.write(outcomments)

        if args.rejfile:
            rejcomments = (' ' + args.rejfile + ' ').center(80, '#') + '\n'
            rejcomments += comments + incomments
            rejfile.write(rejcomments)

    # If no columns specified, assume we mean copy columns from infile
    if not(args.exclude or bool(args.header) or bool(regexpfields)):
        args.copy = True

    outfieldnames = []
    if args.exclude:
        outfieldnames += [fieldname for fieldname in infieldnames if fieldname not in args.exclude.split(',')]
    elif args.copy:
        outfieldnames += infieldnames
    if args.header:
        outfieldnames += [fieldname for fieldname in args.header.split(',') if fieldname not in outfieldnames]
    if regexpfields:
        outfieldnames += [fieldname for fieldname in regexpfields if fieldname not in outfieldnames]

    outcsv=unicodecsv.DictWriter(outfile, fieldnames=outfieldnames, extrasaction='ignore', lineterminator=os.linesep)

    if not args.no_header:
        outcsv.writeheader()
    if args.rejfile:
        rejcsv=unicodecsv.DictWriter(rejfile, fieldnames=outfieldnames, extrasaction='ignore', lineterminator=os.linesep)
        if not args.no_header:
            rejcsv.writeheader()

    argbadchars = re.compile(r'[^0-9a-zA-Z_]')
    if args.filter:
        exec "\
def evalfilter(" + ','.join([argbadchars.sub('_', fieldname) for fieldname in infieldnames]) + ",**kwargs):\n\
    return " + args.filter in locals()

    if args.data:
        exec "\
def evaldata(" + ','.join([argbadchars.sub('_', fieldname) for fieldname in infieldnames]) + ",**kwargs):\n\
    return " + args.data in locals()

    if args.verbosity >= 1:
        print("Loading CSV data.", file=sys.stderr)

    inrowcount = 0
    outrowcount = 0
    rejrowcount = 0
    # NB Code for single- and multi-threaded processing is separate
    if args.jobs == 1:
        for row in inreader:
            if args.limit and inrowcount == args.limit:
                break
            inrowcount += 1

            rowargs = {argbadchars.sub('_', key): value for key, value in row.iteritems()}
            keep = True
            if args.filter:
                keep = evalfilter(**rowargs) or False
            if keep and args.regexp:
                regexpmatch = regexp.match(unicode(row[args.column]))
                keep = regexpmatch or False
            if keep and (since or until):
                date = row.get('date')
                if date:
                    date = dateparser.parse(date)
                    if until and date >= until:
                        keep = False
                    elif since and date < since:
                        keep = False

            if keep == args.invert and not args.rejfile:
                continue

            outrow = row.copy()
            if args.regexp and regexpmatch:
                outrow.update({regexpfield: regexpmatch.group(regexpfield) for regexpfield in regexpfields})
            if args.data:
                datadict = evaldata(**rowargs)
                outrow.update(datadict)

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
                if args.limit and inrowcount == args.limit:
                    break
                try:
                    row = next(inreader)
                    inrowcount += 1
                    row['_Id'] = batchcount
                    rows.append(row)
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

                    rowargs = {argbadchars.sub('_', key): value for key, value in row.iteritems()}
                    keep = True
                    if args.filter:
                        keep = evalfilter(**rowargs) or False
                    if keep and args.regexp:
                        regexpmatch = regexp.match(unicode(row[args.column]))
                        keep = regexpmatch or False
                    if keep and (since or until):
                        date = row.get('date')
                        if date:
                            date = dateparser.parse(date)
                            if until and date >= until:
                                keep = False
                            elif since and date < since:
                                keep = False

                    if keep == args.invert and not args.rejfile:
                        continue

                    outrow = row.copy()
                    if args.regexp and regexpmatch:
                        outrow.update({regexpfield: regexpmatch.group(regexpfield) for regexpfield in regexpfields})
                    if args.data:
                        datadict = evaldata(**rowargs)
                        outrow.update(datadict)

                    if keep != args.invert:
                        result[row['_Id']] = outrow
                    else:
                        reject[row['_Id']] = outrow

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
            for index in sorted(mergedresult.keys()):
                outcsv.writerow(mergedresult[index])
                outrowcount += 1
                if args.number and outrowcount == args.number:
                    break

                if args.number and outrowcount == args.number:
                    endindex = index
                    break

            if args.rejfile:
                for index in sorted(mergedreject.keys()):
                    if index < endindex:
                        break

                    rejcsv.writerow(mergedreject[index])

            if args.number and outrowcount == args.number:
                break

        outfile.close()
        if args.rejfile:
            rejfile.close()

if __name__ == '__main__':
    csvFilter(None)
