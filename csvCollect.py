#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2017 Jonathan Schultz
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
import calendar
from pytimeparse.timeparse import timeparse
from operator import sub, add

def csvCollect(arglist):

    parser = argparse.ArgumentParser(description='CSV data collection.',
                                     fromfile_prefix_chars='@')

    parser.add_argument('-v', '--verbosity',  type=int, default=1)
    parser.add_argument('-j', '--jobs',       type=int, help='Number of parallel tasks, default is number of CPUs. May affect performance but not results.')
    parser.add_argument('-b', '--batch',      type=int, default=100000, help='Number of rows to process per batch. Use to limit memory usage with very large files. May affect performance but not results.')

    parser.add_argument('-p', '--prelude',    type=str, nargs="*", help='Python code to execute before processing')
    parser.add_argument('-f', '--filter',     type=str, help='Python expression evaluated to determine whether row is included')
    parser.add_argument(      '--since',      type=str, help='Lower bound date/time in any sensible format.')
    parser.add_argument(      '--until',      type=str, help='Upper bound date/time in any sensible format.')
    parser.add_argument('-l', '--limit',      type=int, help='Limit number of rows to process')

    parser.add_argument('-c', '--column',     type=str, help='Column to apply regular expression, default is "text"')
    parser.add_argument('-r', '--regexp',     type=str, help='Regular expression to create output columns.')
    parser.add_argument('-i', '--ignorecase', action='store_true', help='Ignore case in regular expression')
    parser.add_argument('-s', '--score',      type=str, nargs="*", default=['1'], help='Python expression(s) to evaluate row score(s), for example "1 + retweets + favorites"')
    parser.add_argument('-t', '--threshold',  type=float, help='Threshold (first) score for result to be output')

    parser.add_argument('-in', '--interval',  type=str, help='Interval for measuring frequency, for example "1 day".')

    parser.add_argument('-H', '--header',     type=str, help='Comma-separated list of column names to create.')
    parser.add_argument('-d', '--data',       type=str, nargs="*", help='Python code to produce list of lists to output as columns.')

    parser.add_argument('-o', '--outfile',    type=str, help='Output CSV file, otherwise use stdout.')
    parser.add_argument('-n', '--number',     type=int, help='Maximum number of results to output')
    parser.add_argument('--no-comments',      action='store_true', help='Do not output descriptive comments')
    parser.add_argument('--no-header',        action='store_true', help='Do not output CSV header with column names')

    parser.add_argument('infile', type=str, nargs='?', help='Input CSV file, otherwise use stdin.')

    args = parser.parse_args(arglist)
    hiddenargs = ['verbosity', 'jobs', 'batch', 'preset', 'no_comments']

    if (args.regexp is None) == (args.data is None):
        raise RuntimeError("Exactly one of 'data' and 'regexp' must be specified.")

    if args.regexp and not args.column:
        raise RuntimeError("'column' must be specified for regexp.")

    if args.interval:   # Multiprocessing requires single thread
        args.jobs = 1
    else:
        if args.jobs is None:
            args.jobs = multiprocessing.cpu_count()

        if args.verbosity >= 1:
            print("Using " + str(args.jobs) + " jobs.", file=sys.stderr)

        if args.batch is None:
            args.batch = sys.maxint

    if args.prelude:
        if args.verbosity >= 1:
            print("Executing prelude code.", file=sys.stderr)

        for line in args.prelude:
            exec(line) in locals()

    fields = []
    if args.regexp:
        regexp = re.compile(args.regexp, re.UNICODE | (re.IGNORECASE if args.ignorecase else 0))
        fields += list(regexp.groupindex)
    if args.data:
        if args.header:
            fields += args.header.split(',')
            if len(args.data) != len(fields):
                raise RuntimeError("Number of column headers must equal number of data items.")
        else:
            fields = list(range(1, len(args.data)+1))

    until = dateparser.parse(args.until) if args.until else None
    since = dateparser.parse(args.since) if args.since else None

    if args.interval:
        interval = timeparse(args.interval)
        if interval is None:
            raise RuntimeError("Interval: " + args.interval + " not recognised.")

    if args.infile is None:
        infile = sys.stdin
    else:
        infile = file(args.infile, 'rU')

    # Collect comments and open infile.
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

    if not args.no_comments:
        comments = ((' ' + args.outfile + ' ') if args.outfile else '').center(80, '#') + '\n'
        comments += '# ' + os.path.basename(sys.argv[0]) + '\n'
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

        outfile.write(comments + incomments)

    # Dynamic code for filter, data and score
    argbadchars = re.compile(r'[^0-9a-zA-Z_]')
    if args.filter:
            exec "\
def evalfilter(" + ','.join([argbadchars.sub('_', fieldname) for fieldname in infieldnames]) + ",**kwargs):\n\
    return " + args.filter in locals()

    if args.data:
        exec "\
def evalcolumn(" + ','.join([argbadchars.sub('_', fieldname) for fieldname in infieldnames]) + ",**kwargs):\n\
    return " + '\n'.join(args.data) in locals()

    exec "\
def evalscore(" + ','.join([argbadchars.sub('_', fieldname) for fieldname in infieldnames]) + ",**kwargs):\n\
    return [" + ','.join([scoreitem for scoreitem in args.score]) + "]" in locals()

    if args.verbosity >= 1:
        print("Loading CSV data.", file=sys.stderr)

    inrowcount = 0
    # NB Code for single- and multi-threaded processing is separate
    mergedresult = {}
    if args.jobs == 1:
        rows=[]
        if args.interval:
            runningresult = {}
        while True:
            if args.limit and inrowcount == args.limit:
                break

            try:
                while True:
                    row = next(inreader)
                    inrowcount += 1
                    rowargs = {argbadchars.sub('_', key): value for key, value in row.iteritems()}
                    keep = True
                    if args.filter:
                        keep = evalfilter(**rowargs) or False
                    if keep and (since or until):
                        date = row.get('date')
                        if date:
                            date = dateparser.parse(date)
                            if until and date >= until:
                                keep = False
                            elif since and date < since:
                                keep = False

                    if keep:
                        break

            except StopIteration:
                break

            # Deal with frequency calculation using column 'date'
            if args.interval:
                row['datesecs'] = calendar.timegm(row['date'].timetuple())
                firstrow = rows[0] if len(rows) else None
                while firstrow and firstrow['datesecs'] - row['datesecs'] > interval:
                    indexes  = firstrow['indexes']
                    rowscore = firstrow['score']
                    for index in indexes:
                        runningresult[index] = map(sub, runningresult[index], rowscore)

                    del rows[0]
                    firstrow = rows[0] if len(rows) else None

            rowscore = None
            indexes = []
            if args.regexp:
                matches = regexp.finditer(unicode(row[args.column]))
                for match in matches:
                    if not rowscore:
                        rowscore = evalscore(**rowargs)

                    if args.ignorecase:
                        index = tuple(value.lower() for value in match.groupdict().values())
                    else:
                        index = tuple(match.groupdict().values())

                    if args.interval:
                        indexes.append(index)
                        runningresult[index] = map(add, runningresult.get(index, [0] * len(args.score)), rowscore)
                        curmergedresult = mergedresult.get(index, [0] * len(args.score))
                        mergedresult[index] = [max(curmergedresult[idx], runningresult[index][idx]) for idx in range(len(args.score))]
                    else:
                        mergedresult[index] = map(add, mergedresult.get(index, [0] * len(args.score)), rowscore)

            if args.data:
                matches = evalcolumn(**rowargs)
                for match in matches:
                    if not rowscore:
                        rowscore = evalscore(**rowargs)

                    if args.ignorecase:
                        index = tuple(value.lower() for value in match)
                    else:
                        index = tuple(value for value in match)

                    if args.interval:
                        indexes.append(index)
                        runningresult[index] = map(add, runningresult.get(index, [0] * len(args.score)), rowscore)
                        curmergedresult = mergedresult.get(index, [0] * len(args.score))
                        mergedresult[index] = [max(curmergedresult[idx], runningresult[index][idx]) for idx in range(len(args.score))]
                    else:
                        mergedresult[index] = map(add, mergedresult.get(index, [0] * len(args.score)), rowscore)

            if args.interval and rowscore:
                row['score']   = rowscore
                row['indexes'] = indexes
                rows.append(row)

    else:
        while True:
            if args.verbosity >= 2:
                print("Loading CSV batch.", file=sys.stderr)

            rows = []
            batchcount = 0
            while batchcount < args.batch:
                if args.limit and inrowcount == args.limit:
                    break
                try:
                    rows.append(next(inreader))
                    inrowcount += 1
                    batchcount += 1
                except StopIteration:
                    break

            if batchcount == 0:
                break

            if args.verbosity >= 2:
                print("Processing CSV batch.", file=sys.stderr)

            rowcount = len(rows)
            results = pymp.shared.list()
            with pymp.Parallel(args.jobs) as p:
                result = {}
                for rowindex in p.range(0, rowcount):
                    row = rows[rowindex]
                    rowargs = {argbadchars.sub('_', key): value for key, value in row.iteritems()}
                    keep = True
                    if args.filter:
                        keep = evalfilter(**rowargs) or False
                    if keep and (since or until):
                        date = row.get('date')
                        if date:
                            date = dateparser.parse(date)
                            if until and date >= until:
                                keep = False
                            elif since and date < since:
                                keep = False

                    if not keep:
                        continue

                    rowscore = None
                    if args.regexp:
                        matches = regexp.finditer(unicode(row[args.column]))
                        rowscore = None
                        for match in matches:
                            if not rowscore:
                                rowscore = evalscore(**rowargs)

                            if args.ignorecase:
                                index = tuple(value.lower() for value in match.groupdict().values())
                            else:
                                index = tuple(match.groupdict().values())

                            result[index] = map(add, result.get(index, [0] * len(args.score)), rowscore)

                    if args.data:
                        matches = evalcolumn(**rowargs)
                        for match in matches:
                            if not rowscore:
                                rowscore = evalscore(**rowargs)

                            if args.ignorecase:
                                index = tuple(value.lower() for value in match)
                            else:
                                index = tuple(value for value in match)

                            result[index] = map(add, result.get(index, [0] * len(args.score)), rowscore)

                if args.verbosity >= 2:
                    print("Thread " + str(p.thread_num) + " found " + str(len(result)) + " results.", file=sys.stderr)

                with p.lock:
                    results.append(result)

            for result in results:
                for index in result:
                    mergedresult[index] = map(add, mergedresult.get(index, [0] * len(args.score)), result[index])

    if args.verbosity >= 1:
        print("Sorting " + str(len(mergedresult)) + " results.", file=sys.stderr)

    # Sort on first score value
    sortedresult = sorted([{'match': match, 'score':mergedresult[match]}
                            for match in mergedresult.keys() if mergedresult[match][0] >= args.threshold or 0],
                        key=lambda item: (-item['score'][0], item['match']))

    if args.number:
        sortedresult = sortedresult[0:args.number]

    for result in sortedresult:
        for idx in range(len(fields)):
            result[fields[idx]] = result['match'][idx]
        for idx in range(len(args.score)):
            result[args.score[idx]] = result['score'][idx]

    outunicodecsv=unicodecsv.DictWriter(outfile, fieldnames=fields + args.score,
                                        extrasaction='ignore', lineterminator=os.linesep)
    if not args.no_header:
        outunicodecsv.writeheader()
    if len(sortedresult) > 0:
        outunicodecsv.writerows(sortedresult)
    outfile.close()

if __name__ == '__main__':
    csvCollect(None)
