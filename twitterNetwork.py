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
import os
import shutil
import string
import unicodedata
from dateutil import parser as dateparser
import pymp

def cleanKey(key):
    return re.sub('[^0-9a-zA-Z_]', '_', key)

def cleanDictKeys(d):
    return {cleanKey(key): value for key, value in d.iteritems()}

def twitterNetwork(arglist):
    parser = argparse.ArgumentParser(description='Twitter network matrix computation.',
                                     fromfile_prefix_chars='@')

    parser.add_argument('-v', '--verbosity', type=int, default=1)
    parser.add_argument('-j', '--jobs',      type=int, help='Number of parallel tasks, default is number of CPUs')
    parser.add_argument('-b', '--batch',     type=int, default=100000, help='Number of tweets to process per batch, or zero for unlimited. May affect performance but not matrices.')

    parser.add_argument('-p', '--prelude',   type=str, nargs="*", help='Python code to execute before processing')
    parser.add_argument('-f', '--filter',    type=str, help='Python expression evaluated to determine whether tweet is included')
    parser.add_argument(      '--since',     type=str, help='Lower bound tweet date/time in any sensible format.')
    parser.add_argument(      '--until',     type=str, help='Upper bound tweet date/time in any sensible format.')
    parser.add_argument('-l', '--limit',     type=int, help='Limit number of tweets to process')

    parser.add_argument(      '--fromlist',  type=str, required=True, help='Python code evaluated to generate "from" code(s), for example "user"')
    parser.add_argument(      '--tolist',    type=str, required=True, help='Python code evaluated to generate "to" code(s), for example "mentions.split()')
    parser.add_argument('-s', '--score',     type=str, default='1', help='Python expression to evaluate tweet score(s), for example "1 + retweets + favorites"')
    parser.add_argument('-tt', '--tothreshold', type=float, help='Threshold score for "from" vector to be included')
    parser.add_argument('-ft', '--fromthreshold', type=float, help='Threshold score for "from" vector to be included')
    parser.add_argument('-t',  '--threshold', type=float, help='Threshold score for pair to be included')

    parser.add_argument('-o', '--outfile',    type=str, help='Output CSV file, otherwise use stdout.')
    parser.add_argument('--no-comments',      action='store_true', help='Do not output descriptive comments')
    parser.add_argument('--no-header',        action='store_true', help='Do not output CSV header with column names')

    parser.add_argument('infile', type=str, nargs='?', help='Input CSV file, otherwise use stdin.')

    args = parser.parse_args(arglist)
    hiddenargs = ['verbosity', 'jobs', 'batch', 'no_comments']

    if args.jobs is None:
        import multiprocessing
        args.jobs = multiprocessing.cpu_count()

    if args.verbosity >= 1:
        print("Using " + str(args.jobs) + " jobs.", file=sys.stderr)

    if args.batch == 0:
        args.batch = sys.maxint

    if args.prelude:
        if args.verbosity >= 1:
            print("Executing prelude code.", file=sys.stderr)

        for line in args.prelude:
            exec(line) in globals()

    until = dateparser.parse(args.until) if args.until else None
    since = dateparser.parse(args.since) if args.since else None

    if args.outfile is None:
        outfile = sys.stdout
    else:
        if os.path.exists(args.outfile):
            shutil.move(args.outfile, args.outfile + '.bak')

        outfile = file(args.outfile, 'w')

    twitterread  = TwitterRead(args.infile, since=since, until=until, limit=args.limit)
    if not args.no_comments:
        comments = ((' ' + args.outfile + ' ') if args.outfile else '').center(80, '#') + '\n'
        comments += '# ' + os.path.basename(sys.argv[0]) + '\n'
        print (args.__dict__)
        arglist = args.__dict__.keys()
        for arg in arglist:
            if arg not in hiddenargs:
                val = getattr(args, arg)
                if type(val) == int:
                    comments += '#     --' + arg + '=' + str(val) + '\n'
                elif type(val) == str:
                    comments += '#     --' + arg + '="' + val + '"\n'
                elif type(val) == bool and val:
                    comments += '#     --' + arg + '\n'
                elif type(val) == list:
                    for valitem in val:
                        if type(valitem) == int:
                            comments += '#     --' + arg + '=' + str(valitem) + '\n'
                        elif type(valitem) == str:
                            comments += '#     --' + arg + '="' + valitem + '"\n'

        outfile.write(comments+twitterread.comments)

    if args.filter:
        exec "\
def evalfilter(" + ','.join([cleanKey(fieldname) for fieldname in twitterread.fieldnames]) + ",**kwargs):\n\
    return [" + ','.join([filteritem for filteritem in args.filter]) + "]"

    exec "\
def evalscore(" + ','.join([cleanKey(fieldname) for fieldname in twitterread.fieldnames]) + ",**kwargs):\n\
    return " + args.score

    exec "\
def evalfrom(" + ','.join([cleanKey(fieldname) for fieldname in twitterread.fieldnames]) + ",**kwargs):\n\
    return " + args.fromlist

    exec "\
def evalto(" + ','.join([cleanKey(fieldname) for fieldname in twitterread.fieldnames]) + ",**kwargs):\n\
    return " + args.tolist

    if args.verbosity >= 1:
        print("Loading twitter data.", file=sys.stderr)

    mergededge = {}
    mergedfromtotal = {}
    mergedtototal = {}
    while True:
        if args.verbosity >= 2:
            print("Loading twitter batch.", file=sys.stderr)

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
            print("Processing twitter batch.", file=sys.stderr)

        rowcount = len(rows)
        edges = pymp.shared.list()
        fromtotals = pymp.shared.list()
        tototals = pymp.shared.list()
        with pymp.Parallel(args.jobs) as p:
            edge = {}
            fromtotal = {}
            tototal = {}
            for rowindex in p.range(0, rowcount):
                row = rows[rowindex]
                rowargs = cleanDictKeys(row)
                if args.filter and not evalfilter(**rowargs):
                    continue

                rowfrom  = evalfrom(**rowargs)
                rowto    = evalto(**rowargs)
                rowscore = evalscore(**rowargs)

                if args.verbosity >= 3:
                    print ("From: " + str(rowfrom), file=sys.stderr)
                    print ("To: " + str(rowto), file=sys.stderr)

                for fromitem in rowfrom:
                    for toitem in rowto:
                        duple = (fromitem, toitem)
                        edge[duple] = edge.get(duple, 0) + rowscore
                        fromtotal[fromitem] = fromtotal.get(fromitem, 0) + rowscore
                        tototal[toitem] = tototal.get(toitem, 0) + rowscore

            with p.lock:
                edges.append(edge)
                fromtotals.append(fromtotal)
                tototals.append(tototal)

        for edge in edges:
            for duple in edge.keys():
                mergededge[duple] = mergededge.get(duple, 0) + edge[duple]

        for fromtotal in fromtotals:
            for fromitem in fromtotal.keys():
                mergedfromtotal[fromitem] = mergedfromtotal.get(fromitem, 0) + fromtotal[fromitem]

        for tototal in tototals:
            for toitem in tototal.keys():
                mergedtototal[toitem] = mergedtototal.get(toitem, 0) + tototal[toitem]

    if args.verbosity >= 1:
        print("Saving network matrix.", file=sys.stderr)

    outunicodecsv=unicodecsv.writer(outfile, lineterminator=os.linesep)
    if not args.no_header:
        outunicodecsv.writerow(['from', 'to', 'score'])
    for duple, value in sorted(mergededge.iteritems(), key=lambda (k,v): (-v,k)):
        fromitem = duple[0]
        if mergedfromtotal[fromitem] < (args.fromthreshold or 0):
            continue
        toitem = duple[1]
        if mergedtototal[toitem] < (args.tothreshold or 0):
            continue

        if mergededge[duple] < (args.threshold or 0):
            continue

        outunicodecsv.writerow([fromitem, toitem, value])

    outfile.close()

if __name__ == '__main__':
    twitterNetwork(None)
