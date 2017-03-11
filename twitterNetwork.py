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
import string
import unicodedata
from dateutil import parser as dateparser
import pymp
from igraph import *
import numpy as np

def twitterNetwork(arglist):
    parser = argparse.ArgumentParser(description='Twitter network matrix computation.')

    parser.add_argument('-v', '--verbosity', type=int, default=1)
    parser.add_argument('-j', '--jobs',      type=int, help='Number of parallel tasks, default is number of CPUs')
    parser.add_argument('-b', '--batch',     type=int, default=100000, help='Number of tweets to process per batch, or zero for unlimited. May affect performance but not matrices.')

    parser.add_argument('-p', '--prelude',   type=str, nargs="*", help='Python code to execute before processing')
    parser.add_argument('-f', '--filter',    type=str, help='Python expression evaluated to determine whether tweet is included')
    parser.add_argument(      '--since',     type=str, help='Lower bound tweet date.')
    parser.add_argument(      '--until',     type=str, help='Upper bound tweet date.')
    parser.add_argument('-l', '--limit',     type=int, help='Limit number of tweets to process')

    parser.add_argument(      '--from',      type=str, required=True, dest='fromcode', help='Python code evaluated to generate "from" code(s), for example "user"')
    parser.add_argument(      '--to',        type=str, required=True, help='Python code evaluated to generate "to" code(s), for example "mentions.split()')
    parser.add_argument('-s', '--score',     type=str, default='1', help='Python expression to evaluate tweet score(s), for example "1 + retweets + favorites"')
    parser.add_argument('-tt', '--tothreshold', type=float, help='Threshold score for "from" matrix to be included')
    parser.add_argument('-ft', '--fromthreshold', type=float, help='Threshold score for "from" matrix to be included')

    parser.add_argument('-o', '--outfile',    type=str, help='Output CSV file, otherwise use stdout.')
    parser.add_argument('--no-comments',    action='store_true', help='Do not output descriptive comments')

    parser.add_argument('infile', type=str, nargs='?', help='Input CSV file, otherwise use stdin.')

    args = parser.parse_args(arglist)

    if args.jobs is None:
        import multiprocessing
        args.jobs = multiprocessing.cpu_count()

    if args.verbosity > 1:
        print("Using " + str(args.jobs) + " jobs.", file=sys.stderr)

    if args.batch == 0:
        args.batch = sys.maxint

    if args.prelude:
        if args.verbosity > 1:
            print("Executing prelude code.", file=sys.stderr)

        for line in args.prelude:
            exec(line) in globals()

    if args.filter:
        filter = compile(args.filter, 'filter argument', 'eval')
        def evalfilter(user, date, retweets, favorites, text, lang, geo, mentions, hashtags, id, permalink, **extra):
            return eval(filter)

    score = compile(args.score, 'score argument', 'eval')
    def evalscore(user, date, retweets, favorites, text, lang, geo, mentions, hashtags, id, permalink, **extra):
        return eval(score)

    fromcode = compile(args.fromcode, 'from argument', 'eval')
    def evalfrom(user, date, retweets, favorites, text, lang, geo, mentions, hashtags, id, permalink, **extra):
        return eval(fromcode)

    to = compile(args.to, 'to argument', 'eval')
    def evalto(user, date, retweets, favorites, text, lang, geo, mentions, hashtags, id, permalink, **extra):
        return eval(to)

    # Parse since and until dates
    if args.until:
        args.until = dateparser.parse(args.until).date().isoformat()
    if args.since:
        args.since = dateparser.parse(args.since).date().isoformat()

    if args.outfile is None:
        outfile = sys.stdout
    else:
        outfile = file(args.outfile, 'w')

    twitterread  = TwitterRead(args.infile, since=args.since, until=args.until, limit=args.limit)
    if not args.no_comments:
        comments = ''
        if args.outfile:
            comments += (' ' + args.outfile + ' ').center(80, '#') + '\n'
        else:
            comments += '#' * 80 + '\n'

        comments += '# twitterNetwork\n'
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
        comments += '#     from=' + args.fromcode + '\n'
        comments += '#     to=' + args.to + '\n'
        comments += '#     score=' + args.score + '\n'
        if args.fromthreshold:
            comments += '#     fromthreshold=' + str(args.fromthreshold) + '\n'
        if args.tothreshold:
            comments += '#     tothreshold=' + str(args.tothreshold) + '\n'

        comments += twitterread.comments

        outfile.write(comments)

    if args.verbosity > 1:
        print("Loading twitter data.", file=sys.stderr)

    mergededge = {}
    mergedfromtotal = {}
    mergedtototal = {}
    while True:
        if args.verbosity > 2:
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

        if args.verbosity > 2:
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
                if args.filter and not evalfilter(**row):
                    continue

                rowfrom  = evalfrom(**row)
                rowto    = evalto(**row)
                rowscore = evalscore(**row)

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

    if args.verbosity > 1:
        print("Saving network matrix.", file=sys.stderr)

    outunicodecsv=unicodecsv.writer(outfile)
    outunicodecsv.writerow(['from', 'to', 'score'])
    for duple in mergededge.keys():
        fromitem = duple[0]
        if mergedfromtotal[fromitem] < (args.fromthreshold or sys.maxint):
            continue
        toitem = duple[1]
        if mergedtototal[toitem] < (args.tothreshold or sys.maxint):
            continue

        outunicodecsv.writerow([fromitem, toitem, mergededge[duple]])

    outfile.close()

if __name__ == '__main__':
    twitterNetwork(None)