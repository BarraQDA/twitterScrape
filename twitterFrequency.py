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
from TwitterFeed import TwitterRead
import unicodecsv
import string
import unicodedata
import pymp
import re
import datetime
from dateutil import parser as dateparser
from pytimeparse.timeparse import timeparse
import calendar

def twitterFrequency(arglist):
    parser = argparse.ArgumentParser(description='Twitter feed frequency matrix producer.',
                                     fromfile_prefix_chars='@')

    parser.add_argument('-v', '--verbosity',  type=int, default=1)

    parser.add_argument('-p', '--prelude',    type=str, nargs="*", help='Python code to execute before processing')
    parser.add_argument('-f', '--filter',     type=str, nargs='+', required=True, help='Python expression evaluated to determine whether tweet is included')
    parser.add_argument('-t', '--title',      type=str, nargs='*', help='Title of column corresponding to filter')
    parser.add_argument(      '--since',      type=str, help='Lower bound tweet date/time in any sensible format.')
    parser.add_argument(      '--until',      type=str, help='Upper bound tweet date/time in any sensible format.')
    parser.add_argument('-l', '--limit',      type=int, help='Limit number of tweets to process')

    parser.add_argument('-s', '--score',      type=str, default='1', help='Python expression to evaluate tweet score, for example "1 + retweets + favorites"')

    parser.add_argument(      '--interval',   type=str, default='1 day', help='Interval for measuring frequency, for example "1 day".')

    parser.add_argument('-o', '--outfile',    type=str, help='Output CSV file, otherwise use stdout.')
    parser.add_argument('--no-comments',      action='store_true', help='Do not output descriptive comments')
    parser.add_argument('--no-header',        action='store_true', help='Do not output CSV header with column names')

    parser.add_argument('infile', type=str, nargs='?', help='Input CSV file, otherwise use stdin.')
    hiddenargs = ['verbosity', 'no_comments']

    args = parser.parse_args(arglist)

    if args.prelude:
        if args.verbosity >= 1:
            print("Executing prelude code.", file=sys.stderr)

        for line in args.prelude:
            exec(line) in globals()

    until = dateparser.parse(args.until) if args.until else None
    since = dateparser.parse(args.since) if args.since else None

    interval = timeparse(args.interval)
    if interval is None:
        raise RuntimeError("Interval: " + args.interval + " not recognised.")

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

    exec "\
def evalfilter(" + ','.join(twitterread.fieldnames).replace('-','_') + ", **kwargs):\n\
    return [" + ','.join([filteritem for filteritem in args.filter]) + "]"

    exec "\
def evalscore(" + ','.join(twitterread.fieldnames).replace('-','_') + ", **kwargs):\n\
    return " + args.score

    outunicodecsv=unicodecsv.writer(outfile, lineterminator=os.linesep)
    if not args.no_header:
        outunicodecsv.writerow(['date'] + (args.title or args.filter))

    if args.verbosity >= 1:
        print("Loading twitter data.", file=sys.stderr)

    rows=[]
    runningscore = [0] * len(args.filter)
    while True:
        try:
            row = next(twitterread)
        except StopIteration:
            break

        rowargs = {key.replace('-','_'): value for key, value in row.iteritems()}
        row['score']     = evalscore(**rowargs)
        row['datesecs']  = calendar.timegm(row['date'].timetuple())

        firstrow = rows[0] if len(rows) > 0 else None
        while firstrow and firstrow['datesecs'] - row['datesecs'] > interval:
            filters = firstrow['filters']
            for filteridx in range(len(args.filter)):
                if filters[filteridx]:
                    runningscore[filteridx] -= firstrow['score']

            if any(filters):
                outunicodecsv.writerow([datetime.datetime.utcfromtimestamp(firstrow['datesecs'] - interval)] + runningscore)

            del rows[0]
            firstrow = rows[0] if len(rows) else None

        filters = evalfilter(**rowargs)
        for filteridx in range(len(args.filter)):
            if filters[filteridx]:
                runningscore[filteridx] += row['score']

        if args.limit and twitterread.count == args.limit:
            break

        if not any(filters):
            continue

        row['filters'] = filters[:]
        rows.append(row)

        outunicodecsv.writerow([row['date']] + runningscore)


    outfile.close()


if __name__ == '__main__':
    twitterFrequency(None)
