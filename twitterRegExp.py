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
import calendar
from pytimeparse.timeparse import timeparse
from operator import sub, add

def twitterRegExp(arglist):
    presets = {
        'hashtags':{ 'column':'hashtags', 'regexp':r'(?P<hashtag>#\w+)',      'ignorecase':True },
        'mentions':{ 'column':'mentions', 'regexp':r'(?P<mention>@\w+)',      'ignorecase':True },
        'links':   { 'column':'text',     'regexp':r'(?P<link>https?://\S+)', 'ignorecase':True }
    }

    parser = argparse.ArgumentParser(description='Twitter CSV file regular expression extraction.')

    parser.add_argument('-v', '--verbosity',  type=int, default=1)
    parser.add_argument('-j', '--jobs',       type=int, help='Number of parallel tasks, default is number of CPUs. May affect performance but not results.')
    parser.add_argument('-b', '--batch',      type=int, default=100000, help='Number of tweets to process per batch. Use to limit memory usage with very large files. May affect performance but not results.')

    parser.add_argument(      '--preset',     choices=presets)

    parser.add_argument('-p', '--prelude',    type=str, nargs="*", help='Python code to execute before processing')
    parser.add_argument('-f', '--filter',     type=str, help='Python expression evaluated to determine whether tweet is included')
    parser.add_argument(      '--since',      type=str, help='Lower bound tweet date.')
    parser.add_argument(      '--until',      type=str, help='Upper bound tweet date.')
    parser.add_argument('-l', '--limit',      type=int, help='Limit number of tweets to process')

    parser.add_argument('-c', '--column',     type=str, help='Column to apply regular expression, default is "text"')
    parser.add_argument('-r', '--regexp',     type=str, help='Regular expression applied to tweet text to create output columns.')
    parser.add_argument('-i', '--ignorecase', action='store_true', help='Ignore case in regular expression')
    parser.add_argument('-s', '--score',      type=str, nargs="*", default=['1'], help='Python expression(s) to evaluate tweet score(s), for example "1 + retweets + favorites"')
    parser.add_argument('-t', '--threshold',  type=float, help='Threshold (first) score for result to be output')

    parser.add_argument('-in', '--interval',  type=str, help='Interval for measuring frequency, for example "1 day".')

    parser.add_argument('-o', '--outfile',    type=str, help='Output CSV file, otherwise use stdout.')
    parser.add_argument('-n', '--number',     type=int, default=0, help='Maximum number of results to output')
    parser.add_argument('--no-comments',    action='store_true', help='Do not output descriptive comments')

    parser.add_argument('infile', type=str, nargs='?', help='Input CSV file, otherwise use stdin.')

    args = parser.parse_args(arglist)

    if args.preset:
        values = presets[args.preset]
        for name in values.keys():
            if not getattr(args, name):
                setattr(args, name, values[name])

    # Handle required arguments and defaults manually so that presets can work
    if not args.regexp:
        raise RuntimeError("At least one of 'preset' and 'regexp' must be specified.")
    if not args.column:
        args.column = 'text'

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
            exec(line) in globals()

    if args.filter:
        filter = compile(args.filter, 'filter argument', 'eval')
        def evalfilter(user, date, retweets, favorites, text, lang, geo, mentions, hashtags, id, permalink, **extra):
            return eval(filter)

    if args.until:
        args.until = dateparser.parse(args.until).date().isoformat()
    if args.since:
        args.since = dateparser.parse(args.since).date().isoformat()

    regexp = re.compile(args.regexp, re.UNICODE | (re.IGNORECASE if args.ignorecase else 0))
    fields = list(regexp.groupindex)

    score = []
    for scoreitem in args.score:
        score.append(compile(scoreitem, scoreitem, 'eval'))

    def evalscore(user, date, retweets, favorites, text, lang, geo, mentions, hashtags, id, permalink, **extra):
        return [eval(scoreitem) for scoreitem in score]

    if args.interval:
        interval = timeparse(args.interval)
        if interval is None:
            raise RuntimeError("Interval: " + args.interval + " not recognised.")

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

        comments += '# twitterRegExp\n'
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
        comments += '#     column=' + args.column+ '\n'
        comments += '#     regexp=' + args.regexp + '\n'
        if args.ignorecase:
            comments += '#     ignorecase\n'
        if args.interval:
            comments += '#     interval=' + str(args.interval) + '\n'
        for scoreitem in args.score:
            comments += '#     score=' + scoreitem + '\n'
        if args.threshold:
            comments += '#     threshold=' + str(args.threshold) + '\n'
        if args.number:
            comments += '#     number=' + str(args.number) + '\n'

        outfile.write(comments + twitterread.comments)

    if args.verbosity >= 1:
        print("Loading twitter data.", file=sys.stderr)

    # NB Code for single- and multi-threaded processing is separate
    mergedresult = {}
    if args.jobs == 1:
        rows=[]
        if args.interval:
            runningresult = {}
        while True:
            try:
                while True:
                    row = next(twitterread)
                    if (not args.filter) or evalfilter(**row):
                        break

            except StopIteration:
                break

            if args.interval:
                row['datesecs'] = calendar.timegm(dateparser.parse(row['date']).timetuple())
                firstrow = rows[0] if len(rows) else None
                while firstrow and firstrow['datesecs'] - row['datesecs'] > interval:
                    indexes  = firstrow['indexes']
                    rowscore = firstrow['score']
                    for index in indexes:
                        runningresult[index] = map(sub, runningresult[index], rowscore)

                    del rows[0]
                    firstrow = rows[0] if len(rows) else None

            matches = regexp.finditer(row[args.column])
            rowscore = None
            indexes = []
            for match in matches:
                rowscore = rowscore or evalscore(**row)
                if args.ignorecase:
                    index = tuple(value.lower() for value in match.groupdict().values())
                else:
                    index = tuple(match.groupdict().values())

                if args.interval:
                    indexes.append(index)
                    runningresult[index] = map(add, runningresult.get(index, [0] * len(score)), rowscore)
                    curmergedresult = mergedresult.get(index, [0] * len(score))
                    mergedresult[index] = [max(curmergedresult[idx], runningresult[index][idx]) for idx in range(len(score))]
                else:
                    mergedresult[index] = map(add, mergedresult.get(index, [0] * len(score)), rowscore)

            if args.interval and rowscore:
                row['score']   = rowscore
                row['indexes'] = indexes
                rows.append(row)

            if args.limit and twitterread.count == args.limit:
                break

    else:
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
            results = pymp.shared.list()
            with pymp.Parallel(args.jobs) as p:
                result = {}
                for rowindex in p.range(0, rowcount):
                    row = rows[rowindex]
                    if args.filter and not evalfilter(**row):
                        continue

                    matches = regexp.finditer(row[args.column])
                    rowscore = None
                    for match in matches:
                        rowscore = rowscore or evalscore(**row)
                        if args.ignorecase:
                            index = tuple(value.lower() for value in match.groupdict().values())
                        else:
                            index = tuple(match.groupdict().values())

                        result[index] = map(add, result.get(index, [0] * len(score)), rowscore)

                if args.verbosity >= 2:
                    print("Thread " + str(p.thread_num) + " found " + str(len(result)) + " results.", file=sys.stderr)

                with p.lock:
                    results.append(result)

            for result in results:
                for index in result:
                    mergedresult[index] = map(add, mergedresult.get(index, [0] * len(score)), result[index])

    if args.verbosity >= 1:
        print("Sorting " + str(len(mergedresult)) + " results.", file=sys.stderr)

    # Sort on first score value
    sortedresult = sorted([{'match': match, 'score':mergedresult[match]}
                            for match in mergedresult.keys() if mergedresult[match][0] >= args.threshold or 0],
                        key=lambda item: (-item['score'][0], item['match']))

    if args.number != 0:
        sortedresult = sortedresult[0:args.number]

    for result in sortedresult:
        for idx in range(len(fields)):
            result[fields[idx]] = result['match'][idx]
        for idx in range(len(score)):
            result[args.score[idx]] = result['score'][idx]

    outunicodecsv=unicodecsv.DictWriter(outfile, fieldnames=fields + args.score, extrasaction='ignore')
    outunicodecsv.writeheader()
    if len(sortedresult) > 0:
        outunicodecsv.writerows(sortedresult)
    outfile.close()

if __name__ == '__main__':
    twitterRegExp(None)
