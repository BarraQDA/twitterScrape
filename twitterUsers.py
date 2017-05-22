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
from TwitterFeed import TwitterRead
import unicodecsv
import re
from dateutil import parser as dateparser
import datetime

def cleanKey(key):
    return re.sub('[^0-9a-zA-Z_]', '_', key)

def cleanDictKeys(d):
    return {cleanKey(key): value for key, value in d.iteritems()}

def twitterUsers(arglist):

    parser = argparse.ArgumentParser(description='Retrieve twitter users from ID.',
                                     fromfile_prefix_chars='@')

    parser.add_argument('-v', '--verbosity', type=int, default=1)

    # Twitter authentication stuff - not used but include so replay works
    parser.add_argument('--consumer-key',    type=str, help=argparse.SUPPRESS)
    parser.add_argument('--consumer-secret', type=str, help=argparse.SUPPRESS)

    parser.add_argument('--application-only-auth', action='store_true', help=argparse.SUPPRESS)
    parser.add_argument('--access-token-key',    type=str, help=argparse.SUPPRESS)
    parser.add_argument('--access-token-secret', type=str, help=argparse.SUPPRESS)

    parser.add_argument('-p', '--prelude',    type=str, nargs="*", help='Python code to execute before processing')
    parser.add_argument('-f', '--filter',     type=str, help='Python expression evaluated to determine whether tweet is included')
    parser.add_argument(      '--since',      type=str, help='Lower bound tweet date/time in any sensible format.')
    parser.add_argument(      '--until',      type=str, help='Upper bound tweet date/time in any sensible format.')
    parser.add_argument('-l', '--limit',      type=int, help='Limit number of tweets to process')

    parser.add_argument('-o', '--outfile', type=str, help='Output CSV user file, otherwise use stdout')
    parser.add_argument('-n', '--number',     type=int, help='Maximum number of results to output')
    parser.add_argument('--no-comments',   action='store_true', help='Do not output descriptive comments')
    parser.add_argument('--no-header',     action='store_true', help='Do not output CSV header with column names')

    parser.add_argument('infile', type=str, nargs='?', help='Input CSV file, otherwise use stdin')

    args = parser.parse_args(arglist)
    hiddenargs = ['verbosity', 'consumer_key', 'consumer_secret', 'application_only_auth', 'access_token_key', 'access_token_secret', 'no_comments']

    if args.prelude:
        if args.verbosity >= 1:
            print("Executing prelude code.", file=sys.stderr)

        for line in args.prelude:
            exec(line) in globals()

    until = dateparser.parse(args.until) if args.until else None
    since = dateparser.parse(args.since) if args.since else None

    twitterread = TwitterRead(args.infile, since=since, until=until, limit=args.limit)

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

        outfile.write(comments + twitterread.comments)

    if args.filter:
            exec "\
def evalfilter(" + ','.join([cleanKey(fieldname) for fieldname in twitterread.fieldnames]) + ",**kwargs):\n\
    return " + args.filter

    if args.verbosity >= 1:
        print("Loading tweets.", file=sys.stderr)

    users = {}
    while True:
        try:
            while True:
                row = next(twitterread)
                if args.filter:
                    rowargs = cleanDictKeys(row)
                    if evalfilter(**rowargs):
                        break
                else:
                    break

            users[row['user'].lower()] = row['user']
            if row.get('reply-to-user', '') or '' != '':
                users[row['reply-to-user'].lower()] = row['reply-to-user']
            for mention in (row.get('mentions', '') or '').split():
                users[mention.lower()] = mention
        except StopIteration:
            break

    if args.verbosity >= 2:
        print("Loaded ", twitterread.count, " tweets, ", len(users), " users. ", file=sys.stderr)

    userkeys = users.keys()
    if args.number:
        userkeys = userkeys[0:args.number]

    outcsv=unicodecsv.writer(outfile, lineterminator=os.linesep)
    outcsv.writerow(['screen_name'])
    for user in sorted(userkeys):
        outcsv.writerow([users[user]])

    outfile.close()

if __name__ == '__main__':
    twitterUsers(None)
