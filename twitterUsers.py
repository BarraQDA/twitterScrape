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
from TwitterFeed import TwitterRead
import unicodecsv
import re
from dateutil import parser as dateparser
import datetime

MENTIONREGEXP=re.compile(r'(@\w+)', re.UNICODE)
HASHTAGREGEXP=re.compile(r'(#\w+)', re.UNICODE)

def twitterUsers(arglist):

    parser = argparse.ArgumentParser(description='Retrieve twitter users from ID.',
                                     fromfile_prefix_chars='@')

    parser.add_argument('-v', '--verbosity', type=int, default=1)

    parser.add_argument(      '--since',      type=str, help='Lower bound tweet date/time in any sensible format.')
    parser.add_argument(      '--until',      type=str, help='Upper bound tweet date/time in any sensible format.')
    parser.add_argument('-l', '--limit',      type=int, help='Limit number of tweets to process')

    parser.add_argument('-o', '--outfile', type=str, help='Output CSV user file, otherwise use stdout')
    parser.add_argument('-n', '--number',     type=int, default=0, help='Maximum number of results to output')
    parser.add_argument('--no-comments',   action='store_true', help='Do not output descriptive comments')
    parser.add_argument('--no-header',     action='store_true', help='Do not output CSV header with column names')

    parser.add_argument('infile', type=str, nargs='?', help='Input CSV file, otherwise use stdin')

    args = parser.parse_args(arglist)

    until = dateparser.parse(args.until) if args.until else None
    since = dateparser.parse(args.since) if args.since else None

    twitterread = TwitterRead(args.infile, since=since, until=until, limit=args.limit)

    if args.outfile is None:
        outfile = sys.stdout
    else:
        if os.path.exists(args.outfile):
            shutil.move(args.outfile, args.outfile + '.bak')

        outfile = file(args.outfile, 'w')

    if args.no_comments:
        comments = None
    else:
        if args.outfile:
            comments = (' ' + args.outfile + ' ').center(80, '#') + '\n'
        else:
            comments = '#' * 80 + '\n'

        comments += '# twitterUsers\n'
        comments += '#     outfile=' + (args.outfile or '<stdout>') + '\n'
        comments += '#     infile=' + (args.infile or '<stdin>') + '\n'
        if args.since:
            comments += '#     since=' + args.since+ '\n'
        if args.until:
            comments += '#     until=' + args.until + '\n'
        if args.limit:
            comments += '#     limit=' + str(args.limit) + '\n'
        if args.number:
            comments += '#     number=' + str(args.number) + '\n'
        if args.no_header:
            comments += '#     no-header\n'

        comments += twitterread.comments
        outfile.write(comments)

    outcsv=unicodecsv.writer(outfile, lineterminator=os.linesep)
    outcsv.writerow(['screen_name'])

    if args.verbosity >= 2:
        print("Loading tweets.", file=sys.stderr)

    users = {}
    while True:
        try:
            row = next(twitterread)
            users[row['user'].lower()] = row['user']
            if row.get('reply-to-user', '') or '' != '':
                users[row['reply-to-user'].lower()] = row['reply-to-user']
            for mention in row.get('mentions', []).split():
                users[mention.lower()] = mention
        except StopIteration:
            break

    if args.verbosity >= 2:
        print("Loaded ", twitterread.count, " tweets, ", len(users), " users. ", file=sys.stderr)

    userkeys = users.keys()
    if args.number:
        userkeys = userkeys[0:args.number]

    for user in sorted(userkeys):
        outcsv.writerow([users[user]])

    outfile.close()

if __name__ == '__main__':
    twitterUsers(None)
