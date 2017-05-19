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
from requests_oauthlib import OAuth1Session
import twitter
import sys
from TwitterFeed import TwitterRead, TwitterWrite
import unicodecsv
import re

def twitterEmbed(arglist):

    parser = argparse.ArgumentParser(description='Retrieve tweet HTML.',
                                     fromfile_prefix_chars='@')

    parser.add_argument('-v', '--verbosity', type=int, default=1)

    parser.add_argument(      '--since',      type=str, help='Lower bound tweet date/time in any sensible format.')
    parser.add_argument(      '--until',      type=str, help='Upper bound tweet date/time in any sensible format.')
    parser.add_argument('-l', '--limit',      type=int, help='Limit number of tweets to process')

    parser.add_argument('-o', '--outfile', type=str, help='Output CSV file, otherwise use stdout')
    parser.add_argument('--overwrite',     action='store_true', help='Overwrite input fields with hydrated data')
    parser.add_argument('--no-comments',   action='store_true', help='Do not output descriptive comments')
    parser.add_argument('--no-header',     action='store_true', help='Do not output CSV header with column names')

    parser.add_argument('infile', type=str, nargs='?', help='Input CSV file, otherwise use stdin')

    args = parser.parse_args(arglist)

    until = dateparser.parse(args.until) if args.until else None
    since = dateparser.parse(args.since) if args.since else None

    twitterread = TwitterRead(args.infile, since=since, until=until, limit=args.limit)
    if args.no_comments:
        comments = None
    else:
        if args.outfile:
            comments = (' ' + args.outfile + ' ').center(80, '#') + '\n'
        else:
            comments = '#' * 80 + '\n'

        comments += '# twitterEmbed\n'
        if args.outfile:
            comments += '#     outfile=' + args.outfile + '\n'
        if args.infile:
            comments += '#     infile=' + args.infile + '\n'
        if args.limit:
            comments += '#     limit=' + str(args.limit) + '\n'
        if args.no_header:
            comments += '#     no-header\n'

        comments += twitterread.comments

    api = twitter.Api()

    fieldnames = twitterread.fieldnames
    if 'html' not in fieldnames:
        fieldnames.append('html')

    twitterwrite = TwitterWrite(args.outfile, comments=comments, fieldnames=fieldnames, header=not args.no_header)

    for row in twitterread:
        try:
            row['html'] = api.GetStatusOembed(row['id'])['html']
        except twitter.TwitterError as err:
            if args.verbosity >= 1:
                print("Failed to retrieve HTML for tweet id: " + str(row['id']))
                print(err)

            continue

        twitterwrite.write(row)

if __name__ == '__main__':
    twitterEmbed(None)
