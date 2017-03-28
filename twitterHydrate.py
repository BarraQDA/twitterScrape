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
from TwitterFeed import TwitterRead, TwitterWrite
import unicodecsv
import rest
import re
from dateutil import parser as dateparser
import datetime
import logging

logging.basicConfig(level=logging.INFO)

MENTIONREGEXP=re.compile(r'(@\w+)', re.UNICODE)
HASHTAGREGEXP=re.compile(r'(#\w+)', re.UNICODE)

def twitterHydrate(arglist):

    parser = argparse.ArgumentParser(description='Hydrate twitter ids.',
                                     fromfile_prefix_chars='@')

    parser.add_argument('-v', '--verbosity', type=int, default=1)
    parser.add_argument('-t', '--threshold', type=int, default=60, help='Number of seconds out of sequence to report.')

    parser.add_argument('-l', '--limit',      type=int, help='Limit number of tweets to process')

    parser.add_argument('-o', '--outfile', type=str, help='Output CSV file, otherwise use stdout')
    parser.add_argument('--no-comments',   action='store_true', help='Do not output descriptive comments')

    parser.add_argument('infile', type=str, nargs='?', help='Input CSV file, otherwise use stdin')

    args = parser.parse_args()

    twitterread = TwitterRead(args.infile, limit=args.limit)
    if args.no_comments:
        comments = None
    else:
        if args.outfile:
            comments = (' ' + args.outfile + ' ').center(80, '#') + '\n'
        else:
            comments = '#' * 80 + '\n'

        comments += '# twitterHydrate\n'
        comments += '#     outfile=' + (args.outfile or '<stdout>') + '\n'
        comments += '#     infile=' + (args.infile or '<stdin>') + '\n'
        if args.limit:
            comments += '#     limit=' + str(args.limit) + '\n'

        comments += twitterread.comments

    twitterwrite = TwitterWrite(args.outfile, comments=comments, fieldnames = ['user', 'date', 'text', 'replies', 'retweets', 'favorites', 'reply-to', 'reply-to-user', 'reply-to-user-id', 'lang', 'geo', 'mentions', 'hashtags', 'user-id', 'id'])

    ids_to_fetch = sorted([row['id'] for row in twitterread], reverse=True)

    for page in rest.fetch_tweet_list(ids_to_fetch):
        page = sorted(page, reverse=True, key=lambda item: item['id'])
        for tweet in page:
            tweet['user-id']           = tweet['user']['id']
            tweet['user']              = tweet['user']['screen_name']
            tweet['date']              = dateparser.parse(tweet['created_at']).replace(tzinfo=None).isoformat()
            tweet['retweets']          = tweet['retweet_count']
            tweet['favorites']         = tweet['favorite_count']
            tweet['reply-to']          = tweet['in_reply_to_status_id']
            tweet['reply-to-user']     = tweet['in_reply_to_screen_name']
            tweet['reply-to-user-id']  = tweet['in_reply_to_user_id']
            tweet['mentions']  = " ".join(MENTIONREGEXP.findall(tweet['text']))
            tweet['hashtags']  = " ".join(HASHTAGREGEXP.findall(tweet['text']))

            twitterwrite.write(tweet)

if __name__ == '__main__':
    twitterHydrate(None)
