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
import webbrowser
import twitter
import sys
from TwitterFeed import TwitterRead, TwitterWrite
import unicodecsv
import re
from dateutil import parser as dateparser
import datetime

MENTIONREGEXP=re.compile(r'(@\w+)', re.UNICODE)
HASHTAGREGEXP=re.compile(r'(#\w+)', re.UNICODE)

def twitterHydrate(arglist):

    parser = argparse.ArgumentParser(description='Hydrate twitter ids.',
                                     fromfile_prefix_chars='@')

    parser.add_argument('-v', '--verbosity', type=int, default=1)
    parser.add_argument('-b', '--batch',     type=int, default=10000, help='Number of tweets to process per batch. Use to limit memory usage with very large files. May affect performance but not results.')

    # Twitter authentication stuff
    parser.add_argument('--consumer-key', type=str,
                        help='Consumer key for Twitter authentication')
    parser.add_argument('--consumer-secret', type=str,
                        help='Consumer secret for Twitter authentication')

    parser.add_argument('-a', '--application-only-auth', action='store_true')
    parser.add_argument('--access-token-key', type=str,
                        help='Access token key for Twitter authentication')
    parser.add_argument('--access-token-secret', type=str,
                        help='Access token secret for Twitter authentication')

    parser.add_argument('-l', '--limit',      type=int, help='Limit number of tweets to process')

    parser.add_argument(      '--html',    action='store_true', help='Retrieve embeddable tweet HTML')

    parser.add_argument('-o', '--outfile', type=str, help='Output CSV file, otherwise use stdout')
    parser.add_argument('--overwrite',     action='store_true', help='Overwrite input fields with hydrated data')
    parser.add_argument('--no-comments',   action='store_true', help='Do not output descriptive comments')
    parser.add_argument('--no-header',     action='store_true', help='Do not output CSV header with column names')

    parser.add_argument('infile', type=str, nargs='?', help='Input CSV file, otherwise use stdin')

    args = parser.parse_args(arglist)

    if args.batch is None:
        args.batch = sys.maxint

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

    # Twitter URLs
    REQUEST_TOKEN_URL = 'https://api.twitter.com/oauth/request_token'
    ACCESS_TOKEN_URL = 'https://api.twitter.com/oauth/access_token'
    AUTHORIZATION_URL = 'https://api.twitter.com/oauth/authorize'
    SIGNIN_URL = 'https://api.twitter.com/oauth/authenticate'

    if not all([args.consumer_key, args.consumer_secret]):
        print ("""
    To access Twitter's API, you need a consumer key and secret for a registered
    Twitter application. You can register an application or retrieve the consumer key
    and secret for an already registerd application at https://dev.twitter.com/apps
    """)
        sys.exit()

    if args.application_only_auth:
        api = twitter.Api(
                    consumer_key=args.consumer_key,
                    consumer_secret=args.consumer_secret,
                    application_only_auth=True,
                    sleep_on_rate_limit=True
            )
    else:
        if not all([args.access_token_key, args.access_token_secret]):
            oauth_client = OAuth1Session(args.consumer_key, client_secret=args.consumer_secret, callback_uri='oob')

            resp = oauth_client.fetch_request_token(REQUEST_TOKEN_URL)
            url = oauth_client.authorization_url(AUTHORIZATION_URL)

            print('Opening browser for Twitter authentication: ' + url, file=sys.stderr)

            webbrowser.open(url)
            print('Enter your pincode? ', file=sys.stderr)
            pincode = raw_input()

            oauth_client = OAuth1Session(args.consumer_key, client_secret=args.consumer_secret,
                                        resource_owner_key=resp.get('oauth_token'),
                                        resource_owner_secret=resp.get('oauth_token_secret'),
                                        verifier=pincode)
            resp = oauth_client.fetch_access_token(ACCESS_TOKEN_URL)
            args.access_token_key = resp.get('oauth_token')
            args.access_token_secret = resp.get('oauth_token_secret')

            print('To re-use access token next time use the following arguments:', file=sys.stderr)
            print('    --access-token-key ' + args.access_token_key + ' --access-token-secret ' + args.access_token_secret, file=sys.stderr)

        api = twitter.Api(
                    consumer_key=args.consumer_key,
                    consumer_secret=args.consumer_secret,
                    access_token_key=args.access_token_key,
                    access_token_secret=args.access_token_secret,
                    sleep_on_rate_limit=True
            )

    fieldnames = twitterread.fieldnames + list(({'user', 'date', 'text', 'replies', 'retweets', 'favorites', 'reply-to', 'reply-to-user', 'reply-to-user-id', 'lang', 'geo', 'mentions', 'hashtags', 'user-id', 'id'} | ({'html'} if args.html else set())) - set(twitterread.fieldnames))

    twitterwrite = TwitterWrite(args.outfile, comments=comments, fieldnames=fieldnames, header=not args.no_header)

    while True:
        if args.verbosity >= 2:
            print("Loading twitter batch.", file=sys.stderr)

        rows = []
        batchcount = 0
        while batchcount < args.batch:
            try:
                row = next(twitterread)
                batchcount += 1
                rows.append(row)

            except StopIteration:
                break

        if batchcount == 0:
            break

        ids = [row['id'] for row in rows]
        tweets = api.GetStatuses(ids, map=True)
        for row in rows:
            tweet = tweets[row['id']]
            if tweet:
                assert (tweet.id == row['id'])

                if args.overwrite or row.get('user') is None:
                    row['user'] = tweet.user.screen_name
                if args.overwrite or row.get('date') is None:
                    row['date'] = datetime.datetime.utcfromtimestamp(tweet.created_at_in_seconds).isoformat()
                if args.overwrite or row.get('text') is None:
                    row['text'] = tweet.text
                if args.overwrite or row.get('reply-to') is None:
                    row['reply-to'] = tweet.in_reply_to_status_id
                if args.overwrite or row.get('reply-to-user') is None:
                    row['reply-to-user'] = tweet.in_reply_to_screen_name
                if args.overwrite or row.get('reply-to-user-id') is None:
                    row['reply-to-user-id'] = tweet.in_reply_to_user_id
                if args.overwrite or row.get('retweets') is None:
                    row['retweets'] = tweet.retweet_count
                if args.overwrite or row.get('favorites') is None:
                    row['favorites'] = tweet.favorite_count
                if args.overwrite or row.get('lang') is None:
                    row['lang'] = tweet.lang
                if args.overwrite or row.get('geo') is None:
                    row['geo'] = tweet.geo
                if args.overwrite or row.get('mentions') is None:
                    row['mentions'] = u' '.join([u'@'+user.screen_name for user in tweet.user_mentions])
                if args.overwrite or row.get('hashtags') is None:
                    row['hashtags'] = u' '.join([u'#'+hashtag.text for hashtag in tweet.hashtags])
                if args.overwrite or row.get('user-id') is None:
                    row['user-id'] = tweet.user.id

                if args.html:
                    row['html'] = api.GetStatusOembed(tweet.id)
            elif args.verbosity >= 3:
                print("Tweet id: " + str(row['id']) + " not retrieved.", file=sys.stderr)

            twitterwrite.write(row)

if __name__ == '__main__':
    twitterHydrate(None)
