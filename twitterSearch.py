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
import os
import shutil
import unicodecsv
import string
import pytz
import datetime

# Hack away deprecation warning -
import warnings
warnings.simplefilter("ignore", DeprecationWarning)

def twitterSearch(arglist):

    parser = argparse.ArgumentParser(description='Search twitter using REST API.',
                                     fromfile_prefix_chars='@')

    parser.add_argument('-v', '--verbosity', type=int, default=1)

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

    parser.add_argument('-u', '--user',     type=str, help='Twitter username to filter.')
    parser.add_argument('-q', '--query',    type=str, help='Search string for twitter feed. Either USER or QUERY must be defined to open a twitter feed.')
    parser.add_argument('-l', '--language', type=str, help='Language code to filter.')
    parser.add_argument('-g', '--geo',      type=str, help='Geographic code to filter.')

    parser.add_argument(      '--since',    type=str, help='Lower bound search date.')
    parser.add_argument(      '--until',    type=str, help='Upper bound search date.')

    parser.add_argument('-o', '--outfile', type=str, nargs='?',
                        help='Output file name, otherwise use stdout.')
    parser.add_argument('-n', '--number',   type=int, default=0, help='Maximum number of results to output')
    parser.add_argument('--no-comments',    action='store_true', help='Do not output descriptive comments')

    parser.add_argument('-m', '--maxid',  type=str, nargs='?',
                        help='Maximum status id.')


    args = parser.parse_args(arglist)

    if args.outfile is None:
        outfile = sys.stdout
    else:
        if os.path.exists(args.outfile):
            shutil.move(args.outfile, args.outfile + '.bak')

        outfile = file(args.outfile, 'w')

    if not args.no_comments:
        comments = ''

        comments += '# twitterSearch\n'
        comments += '#     outfile=' + (args.outfile or '<stdout>') + '\n'
        if args.user:
            comments += '#     user=' + args.user + '\n'
        if args.query:
            comments += '#     query=' + args.query + '\n'
        if args.language:
            comments += '#     language=' + args.language + '\n'
        if args.since:
            comments += '#     since=' + args.since+ '\n'
        if args.until:
            comments += '#     until=' + args.until + '\n'
        if args.number:
            comments += '#     number=' + str(args.number) + '\n'

        outfile.write(comments)

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
                    application_only_auth=True
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

    tweetcount = 0
    if args.maxid is None:
        maxid = None
    else:
        maxid = int(args.maxid)

    fieldnames = ['user', 'date', 'text', 'replies', 'retweets', 'favorites', 'reply-to', 'reply-to-user', 'reply-to-user-id', 'quote', 'lang', 'geo', 'mentions', 'hashtags', 'user-id', 'id']
    outunicodecsv=unicodecsv.DictWriter(outfile, fieldnames,
                                        extrasaction='ignore', lineterminator=os.linesep)
    outunicodecsv.writeheader()

    while True:
        query  = 'q='
        query += args.query                 if args.query    else ''
        query += ('&geocode=' + args.geo)   if args.geo      else ''
        query += ('&from=' + args.user)     if args.user     else ''
        query += ('&lang=' + args.language) if args.language else ''
        query += ('&since=' + args.since)   if args.since    else ''
        query += ('&until=' + args.until)   if args.until    else ''
        query += ('&count=' + str(args.number - tweetcount)) if args.number else ''
        query += ('&max_id='+str(maxid)) if maxid else ''
        if args.verbosity >= 2:
            print('Query: ' + query, file=sys.stderr)
        tweets = api.GetSearch(raw_query=query)
        if len(tweets) == 0:
            break

        for tweet in tweets:
            if tweet.retweeted_status is None:
                outunicodecsv.writerow({
                    'user': tweet.user.screen_name,
                    'date': datetime.datetime.utcfromtimestamp(tweet.created_at_in_seconds).isoformat(),
                    'text': tweet.text,
                    'reply-to': tweet.in_reply_to_status_id,
                    'reply-to-user': tweet.in_reply_to_screen_name,
                    'reply-to-user-id': tweet.in_reply_to_user_id,
                    'retweets': tweet.retweet_count,
                    'favorites': tweet.favorite_count,
                    'lang': tweet.lang,
                    'geo': tweet.geo,
                    'mentions': u' '.join([u'@'+user.screen_name for user    in tweet.user_mentions]),
                    'hashtags': u' '.join([u'#'+hashtag.text     for hashtag in tweet.hashtags]),
                    'user-id': tweet.user.id,
                    'id': tweet.id_str,
                })

                tweetcount += 1
                if args.number and tweetcount == args.number:
                    break

        if args.number and tweetcount == args.number:
            break

        maxid = tweets[-1].id - 1

    outfile.close()

if __name__ == '__main__':
    twitterSearch(None)
