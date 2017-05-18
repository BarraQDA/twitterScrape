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
from requests_oauthlib import OAuth1Session
import webbrowser
import twitter
import sys
import os
import shutil
import unicodecsv
import re

MENTIONREGEXP=re.compile(r'(@\w+)', re.UNICODE)
HASHTAGREGEXP=re.compile(r'(#\w+)', re.UNICODE)

def twitterUserHydrate(arglist):

    parser = argparse.ArgumentParser(description='Retrieve twitter users from ID.',
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

    parser.add_argument(      '--retry',      type=int, default=5, help='Number of times to retry failed Twitter API call')

    parser.add_argument('-l', '--limit',      type=int, help='Limit number of tweets to process')

    parser.add_argument('-o', '--outfile', type=str, help='Output CSV file, otherwise use stdout')
    parser.add_argument('--no-comments',   action='store_true', help='Do not output descriptive comments')
    parser.add_argument('--no-header',     action='store_true', help='Do not output CSV header with column names')

    parser.add_argument('infile', type=str, nargs='?', help='Input CSV file, otherwise use stdin')

    args = parser.parse_args(arglist)

    if args.infile is None:
        infile = sys.stdin
    else:
        infile = file(args.infile, 'r')

    # Skip comments at start of infile.
    incomments = ''
    while True:
        line = infile.readline()
        if line[:1] == '#':
            incomments += line
        else:
            infieldnames = next(unicodecsv.reader([line]))
            break

    inreader=unicodecsv.DictReader(infile, fieldnames=infieldnames)

    if args.outfile is None:
        outfile = sys.stdout
    else:
        if os.path.exists(args.outfile):
            shutil.move(args.outfile, args.outfile + '.bak')

        outfile = file(args.outfile, 'w')

    if not args.no_comments:
        if args.outfile:
            comments = (' ' + args.outfile + ' ').center(80, '#') + '\n'
        else:
            comments = '#' * 80 + '\n'

        comments += '# twitterUserHydrate\n'
        comments += '#     outfile=' + (args.outfile or '<stdout>') + '\n'
        comments += '#     infile=' + (args.infile or '<stdin>') + '\n'
        if args.limit:
            comments += '#     limit=' + str(args.limit) + '\n'
        if args.no_header:
            comments += '#     no-header\n'

        outfile.write(comments + incomments)

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

    if args.verbosity >= 1:
        print("Loading users.", file=sys.stderr)

    fieldnames = None
    while True:
        if args.verbosity >= 2:
            print("Loading batch.", file=sys.stderr)

        rows = []
        while len(rows) < 100:
            try:
                rows.append(next(inreader))
            except StopIteration:
                break

        if len(rows) == 0:
            break

        retry = args.retry
        while True:
            try:
                userdata  = api.UsersLookup(screen_name=[row['screen_name'].encode('utf-8') for row in rows])
                break
            except twitter.error.TwitterError as error:
                for message in error.message:
                    if message['code'] == 88 and retry > 0:
                        if args.verbosity >= 2:
                            print("Retrying after twitter error: ", error, file=sys.stderr)
                        retry -= 1
                        break
                else:
                    raise

        for userdatum in userdata:
            userdict = userdatum.AsDict()
            if not fieldnames:
                fieldnames = infieldnames + userdict.keys() + list({'default_profile', 'default_profile_image', 'follow_request_sent', 'geo_enabled', 'is_translator', 'profile_background_tile', 'profile_user_background_image', 'protected', 'verified', 'withheld_in_countries', 'withheld_scope'} - set(infieldnames) - set(userdict.keys()))

                outunicodecsv=unicodecsv.DictWriter(outfile, fieldnames=fieldnames,                                        extrasaction='ignore', lineterminator=os.linesep)
                if not args.no_header:
                    outunicodecsv.writeheader()

            outunicodecsv.writerow(userdict)

    outfile.close()

if __name__ == '__main__':
    twitterUserHydrate(None)
