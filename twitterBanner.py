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
from dateutil import parser as dateparser
import datetime

MENTIONREGEXP=re.compile(r'(@\w+)', re.UNICODE)
HASHTAGREGEXP=re.compile(r'(#\w+)', re.UNICODE)

def twitterBanner(arglist):

    parser = argparse.ArgumentParser(description='Retrieve twitter users banner data from ID.',
                                     fromfile_prefix_chars='@')

    parser.add_argument('-v', '--verbosity', type=int, default=1)

    # Twitter authentication stuff
    parser.add_argument('--consumer-key', type=str, required=True,
                        help='Consumer key for Twitter authentication')
    parser.add_argument('--consumer-secret', type=str, required=True,
                        help='Consumer secret for Twitter authentication')

    parser.add_argument('--access-token-key', type=str,
                        help='Access token key for Twitter authentication')
    parser.add_argument('--access-token-secret', type=str,
                        help='Access token secret for Twitter authentication')

    parser.add_argument('-s', '--size', type=str, default='300x100', help='Banner size to fetch')

    parser.add_argument('-l', '--limit', type=int, help='Limit number of tweets to process')

    parser.add_argument('-o', '--outfile', type=str, help='Output CSV file, otherwise use stdout')
    parser.add_argument('--no-comments',   action='store_true', help='Do not output descriptive comments')
    parser.add_argument('--no-header',     action='store_true', help='Do not output CSV header with column names')

    parser.add_argument('infile', type=str, nargs='?', help='Input CSV user file, otherwise use stdin')

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

        comments += '# twitterUsers\n'
        comments += '#     outfile=' + (args.outfile or '<stdout>') + '\n'
        comments += '#     infile=' + (args.infile or '<stdin>') + '\n'
        if args.limit:
            comments += '#     limit=' + str(args.limit) + '\n'
        if args.no_header:
            comments += '#     no-header\n'

        outfile.write(comments + incomments)

    outunicodecsv=unicodecsv.DictWriter(outfile, fieldnames=inreader.fieldnames+ ['banner_url'], extrasaction='ignore', lineterminator=os.linesep)
    if not args.no_header:
        outunicodecsv.writeheader()

    # Twitter URLs
    REQUEST_TOKEN_URL = 'https://api.twitter.com/oauth/request_token'
    ACCESS_TOKEN_URL = 'https://api.twitter.com/oauth/access_token'
    AUTHORIZATION_URL = 'https://api.twitter.com/oauth/authorize'
    SIGNIN_URL = 'https://api.twitter.com/oauth/authenticate'

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

    if args.verbosity >= 2:
        print("Loading tweets.", file=sys.stderr)

    for row in inreader:
        try:
            if 'id' in row.keys():
                userdata  = api.GetProfileBanner(user_id=row['id'])
            else:
                userdata  = api.GetProfileBanner(screen_name=row['screen_name'])

            row['banner_url'] = userdata[args.size]['url']

        except twitter.error.TwitterError as error:
            if args.verbosity >= 2:
                print("Error retrieving banner for ",
                      ("id " + row['id']) if 'id' in row.keys() else ("screen name " + str(row['screen_name'])),
                      file=sys.stderr)

        outunicodecsv.writerow(row)

    outfile.close()

if __name__ == '__main__':
    twitterBanner(None)
