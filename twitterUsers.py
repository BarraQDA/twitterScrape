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
from TwitterFeed import TwitterRead, TwitterWrite
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

    parser.add_argument(      '--since',      type=str, help='Lower bound tweet date/time in any sensible format.')
    parser.add_argument(      '--until',      type=str, help='Upper bound tweet date/time in any sensible format.')
    parser.add_argument('-l', '--limit',      type=int, help='Limit number of tweets to process')

    parser.add_argument('-o', '--outfile', type=str, help='Output CSV file, otherwise use stdout')
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

        comments += '# twitterUsers\n'
        comments += '#     outfile=' + (args.outfile or '<stdout>') + '\n'
        comments += '#     infile=' + (args.infile or '<stdin>') + '\n'
        if args.limit:
            comments += '#     limit=' + str(args.limit) + '\n'
        if args.no_header:
            comments += '#     no-header\n'

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


    fieldnames = ['screen_name', 'id', 'name', 'location', 'description', 'created_at', 'email', 'favourites_count', 'followers_count', 'following', 'friends_count', 'lang']

    twitterwrite = TwitterWrite(args.outfile, comments=comments, fieldnames=fieldnames, header=not args.no_header)

    if args.verbosity >= 2:
        print("Loading tweets.", file=sys.stderr)

    users = set()
    while True:
        try:
            row = next(twitterread)
            users |= set([row['user'].encode('utf-8').lower()])
            if row.get('reply-to-user', '') != '':
                users |= set([row['reply-to-user'].encode('utf-8').lower()])
            for mention in row.get('mentions', []).split():
                users |= set([mention.encode('utf-8').lower()])
        except StopIteration:
            break

    if args.verbosity >= 2:
        print("Loaded ", twitterread.count, " tweets, ", len(users), " users. ", file=sys.stderr)

    users = sorted(users, key=lambda user: user.lower())
    useridx = 0
    while useridx < len(users):
        userslice = users[useridx:useridx+100]
        useridx += 100
        print (userslice)
        userdata  = api.UsersLookup(screen_name=userslice,include_entities=False)
        for userdatum in userdata:
            twitterwrite.write({
                'screen_name':      userdatum.screen_name,
                'id':               userdatum.id,
                'name':             userdatum.name,
                'location':         userdatum.location,
                'description':      userdatum.description,
                'created_at':       dateparser.parse(userdatum.created_at).isoformat(),
                'email':            userdatum.email,
                'favourites_count': userdatum.favourites_count,
                'followers_count':  userdatum.followers_count,
                'following':        userdatum.following,
                'friends_count':    userdatum.friends_count,
                'lang':             userdatum.lang
            })

if __name__ == '__main__':
    twitterUsers(None)
