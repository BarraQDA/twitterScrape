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
import gooey
import argparse
from requests_oauthlib import OAuth1Session
import webbrowser
import twitter
import sys
import os
import shutil
from TwitterFeed import TwitterWrite
import string
from dateutil import parser as dateparser
import calendar
import datetime

# Hack away deprecation warning -
import warnings
warnings.simplefilter("ignore", DeprecationWarning)

def add_arguments(parser):
    parser.description = "Search twitter using API."

    querygroup = parser.add_argument_group('Query')
    querygroup.add_argument('-s', '--string',   type=str,
                            help='String to query')
    querygroup.add_argument('-u', '--user',     type=str,
                            help='Twitter username to match')
    querygroup.add_argument('-l', '--language', type=str,
                            help='Language filter')
    querygroup.add_argument('-g', '--geo',      type=str,
                            help='Geographic filter')
    querygroup.add_argument(      '--since',    type=str, widget='DateChooser',
                            help='Lower bound search date/time')
    querygroup.add_argument(      '--until',    type=str, widget='DateChooser',
                            help='Upper bound search date/time')

    outputgroup = parser.add_argument_group('Output')
    outputgroup.add_argument('-o', '--outfile',  type=str,
                             help='Output file, otherwise use stdout')
    outputgroup.add_argument('-n', '--number',   type=int,
                             help='Maximum number of results to output')
    outputgroup.add_argument('--no-comments',    action='store_true',
                             help='Do not output descriptive comments')
    outputgroup.add_argument('--no-header',      action='store_true',
                             help='Do not output CSV header with column names')

    # Twitter authentication stuff
    authgroup = parser.add_argument_group('Authentication')
    authgroup.add_argument('--auth-file', type=str, default='twitterauth.txt',
                           help='Twitter authentication data file to use or create')

    authgroup.add_argument('--consumer-key', type=str)
    authgroup.add_argument('--consumer-secret', type=str)
    authgroup.add_argument('-a', '--app-only-auth', action='store_true',
                           help="Application-only authentication")
    authgroup.add_argument('--access-token-key', type=str)
    authgroup.add_argument('--access-token-secret', type=str)

    advancedgroup = parser.add_argument_group('Advanced')
    advancedgroup.add_argument('-v', '--verbosity', type=int, default=1)
    advancedgroup.add_argument('-m', '--maxid',  type=str,
                               help='Maximum status id')

    parser.set_defaults(func=twitterSearch)
    parser.set_defaults(hiddenargs=['verbosity', 'auth_file', 'consumer_key', 'consumer_secret', 'app_only_auth', 'access_token_key', 'access_token_secret', 'no_comments'])

@gooey.Gooey(ignore_command=None, force_command='--gui',
             default_cols=1,
             load_cmd_args=True, use_argparse_groups=True, use_tabs=True)
def parse_arguments():
    parser = gooey.GooeyParser()
    add_arguments(parser)
    return vars(parser.parse_args())

def build_comments(kwargs):
    comments = ((' ' + kwargs['outfile'] + ' ') if kwargs['outfile'] else '').center(80, '#') + '\n'
    comments += '# ' + os.path.basename(__file__) + '\n'
    hiddenargs = kwargs['hiddenargs'] + ['hiddenargs', 'func']
    for argname, argval in kwargs.iteritems():
        if argname not in hiddenargs:
            if type(argval) == str or type(argval) == unicode:
                comments += '#     --' + argname + '="' + argval + '"\n'
            elif type(argval) == bool:
                if argval:
                    comments += '#     --' + argname + '\n'
            elif type(argval) == list:
                for valitem in argval:
                    if type(valitem) == str:
                        comments += '#     --' + argname + '="' + valitem + '"\n'
                    else:
                        comments += '#     --' + argname + '=' + str(valitem) + '\n'
            elif argval is not None:
                comments += '#     --' + argname + '=' + str(argval) + '\n'

    comments += '#' * 80 + '\n'
    return comments

def twitterSearch(string, user, language, geo, since, until,
                  outfile, number, no_comments, no_header,
                  auth_file, consumer_key, consumer_secret, app_only_auth, access_token_key, access_token_secret,
                  verbosity, maxid, comments, **dummy):

    until = str(calendar.timegm(dateparser.parse(until).utctimetuple())) if until else None
    since = str(calendar.timegm(dateparser.parse(since).utctimetuple())) if since else None

    fieldnames = ['user', 'date', 'text', 'replies', 'retweets', 'favorites', 'reply-to', 'reply-to-user', 'reply-to-user-id', 'quote', 'lang', 'geo', 'mentions', 'hashtags', 'user-id', 'id']
    twitterwrite = TwitterWrite(outfile, comments=None if no_comments else comments, fieldnames=fieldnames, header=not no_header)

    if auth_file:
        if os.path.exists(auth_file):
            authfile = file(auth_file, 'rU')

            authparser = argparse.ArgumentParser()
            authparser.add_argument('--consumer-key',        type=str)
            authparser.add_argument('--consumer-secret',     type=str)
            authparser.add_argument('--access-token-key',    type=str)
            authparser.add_argument('--access-token-secret', type=str)

            authargs, dummy = authparser.parse_known_args(authfile.read().split())

            consumer_key          = consumer_key        or authargs.consumer_key
            consumer_secret       = consumer_secret     or authargs.consumer_secret
            access_token_key      = access_token_key    or authargs.access_token_key
            access_token_secret   = access_token_secret or authargs.access_token_secret
            app_only_auth = not all([access_token_key, access_token_secret])

    if app_only_auth:
        api = twitter.Api(
                    consumer_key=consumer_key,
                    consumer_secret=consumer_secret,
                    application_only_auth=True,
                    sleep_on_rate_limit=True
            )
    else:
        if not all([access_token_key, access_token_secret]):
            # Twitter URLs
            REQUEST_TOKEN_URL = 'https://api.twitter.com/oauth/request_token'
            ACCESS_TOKEN_URL = 'https://api.twitter.com/oauth/access_token'
            AUTHORIZATION_URL = 'https://api.twitter.com/oauth/authorize'

            oauth_client = OAuth1Session(consumer_key, client_secret=consumer_secret, callback_uri='oob')

            resp = oauth_client.fetch_request_token(REQUEST_TOKEN_URL)
            url = oauth_client.authorization_url(AUTHORIZATION_URL)

            print('Opening browser for Twitter authentication: ' + url, file=sys.stderr)

            webbrowser.open(url)
            print('Enter your pincode? ', file=sys.stderr)
            pincode = raw_input()

            oauth_client = OAuth1Session(consumer_key, client_secret=consumer_secret,
                                        resource_owner_key=resp.get('oauth_token'),
                                        resource_owner_secret=resp.get('oauth_token_secret'),
                                        verifier=pincode)
            resp = oauth_client.fetch_access_token(ACCESS_TOKEN_URL)
            access_token_key = resp.get('oauth_token')
            access_token_secret = resp.get('oauth_token_secret')

            print('To re-use access token next time use the following arguments:', file=sys.stderr)
            print('    --access-token-key ' + access_token_key + ' --access-token-secret ' + access_token_secret, file=sys.stderr)

        api = twitter.Api(
                    consumer_key=consumer_key,
                    consumer_secret=consumer_secret,
                    access_token_key=access_token_key,
                    access_token_secret=access_token_secret,
                    sleep_on_rate_limit=True
            )

    if not os.path.exists(auth_file):
        authfile = file(auth_file, 'w')
        print('--consumer-key',    consumer_key,    file=authfile)
        print('--consumer-secret', consumer_secret, file=authfile)
        if all([access_token_key, access_token_secret]):
            print('--access_token-key',    access_token_key,    file=authfile)
            print('--access_token-secret', access_token_secret, file=authfile)

        authfile.close()

    tweetcount = 0
    if maxid is None:
        maxid = None
    else:
        maxid = int(maxid)

    while True:
        query  = 'q='
        query += string                if string   else ''
        query += ('&geocode=' + geo)   if geo      else ''
        query += ('&from=' + user)     if user     else ''
        query += ('&lang=' + language) if language else ''
        query += ('&since=' + since)   if since    else ''
        query += ('&until=' + until)   if until    else ''
        query += ('&count=' + str(number - tweetcount)) if number else ''
        query += ('&max_id='+str(maxid)) if maxid else ''
        if verbosity >= 2:
            print('Query: ' + query, file=sys.stderr)
        try:
            tweets = api.GetSearch(raw_query=query)
        except twitter.error.TwitterError as error:
            print(error.message)
            break

        if len(tweets) == 0:
            break

        for tweet in tweets:
            if tweet.retweeted_status is None:
                twitterwrite.write({
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
                    'mentions': u' '.join([mention.screen_name for mention in tweet.user_mentions]),
                    'hashtags': u' '.join([hashtag.text        for hashtag in tweet.hashtags]),
                    'user-id': tweet.user.id,
                    'id': tweet.id_str,
                })

                tweetcount += 1
                if number and tweetcount == number:
                    break

        if number and tweetcount == number:
            break

        maxid = tweets[-1].id - 1

    del twitterwrite

if __name__ == '__main__':
    kwargs = parse_arguments()
    kwargs['comments'] = build_comments(kwargs)
    kwargs['func'](**kwargs)
