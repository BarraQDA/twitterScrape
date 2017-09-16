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
# but WITHOUT ANY WARRANTY; without even the implied warranty of264264
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

from __future__ import print_function
import gooey
import argparse
from TwitterFeed import TwitterRead, TwitterWrite
import sys
import os
from dateutil import parser as dateparser
from datetime import datetime, date, timedelta
import pytz
import shutil

def add_arguments(parser):
    parser.description = "Scrape and merge twitter feed."

    querygroup = parser.add_argument_group('Query')
    querygroup.add_argument('-s', '--string',   type=str,
                            help='String to query.')
    querygroup.add_argument('-u', '--user',     type=str,
                            help='Twitter username to match.')
    querygroup.add_argument('-l', '--language', type=str,
                            help='Language filter for twitter feed.')
    querygroup.add_argument(      '--since',    type=str, widget='DateChooser',
                            help='Lower bound search date.')
    querygroup.add_argument(      '--until',    type=str, widget='DateChooser',
                            help='Upper bound search date.')

    outputgroup = parser.add_argument_group('Output')
    outputgroup.add_argument('-o', '--outfile',  type=str, widget='FileSaver',
                             help='Output file, otherwise use stdout.')
    outputgroup.add_argument('-n', '--number',   type=int,
                             help='Maximum number of results to output')
    outputgroup.add_argument('--no-comments',    action='store_true',
                             help='Do not output descriptive comments')
    outputgroup.add_argument('--no-header',      action='store_true',
                             help='Do not output CSV header with column names')

    inputgroup = parser.add_argument_group('Input')
    inputgroup.add_argument('infile', type=str, nargs='*', widget='FileChooser',
                            help='Input CSV files.')
    inputgroup.add_argument('-f', '--force',    action='store_true',
                            help='Run twitter query over periods covered by input file(s). Default is to only run twitter period over gaps.')

    advancedgroup = parser.add_argument_group('Advanced')
    advancedgroup.add_argument('-v', '--verbosity', type=int, default=1)
    advancedgroup.add_argument('-t', '--timeout',   type=int, default=5,
                               help='Timeout for socket operations.')

    parser.set_defaults(func=twitterScrape)
    parser.set_defaults(build_comments=build_comments)
    parser.set_defaults(hiddenargs=['hiddenargs', 'verbosity', 'timeout', 'no_comments'])

@gooey.Gooey(ignore_command=None, force_command='--gui',
             default_cols=1,
             load_cmd_args=True, use_argparse_groups=True, use_tabs=True)
def parse_arguments():
    parser = gooey.GooeyParser()
    add_arguments(parser)
    return vars(parser.parse_args())

def build_comments(kwargs):
    comments = ((' ' + kwargs['outfile'] + ' ') if kwargs['outfile'] else '').center(80, '#') + '\n'
    comments += '# ' + os.path.splitext(os.path.basename(__file__))[0] + '\n'
    hiddenargs = kwargs['hiddenargs'] + ['hiddenargs', 'func', 'build_comments']
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

    return comments

def twitterScrape(string, user, language, since, until,
                  outfile, number, no_comments, no_header,
                  infile, force,
                  verbosity, timeout, comments, **dummy):

    # Import twitter feed modules if we are going to need them
    if string or user:
        from TwitterFeed import TwitterFeed
        import urllib2

    if until:
        until = dateparser.parse(until)
        if until.tzinfo:
            until = until.astimezone(pytz.utc).replace(tzinfo=None)
    if since:
        since = dateparser.parse(since)
        if since.tzinfo:
            since = since.astimezone(pytz.utc).replace(tzinfo=None)

    # Handle in situ replacement of output file
    tempoutfile = None
    if outfile is not None and os.path.isfile(outfile):
        infile += [outfile]
        tempoutfile = outfile + '.part'

    if no_comments:
        comments = None
    elif len(infile) == 0:
        comments += '#' * 80 + '\n'

    # Function to simplify reading tweets from CSV or feed
    def nextornone(reader):
        try:
            return next(reader)
        except StopIteration:
            return None

    # Open and read first row from input files
    inreader = []
    currow = []
    rowcnt = []
    headidx = None
    for fileidx in range(len(infile)):
        thisinreader = TwitterRead(infile[fileidx], since=since, until=until, blanks=True)
        comments += thisinreader.comments

        inreader += [thisinreader]
        if inreader[fileidx].fieldnames != inreader[0].fieldnames:
            raise RuntimeError("File: " + infile[fileidx] + " has mismatched field names")

        currowitem = nextornone(inreader[fileidx])
        currow += [currowitem]
        rowcnt += [0]
        if currowitem:
            if headidx is None or currowitem['id'] > currow[headidx]['id']:
                headidx = fileidx

        if verbosity >= 2:
            if currowitem:
                print("Read id: " + str(currowitem['id']) + " from " + infile[fileidx], file=sys.stderr)
            else:
                print("End of " + infile[fileidx], file=sys.stderr)

    if headidx is not None and verbosity >= 2:
        print("Head input is " + infile[headidx], file=sys.stderr)

    if len(infile) > 0:
        fieldnames = inreader[0].fieldnames
    else:
        fieldnames = ['user', 'date', 'text', 'replies', 'retweets', 'favorites', 'conversation', 'quote', 'quote-user', 'quote-user-id', 'lang', 'geo', 'mentions', 'hashtags', 'user-id', 'id']

    twitterwrite = TwitterWrite(tempoutfile if tempoutfile else outfile, comments=comments, fieldnames=fieldnames, header=not no_header)

    # Prepare twitter feed
    twitterfeed = None
    twittersince = None
    httperror = False
    twitteridx = len(inreader)
    inreader += [None]
    currow += [None]
    rowcnt += [0]
    infile += ['twitter feed']

    # Start twitter feed if already needed
    if (string or user) and (force or until is None or headidx is None or until > currow[headidx]['date']):
        twittersince = since.date() if since else None
        if (not force) and (headidx is not None):
            twittersince = max(twittersince, currow[headidx]['date'].date()) if twittersince else currow[headidx]['date'].date()

        if until:
            twitteruntil = until.date()
            if until.time() > datetime.min.time():
                twitteruntil += timedelta(days=1)
        else:
            twitteruntil = None

        while True:
            if verbosity >= 1:
                print("Opening twitter feed with until:" + (twitteruntil.isoformat() if twitteruntil else '') + ", since:" + (twittersince.isoformat() if twittersince else ''), file=sys.stderr)
            try:
                twitterfeed = TwitterFeed(language=language, user=user, query=string,
                                            until=twitteruntil, since=twittersince, timeout=timeout)
                currowitem = nextornone(twitterfeed)
                while currowitem:
                    if not until or currowitem['date'] < until:
                        break
                    currowitem = nextornone(twitterfeed)

                break
            except (urllib2.HTTPError, urllib2.URLError) as err:
                if verbosity >= 2:
                    print(err, file=sys.stderr)
                pass

        if verbosity >= 2:
            if currowitem:
                print("Read id: " + str(currowitem['id']) + " from twitter feed", file=sys.stderr)

        currow[twitteridx] = currowitem
        if currowitem:
            inreader[twitteridx] = twitterfeed
            if headidx is None or currowitem['id'] > currow[headidx]['id']:
                headidx = twitteridx
                if verbosity >= 2:
                    print("Head input is twitter feed", file=sys.stderr)
        else:
            twitterfeed = None
            if verbosity >= 1:
                print("Twitter feed returned no results", file=sys.stderr)

    pacing = [(headidx is not None and currow[fileidx] and currow[fileidx]['id'] == currow[headidx]['id'])
                    for fileidx in range(len(inreader))]

    if verbosity >= 2:
        for fileidx in range(len(inreader)):
            if pacing[fileidx]:
                print(infile[fileidx] + " is pacing", file=sys.stderr)

    if headidx is None:
        if verbosity >= 1:
            print("Nothing to do.", file=sys.stderr)
        sys.exit()

    # Main loop
    while True:
        # Catch twitter feed that has run past lower bound
        if since and currow[headidx]['date'] < since:
            break

        twitterwrite.write(currow[headidx])
        if number and twitterwrite.count == number:
            break

        rowcnt[headidx] += 1
        lastid = currow[headidx]['id']
        lastdatetime = currow[headidx]['date']

        for fileidx in range(len(inreader)):
            if currow[fileidx] and currow[fileidx]['id'] == lastid:
                currowid = currow[fileidx]['id']
                currowdate = currow[fileidx]['date']
                try:
                    currow[fileidx] = nextornone(inreader[fileidx])
                except (urllib2.HTTPError, urllib2.URLError) as err:
                    httperror = True
                    if verbosity >= 2:
                        print(err, file=sys.stderr)

                if verbosity >= 2:
                    if currow[fileidx]:
                        print("Read id: " + str(currow[fileidx]['id']) + " from " + infile[fileidx], file=sys.stderr)
                    else:
                        print("End of " + infile[fileidx], file=sys.stderr)
                if currow[fileidx] is None:
                    if verbosity >= 1:
                        print("Closing " + infile[fileidx] + " after " + str(rowcnt[fileidx]) + " rows.", file=sys.stderr)
                    rowcnt[fileidx] = 0
                    pacing[fileidx] = False
                    inreader[fileidx] = None
                    if fileidx == twitteridx:
                        twitterfeed = None
                # Test for blank record in CSV
                elif currow[fileidx]['id'] == None:
                    currow[fileidx] = None
                    if verbosity >= 1:
                        print(infile[fileidx] + " has gap after id:" + str(currowid) + " - " + currowdate.isoformat(), file=sys.stderr)

        headidx = None
        for fileidx in range(len(inreader)):
            if currow[fileidx] and (headidx is None or currow[fileidx]['id'] > currow[headidx]['id']):
                headidx = fileidx

        nextheadidx = headidx
        for fileidx in range(len(inreader)):
            if inreader[fileidx]:
                # The follow section is executed following a blank line in a CSV file
                if currow[fileidx] is None:
                    currow[fileidx] = nextornone(inreader[fileidx])
                    if verbosity >= 2:
                        if currow[fileidx]:
                            print("Read id: " + str(currow[fileidx]['id']) + " from " + infile[fileidx], file=sys.stderr)
                        else:
                            print("End of " + infile[fileidx], file=sys.stderr)
                    pacing[fileidx] = False
                    if currow[fileidx] is None:
                        if verbosity >= 1:
                            print("Closing " + infile[fileidx] + " after " + str(rowcnt[fileidx]) + " rows.", file=sys.stderr)
                        rowcnt[fileidx] = 0
                        inreader[fileidx] = None
                    elif nextheadidx is None or currow[fileidx]['id'] > currow[nextheadidx]['id']:
                        nextheadidx = fileidx

                if currow[fileidx]:
                    if pacing[fileidx]:
                        if currow[fileidx]['id'] != currow[headidx]['id']:
                            print("WARNING: Missing tweet, id: " + str(currow[headidx]['id']) + " in file: " + infile[fileidx], file=sys.stderr)
                            pacing[fileidx] = False
                    elif headidx is not None:
                        if currow[fileidx]['id'] == currow[headidx]['id']:
                            if verbosity >= 2:
                                print(infile[fileidx] + " now pacing.", file=sys.stderr)
                            pacing[fileidx] = True

        headidx = nextheadidx
        if verbosity >= 2:
            print("Head input is " + (infile[headidx] if headidx is not None else 'empty'), file=sys.stderr)

        # Stop reading twitter feed if it is now paced by an input file
        if (not force) and inreader[twitteridx] and any(pacing[0:-1]):
            if verbosity >= 1:
                print("Closing twitter feed after " + str(rowcnt[twitteridx]) + " rows.", file=sys.stderr)

            # Remember last date from twitter feed so we can re-use the feed later.
            twitterdate = currow[twitteridx]['date'].date()
            currow[twitteridx] = None
            rowcnt[twitteridx] = 0
            pacing[twitteridx] = False
            inreader[twitteridx] = None

        # If no file is now pacing, try opening a new twitter feed
        while (string or user) and not any(pacing):
            newsince = since.date() if since else None
            if (not force) and (headidx is not None):
                newsince = max(newsince or date.min, currow[headidx]['date'].date())

            # Continue with current twitter feed if since dates match and last retrieved is same day
            # as we are looking for
            if twitterfeed and (force or ((twittersince or date.min) <= (newsince or date.min) and twitterdate == lastdatetime.date())):
                if verbosity >= 1:
                    print("Continuing twitter feed with until:" + (twitteruntil.isoformat() if twitteruntil else '') + ", since:" + (twittersince.isoformat() if twittersince else ''), file=sys.stderr)
            # Otherwise start a new twitter feed.
            else:
                # Set until date one day past lastdatetime because twitter returns tweets strictly before until date
                newuntil = lastdatetime.date() + timedelta(days=1)
                # This condition catches non-exhausted or different twitter feed
                if twitterfeed or (twittersince and twittersince > newsince):
                    twitterfeed = None
                    twittersince = newsince
                    twitteruntil = newuntil
                # This condition allows retrying exhausted twitter feed with until date moved back by 1 day
                elif twitterfeed is None and twittersince == newsince:
                    if not httperror:
                        if twitteruntil and twitteruntil <= newuntil:
                            twitteruntil -= timedelta(days=1)
                        else:
                            twitteruntil = newuntil

                        if (twittersince and twitteruntil <= twittersince) or twitteruntil <= date(2006, 3, 21): # Twitter start date
                            break
                    else:
                        # After error, just retry with same since and until as last time. Should catch too many retries
                        httperror = False
                else:
                    break

                if verbosity >= 1:
                    print("Opening twitter feed with until:" + twitteruntil.isoformat() + ", since:" + (twittersince.isoformat() if twittersince else ''), file=sys.stderr)

                twitterfeed = TwitterFeed(language=language, user=user, query=string,
                                        until=twitteruntil, since=twittersince)

            if twitterfeed:
                try:
                    if verbosity >= 1:
                        print("Searching twitter feed for id:" + str(lastid), file=sys.stderr)
                    currowitem = nextornone(twitterfeed)
                    while currowitem and currowitem['id'] > lastid:
                        currowitem = nextornone(twitterfeed)

                    if verbosity >= 1:
                        if currowitem:
                            print("Found id:" + str(currowitem['id']), file=sys.stderr)

                    if currowitem:
                        if currowitem['id'] == lastid:
                            currowitem = nextornone(twitterfeed)
                            if currowitem:
                                pacing[twitteridx] = True
                                if verbosity >= 2:
                                    print("Twitter feed now pacing.", file=sys.stderr)

                except (urllib2.HTTPError, urllib2.URLError) as err:
                    httperror = True
                    if verbosity >= 1:
                        print(err, file=sys.stderr)

                if currowitem:
                    inreader[twitteridx] = twitterfeed
                    currow[twitteridx] = currowitem
                    if headidx is None or currowitem['id'] > currow[headidx]['id']:
                        headidx = twitteridx
                        if verbosity >= 2:
                            print("Head input is twitter feed", file=sys.stderr)

                    break
                else:
                    twitterfeed = None
                    if verbosity >= 1:
                        print("End of twitter feed", file=sys.stderr)

        if not any(pacing):
            twitterwrite.write({})
            if headidx is not None:
                print("Possible missing tweets between id: " + str(lastid) + " - " + lastdatetime.isoformat() + " and " + str(currow[headidx]['id']) + " - " + currow[headidx]['date'].isoformat(), file=sys.stderr)
            else:
                print("Possible missing tweets after id: " + str(lastid) + " - " + lastdatetime.isoformat(), file=sys.stderr)
                break

    # Finish up
    del twitterwrite
    if tempoutfile:
        shutil.move(tempoutfile, outfile)

def main():
    kwargs = parse_arguments()
    kwargs['comments'] = build_comments(kwargs)
    kwargs['func'](**kwargs)

if __name__ == '__main__':
    main()
