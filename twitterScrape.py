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
import argparse
from TwitterFeed import TwitterRead, TwitterWrite
import sys, os
from dateutil import parser as dateparser
import datetime
import shutil

def twitterScrape(arglist):

    parser = argparse.ArgumentParser(description='Scrape and merge twitter feed using pyquery.',
                                     fromfile_prefix_chars='@')

    parser.add_argument('-v', '--verbosity', type=int, default=1)

    parser.add_argument('-u', '--user',     type=str, help='Twitter username to match.')
    parser.add_argument('-q', '--query',    type=str, help='Search string for twitter feed. Either USER or QUERY must be defined to open a twitter feed.')
    parser.add_argument('-l', '--language', type=str, help='Language filter for twitter feed.')
    parser.add_argument('-f', '--force',    action='store_true', help='Run twitter query over periods covered by input file(s). Default is to only run twitter period over gaps.')
    parser.add_argument('-t', '--timeout',  type=int, default=5, help='Timeout for socket operations.')

    parser.add_argument(      '--since',    type=str, help='Lower bound search date.')
    parser.add_argument(      '--until',    type=str, help='Upper bound search date.')

    parser.add_argument('-o', '--outfile',  type=str, help='Output file, otherwise use stdout.')
    parser.add_argument('-n', '--number',   type=int, default=0, help='Maximum number of results to output')
    parser.add_argument('--no-comments',    action='store_true', help='Do not output descriptive comments')

    parser.add_argument('infile', type=str, nargs='*', help='Input CSV files.')

    args = parser.parse_args()

    # Import twitter feed modules if we are going to need them
    if args.query or args.user:
        from TwitterFeed import TwitterFeed
        import urllib2

    # Parse since and until dates
    if args.until:
        args.until = dateparser.parse(args.until).date().isoformat()
    if args.since:
        args.since = dateparser.parse(args.since).date().isoformat()

    # Handle in situ replacement of output file
    tempoutfile = None
    if args.outfile is not None and os.path.isfile(args.outfile):
        args.infile += [args.outfile]
        tempoutfile = args.outfile + '.part'

    if args.no_comments:
        comments = None
    else:
        comments = ''
        if args.outfile:
            comments += (' ' + args.outfile + ' ').center(80, '#') + '\n'
        else:
            comments += '#' * 80 + '\n'

        comments += '# twitterScrape\n'
        if len(args.infile) > 0:
            comments += '#     infile=' + args.infile[0] + '\n'
            for fileidx in range(1, len(args.infile)):
                comments += '             ' + args.infile[0] + '\n'
        comments += '#     outfile=' + (args.outfile or '<stdout>') + '\n'
        if args.user:
            comments += '#     user=' + args.user + '\n'
        if args.language:
            comments += '#     language=' + args.language + '\n'
        if args.query:
            comments += '#     query=' + args.query + '\n'
        if args.since:
            comments += '#     since=' + args.since + '\n'
        if args.until:
            comments += '#     until=' + args.until + '\n'
        if args.force:
            comments += '#     force\n'
        if args.number:
            comments += '#     number=' + str(args.number) + '\n'

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
    for fileidx in range(len(args.infile)):
        thisinreader = TwitterRead(args.infile[fileidx], since=args.since, until=args.until, blanks=True)
        comments += thisinreader.comments

        inreader += [thisinreader]
        if inreader[fileidx].fieldnames != inreader[0].fieldnames:
            raise RuntimeError("File: " + args.infile[fileidx] + " has mismatched field names")

        currowitem = nextornone(inreader[fileidx])
        currow += [currowitem]
        rowcnt += [0]
        if currowitem:
            if headidx is None or currowitem['id'] > currow[headidx]['id']:
                headidx = fileidx

        if args.verbosity >= 2:
            if currowitem:
                print("Read id: " + str(currowitem['id']) + " from " + args.infile[fileidx], file=sys.stderr)
            else:
                print("End of " + args.infile[fileidx], file=sys.stderr)

    if headidx is not None and args.verbosity >= 2:
        print("Head input is " + args.infile[headidx], file=sys.stderr)

    if len(args.infile) > 0:
        fieldnames = inreader[0].fieldnames
    else:
        fieldnames = ['user', 'date', 'text', 'replies', 'retweets', 'favorites', 'reply-to', 'reply-to-user', 'reply-to-user-id', 'quote', 'lang', 'geo', 'mentions', 'hashtags', 'user-id', 'id']

    # Prepare twitter feed
    twitterfeed = None
    twittersince = None
    httperror = False
    twitteridx = len(inreader)
    inreader += [None]
    currow += [None]
    rowcnt += [0]
    args.infile += ['twitter feed']

    # Start twitter feed if already needed
    if (args.query or args.user) and (args.force or args.until is None or headidx is None or args.until > currow[headidx]['date']):
        since = args.since
        if (not args.force) and (headidx is not None):
            since = max(since, dateparser.parse(currow[headidx]['date']).date().isoformat())

        twittersince = since
        twitteruntil = args.until

        while True:
            if args.verbosity >= 1:
                print("Opening twitter feed with until:" + (twitteruntil or '') + ", since:" + (twittersince or ''), file=sys.stderr)
            try:
                twitterfeed = TwitterFeed(language=args.language, user=args.user, query=args.query,
                                            until=twitteruntil, since=twittersince, timeout=args.timeout)
                currowitem = nextornone(twitterfeed)
                break
            except (urllib2.HTTPError, urllib2.URLError) as err:
                if args.verbosity >= 2:
                    print(err, file=sys.stderr)
                pass

        if args.verbosity >= 2:
            if currowitem:
                print("Read id: " + str(currowitem['id']) + " from twitter feed", file=sys.stderr)

        currow[twitteridx] = currowitem
        if currowitem:
            inreader[twitteridx] = twitterfeed
            currowitem['date'] = currowitem['datetime'].isoformat()
            if headidx is None or currowitem['id'] > currow[headidx]['id']:
                headidx = twitteridx
                if args.verbosity >= 2:
                    print("Head input is twitter feed", file=sys.stderr)
        else:
            twitterfeed = None
            if args.verbosity >= 1:
                print("Twitter feed returned no results", file=sys.stderr)

    pacing = [(headidx is not None and currow[fileidx] and currow[fileidx]['id'] == currow[headidx]['id'])
                    for fileidx in range(len(inreader))]

    if args.verbosity >= 2:
        for fileidx in range(len(inreader)):
            if pacing[fileidx]:
                print(args.infile[fileidx] + " is pacing", file=sys.stderr)

    if headidx is None:
        if args.verbosity >= 1:
            print("Nothing to do.", file=sys.stderr)
        sys.exit()

    twitterwrite = TwitterWrite(tempoutfile if tempoutfile else args.outfile, comments=comments, fieldnames=fieldnames)

    # Main loop
    while True:
        twitterwrite.write(currow[headidx])
        if args.number and twitterwrite.count == args.number:
            break

        rowcnt[headidx] += 1
        lastid = currow[headidx]['id']
        lastdate = currow[headidx]['date']

        for fileidx in range(len(inreader)):
            if currow[fileidx] and currow[fileidx]['id'] == lastid:
                currowid = currow[fileidx]['id']
                currowdate = currow[fileidx]['date']
                try:
                    currow[fileidx] = nextornone(inreader[fileidx])
                except (urllib2.HTTPError, urllib2.URLError) as err:
                    httperror = True
                    if args.verbosity >= 2:
                        print(err, file=sys.stderr)

                if args.verbosity >= 2:
                    if currow[fileidx]:
                        print("Read id: " + str(currow[fileidx]['id']) + " from " + args.infile[fileidx], file=sys.stderr)
                    else:
                        print("End of " + args.infile[fileidx], file=sys.stderr)
                if currow[fileidx] is None:
                    if args.verbosity >= 1:
                        print("Closing " + args.infile[fileidx] + " after " + str(rowcnt[fileidx]) + " rows.", file=sys.stderr)
                    rowcnt[fileidx] = 0
                    pacing[fileidx] = False
                    inreader[fileidx] = None
                    # Forget exhausted twitter feed since it cannot be re-used
                    if fileidx == twitteridx:
                        twitterfeed = None
                # Test for blank record in CSV
                elif currow[fileidx]['id'] == None:
                    currow[fileidx] = None
                    if args.verbosity >= 1:
                        print(args.infile[fileidx] + " has gap after id:" + str(currowid) + " - " + currowdate, file=sys.stderr)

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
                    if args.verbosity >= 2:
                        if currow[fileidx]:
                            print("Read id: " + str(currow[fileidx]['id']) + " from " + args.infile[fileidx], file=sys.stderr)
                        else:
                            print("End of " + args.infile[fileidx], file=sys.stderr)
                    pacing[fileidx] = False
                    if currow[fileidx] is None:
                        if args.verbosity >= 1:
                            print("Closing " + args.infile[fileidx] + " after " + str(rowcnt[fileidx]) + " rows.", file=sys.stderr)
                        rowcnt[fileidx] = 0
                        inreader[fileidx] = None
                    elif nextheadidx is None or currow[fileidx]['id'] > currow[nextheadidx]['id']:
                        nextheadidx = fileidx

                if currow[fileidx]:
                    if 'date' not in currow[fileidx].keys():
                        currow[fileidx]['date'] = currow[fileidx]['datetime'].isoformat()

                    if pacing[fileidx]:
                        if currow[fileidx]['id'] != currow[headidx]['id']:
                            print("WARNING: Missing tweet, id: " + str(currow[headidx]['id']) + " in file: " + args.infile[fileidx], file=sys.stderr)
                            pacing[fileidx] = False
                    elif headidx is not None:
                        if currow[fileidx]['id'] == currow[headidx]['id']:
                            if args.verbosity >= 2:
                                print(args.infile[fileidx] + " now pacing.", file=sys.stderr)
                            pacing[fileidx] = True

        headidx = nextheadidx
        if args.verbosity >= 2:
            print("Head input is " + (args.infile[headidx] if headidx is not None else 'empty'), file=sys.stderr)

        # Stop reading twitter feed if it is now paced by an input file
        if (not args.force) and inreader[twitteridx] and any(pacing[0:-1]):
            if args.verbosity >= 1:
                print("Closing " + args.infile[twitteridx] + " after " + str(rowcnt[twitteridx]) + " rows.", file=sys.stderr)

            # Remember last date from twitter feed so we can re-use the feed later.
            twitterdate = currow[twitteridx]['date']
            currow[twitteridx] = None
            rowcnt[twitteridx] = 0
            pacing[twitteridx] = False
            inreader[twitteridx] = None

        # If no file is now pacing, try opening a new twitter feed
        while (args.query or args.user) and not any(pacing):
            since = args.since
            if (not args.force) and (headidx is not None):
                since = max(since, dateparser.parse(currow[headidx]['date']).date().isoformat())

            # Restart twitter feed if last tweet was a day ahead or if since argument was too late
            if twitterfeed and (args.force or (twittersince <= since and dateparser.parse(twitterdate).date() == dateparser.parse(lastdate).date())):
                if args.verbosity >= 1:
                    print("Continuing twitter feed with until:" + (twitteruntil or '') + ", since:" + (twittersince or ''), file=sys.stderr)
            else:
                # Set until date one day past lastdate because twitter returns tweets strictly before until date
                until = (dateparser.parse(lastdate) + datetime.timedelta(days=1)).date().isoformat()
                # This condition catches non-exhausted or different twitter feed
                if twitterfeed or (twittersince and twittersince > since):
                    twitterfeed = None
                    twittersince = since
                    twitteruntil = until
                # This condition allows retrying exhausted twitter feed with until date moved back by 1 day
                elif twitterfeed is None and twittersince == since:
                    if not httperror:
                        if twitteruntil and twitteruntil <= until:
                            twitteruntil = (dateparser.parse(twitteruntil) - datetime.timedelta(days=1)).date().isoformat()
                        else:
                            twitteruntil = until

                        if twitteruntil <= twittersince or twitteruntil <= '2006-03-21':     # Twitter start date
                            break
                    else:
                        # After error, just retry with same since and until as last time. Should catch too many retries
                        httperror = False
                else:
                    break

                if args.verbosity >= 1:
                    print("Opening twitter feed with until:" + twitteruntil + ", since:" + (twittersince or ''), file=sys.stderr)

                twitterfeed = TwitterFeed(language=args.language, user=args.user, query=args.query,
                                        until=twitteruntil, since=twittersince)

            if twitterfeed:
                try:
                    if args.verbosity >= 1:
                        print("Searching twitter feed for id:" + str(lastid), file=sys.stderr)
                    currowitem = nextornone(twitterfeed)
                    while currowitem and currowitem['id'] > lastid:
                        currowitem = nextornone(twitterfeed)

                    if args.verbosity >= 1:
                        if currowitem:
                            print("Found id:" + str(currowitem['id']), file=sys.stderr)

                    if currowitem:
                        if currowitem['id'] == lastid:
                            currowitem = nextornone(twitterfeed)
                            if currowitem:
                                pacing[twitteridx] = True
                                if args.verbosity >= 2:
                                    print("Twitter feed now pacing.", file=sys.stderr)

                except (urllib2.HTTPError, urllib2.URLError) as err:
                    httperror = True
                    if args.verbosity >= 1:
                        print(err, file=sys.stderr)

                if currowitem:
                    inreader[twitteridx] = twitterfeed
                    if 'date' not in currowitem.keys():
                        currowitem['date'] = currowitem['datetime'].isoformat()
                    currow[twitteridx] = currowitem
                    if headidx is None or currowitem['id'] > currow[headidx]['id']:
                        headidx = twitteridx
                        if args.verbosity >= 2:
                            print("Head input is twitter feed", file=sys.stderr)

                    break
                else:
                    twitterfeed = None
                    if args.verbosity >= 1:
                        print("End of twitter feed", file=sys.stderr)

        if not any(pacing):
            twitterwrite.write({})
            if headidx is not None:
                print("Possible missing tweets between id: " + str(lastid) + " - " + dateparser.parse(lastdate).isoformat() + " and " + str(currow[headidx]['id']) + " - " + dateparser.parse(currow[headidx]['date']).isoformat(), file=sys.stderr)
            else:
                print("Possible missing tweets after id: " + str(lastid) + " - " + dateparser.parse(lastdate).isoformat(), file=sys.stderr)
                break

    # Finish up
    del twitterwrite
    if tempoutfile:
        shutil.move(tempoutfile, args.outfile)

if __name__ == '__main__':
    twitterScrape(None)
