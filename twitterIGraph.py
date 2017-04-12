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
from TwitterFeed import TwitterRead
import os
import shutil
import unicodecsv
import string
import unicodedata
import re
from dateutil import parser as dateparser
import calendar
import datetime
from pytimeparse.timeparse import timeparse

def twitterFilter(arglist):

    parser = argparse.ArgumentParser(description='Create CSV file suitable for Gephi.',
                                     fromfile_prefix_chars='@')

    parser.add_argument('-v', '--verbosity',  type=int, default=1)

    parser.add_argument(      '--since',      type=str, help='Lower bound tweet date.')
    parser.add_argument(      '--until',      type=str, help='Upper bound tweet date.')
    parser.add_argument('-l', '--limit',      type=int, help='Limit number of tweets to process')

    parser.add_argument('-i', '--interval',   type=str, required=True,
                        help='Interval for node/edge persistence. Format is "<w>w <d>d <h>h <m>m <s>s"')
    parser.add_argument('-g', '--granularity', type=int, default=1,
                        help='Time granularity in seconds')

    parser.add_argument('-on', '--outnodefile',    type=str, help='Output CSV file for nodes.')
    parser.add_argument('-oe', '--outedgefile',    type=str, help='Output CSV file for edges.')

    parser.add_argument('infile', type=str, nargs='?', help='Input CSV file, otherwise use stdin.')

    args = parser.parse_args(arglist)

    if args.until:
        args.until = dateparser.parse(args.until).date().isoformat()
    if args.since:
        args.since = dateparser.parse(args.since).date().isoformat()

    interval = int(datetime.timedelta(seconds=timeparse(args.interval)).total_seconds())

    twitterread  = TwitterRead(args.infile, since=args.since, until=args.until, limit=args.limit)

    if args.outedgefile is None:
        outedgefile = sys.stdout
    else:
        if os.path.exists(args.outedgefile):
            shutil.move(args.outedgefile, args.outedgefile + '.bak')

        outedgefile = file(args.outedgefile, 'w')

    if args.outnodefile is None:
        outnodefile = sys.stdout
    else:
        if os.path.exists(args.outnodefile):
            shutil.move(args.outnodefile, args.outnodefile + '.bak')

        outnodefile = file(args.outnodefile, 'w')

    fieldnames = "onset,terminus,head,tail"
    outedgefile.write(fieldnames + '\n')
    outedgecsv=unicodecsv.writer(outedgefile, lineterminator=os.linesep)

    fieldnames = "onset,terminus,vertex.id"
    outnodefile.write(fieldnames + '\n')
    outnodecsv=unicodecsv.writer(outnodefile, lineterminator=os.linesep)

    if args.verbosity >= 1:
        print("Loading twitter data.", file=sys.stderr)

    outedgerows = {}
    outnoderows = {}
    outnodespellings = {}
    outnodeids = {}

    def addnode(id,timestamp):
        nodeid = outnodeids.get(id.lower(), None)
        if not nodeid:
            addnode.lastnodeid += 1
            outnodeids[id.lower()] = addnode.lastnodeid

        outnodespelling = outnodespellings.get(id.lower(), {})
        outnodespelling[id] = outnodespelling.get(id, 0) + 1
        outnodespellings[id.lower()] = outnodespelling

        id = id.lower()
        outrowtslist = outnoderows.get(id) or []
        outrowtslist += [rowts]
        outnoderows[id] = outrowtslist

    addnode.lastnodeid = 0


    def addedge(source,target,timestamp):
        addnode(source,timestamp)
        addnode(target,timestamp)

        outrowindex  = (source.lower(), target.lower())
        outrowtslist = outedgerows.get(outrowindex, [])
        outrowtslist += [rowts]
        outedgerows[outrowindex] = outrowtslist

    starttime = sys.maxint
    endtime = 0

    for row in twitterread:
        rowts = calendar.timegm(dateparser.parse(row['date']).timetuple())
        if rowts < starttime:
            starttime = rowts
        if rowts > endtime:
            endtime = rowts
        for mention in row['mentions'].split():
            addedge(row['user'], mention, rowts)

        reply = row['reply-to-user']
        if reply != '':
            addedge(row['user'], reply, rowts)

    canonicalspelling = {}
    for nodelower, nodespelling in outnodespellings.items():
        frequencies = list(nodespelling.values())
        spellings   = list(nodespelling.keys())
        canonicalspelling[nodelower] = spellings[frequencies.index(max(frequencies))]

    for outrowindex, outrowtslist in outnoderows.items():
        timestamp = outrowtslist[-1]
        outnodecsv.writerow([timestamp/args.granularity, endtime/args.granularity, outnodeids[outrowindex]])
        #for timestamp in outrowtslist:
            #outnodecsv.writerow([timestamp, timestamp + interval, outnodeids[outrowindex]])

    for outrowindex, outrowtslist in outedgerows.items():
        for timestamp in outrowtslist:
            outedgecsv.writerow([timestamp/args.granularity, (timestamp + interval)/args.granularity, outnodeids[outrowindex[0]], outnodeids[outrowindex[1]]])

    outedgefile.close()
    outnodefile.close()

if __name__ == '__main__':
    twitterFilter(None)
