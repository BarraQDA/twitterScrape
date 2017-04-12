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

def twitterFilter(arglist):

    parser = argparse.ArgumentParser(description='Create CSV file suitable for Gephi.',
                                     fromfile_prefix_chars='@')

    parser.add_argument('-v', '--verbosity',  type=int, default=1)

    parser.add_argument(      '--since',      type=str, help='Lower bound tweet date.')
    parser.add_argument(      '--until',      type=str, help='Upper bound tweet date.')
    parser.add_argument('-l', '--limit',      type=int, help='Limit number of tweets to process')

    parser.add_argument('-on', '--outnodefile',    type=str, help='Output CSV file for nodes.')
    parser.add_argument('-oe', '--outedgefile',    type=str, help='Output CSV file for edges.')

    parser.add_argument('infile', type=str, nargs='?', help='Input CSV file, otherwise use stdin.')

    args = parser.parse_args(arglist)

    if args.until:
        args.until = dateparser.parse(args.until).date().isoformat()
    if args.since:
        args.since = dateparser.parse(args.since).date().isoformat()

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

    #fieldnames = "Source,Target,Kind,Interval"
    fieldnames = "Source,Target,Interval"
    outedgefile.write(fieldnames + '\n')
    outedgecsv=unicodecsv.writer(outedgefile, lineterminator=os.linesep)

    fieldnames = "Id,Label,Interval"
    outnodefile.write(fieldnames + '\n')
    outnodecsv=unicodecsv.writer(outnodefile, lineterminator=os.linesep)

    if args.verbosity >= 1:
        print("Loading twitter data.", file=sys.stderr)

    outedgerows = {}
    outnoderows = {}
    outnodespellings = {}

    def addnode(id,timestamp):
        outnodespelling = outnodespellings.get(id.lower(), {})
        outnodespelling[id] = outnodespelling.get(id, 0) + 1
        outnodespellings[id.lower()] = outnodespelling

        id = id.lower()
        outrowtslist = outnoderows.get(id) or []
        outrowtslist += [rowts]
        outnoderows[id] = outrowtslist


    def addedge(source,target,timestamp):
        addnode(source,timestamp)
        addnode(target,timestamp)

        outrowindex  = (source.lower(), target.lower())
        outrowtslist = outedgerows.get(outrowindex, [])
        outrowtslist += [rowts]
        outedgerows[outrowindex] = outrowtslist

    for row in twitterread:
        rowts = calendar.timegm(dateparser.parse(row['date']).timetuple()) * 1000
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
        timestamp = outrowtslist[0]
        intervals = '<[' + str(timestamp) + ',' + str(timestamp) + ']'
        for timestamp in outrowtslist[1:]:
            intervals += ',[' + str(timestamp) + ',' + str(timestamp) + ']'
        intervals += '>'

        outnodecsv.writerow([outrowindex, canonicalspelling[outrowindex]] + [intervals])


    for outrowindex, outrowtslist in outedgerows.items():
        timestamp = outrowtslist[0]
        intervals = '<[' + str(timestamp) + ',' + str(timestamp) + ']'
        for timestamp in outrowtslist[1:]:
            intervals += ',[' + str(timestamp) + ',' + str(timestamp) + ']'
        intervals += '>'

        outedgecsv.writerow(list(outrowindex) + [intervals])


    outedgefile.close()
    outnodefile.close()

if __name__ == '__main__':
    twitterFilter(None)
