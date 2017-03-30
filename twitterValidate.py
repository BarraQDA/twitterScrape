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
import unicodecsv
from dateutil import parser as dateparser

def twitterValidate(arglist):

    parser = argparse.ArgumentParser(description='Validate twitter feed CSV.',
                                     fromfile_prefix_chars='@')

    parser.add_argument('-v', '--verbosity', type=int, default=1)
    parser.add_argument('-t', '--threshold', type=int, default=60, help='Number of seconds out of sequence to report.')

    parser.add_argument('infile', type=str, help='Input CSV file, if missing use stdin.')

    args = parser.parse_args(arglist)

    if args.infile is None:
        infile = sys.stdin
    else:
        infile = file(args.infile, 'r')

    inreader=unicodecsv.DictReader(infile)
    lastid = None
    lastdate = None
    blankrowcount = 0
    for row in inreader:
        curid = row['id']
        if curid == '':
            blankrowcount += 1
        else:
            curdate = dateparser.parse(row['date'])
            if blankrowcount > 1:
                print("Multiple blank rows after id:" + (lastid or '') + " - " + (lastdate or ''), file=sys.stderr)
            blankrowcount = 0
            if lastid is not None and curid >= lastid:
                print("Non-decreasing id at id:" + curid + " - " + curdate.isoformat(), file=sys.stderr)
            if lastdate is not None:
                offset = (curdate - lastdate).total_seconds()
                if offset > (args.threshold or 0):
                    print("Increasing date at id:" + curid + " - " + str(offset), file=sys.stderr)
                elif offset <= -3600:
                    print("Gap of > 1 hour at id:" + curid + " - " + str(offset), file=sys.stderr)

            lastid = curid
            lastdate = curdate

    if blankrowcount > 1:
        print("Multiple blank rows after id:" + (lastid or '') + " - " + (lastdate or ''), file=sys.stderr)

if __name__ == '__main__':
    twitterValidate(None)
