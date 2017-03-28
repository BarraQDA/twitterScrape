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
from TwitterFeed import TwitterFeed, TwitterRead, TwitterWrite

def twitterRepair(arglist):

    parser = argparse.ArgumentParser(description='Validate twitter feed CSV.',
                                     fromfile_prefix_chars='@')

    parser.add_argument('-v', '--verbosity', type=int, default=1)
    parser.add_argument('-t', '--threshold', type=int, default=60, help='Number of seconds out of sequence to report.')

    parser.add_argument('-l', '--limit',     type=int, help='Limit number of tweets to process')

    parser.add_argument('-o', '--outfile',   type=str, help='Output CSV file, otherwise use stdout')

    parser.add_argument('infile',  type=str, help='Input CSV file, if missing use stdin.')

    args = parser.parse_args()

    twitterread  = TwitterRead(args.infile, limit=args.limit, blanks=True)
    twitterwrite = TwitterWrite(args.outfile, comments=twitterread.comments, fieldnames=twitterread.fieldnames)

    for row in twitterread:
        # Do repair here!

        twitterwrite.write(row)

if __name__ == '__main__':
    twitterRepair(None)
