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
import datetime
from dateutil import parser as dateparser
import matplotlib as mpl
import matplotlib.pyplot as plt
from matplotlib.ticker import MaxNLocator

def twitterPlot(arglist):

    parser = argparse.ArgumentParser(description='Twitter frequency matrix plotter.',
                                     fromfile_prefix_chars='@')

    parser.add_argument('-v', '--verbosity',  type=int, default=1)

    parser.add_argument(      '--since',      type=str, help='Lower bound tweet date.')
    parser.add_argument(      '--until',      type=str, help='Upper bound tweet date.')

    parser.add_argument('infile', type=str, nargs='?', help='Input CSV file, otherwise use stdin.')

    args = parser.parse_args()

    # Parse since and until dates
    if args.until:
        args.until = dateparser.parse(args.until).date().isoformat()
    if args.since:
        args.since = dateparser.parse(args.since).date().isoformat()

    if args.infile is None:
        infile = sys.stdin
    else:
        infile = file(args.infile, 'r')

    # Skip comments at start of infile.
    while True:
        line = infile.readline()
        if line[:1] != '#':
            fieldnames = next(unicodecsv.reader([line]))
            break

    inreader=unicodecsv.reader(infile)

    dates = []
    values = []
    for valueidx in range(len(fieldnames) - 1):
        values.append([])
    while True:
        try:
            row = next(inreader)
        except StopIteration:
            break

        if args.until and row[0] >= args.until:
            continue
        if args.since and row[0] < args.since:
            break

        dates.append(dateparser.parse(row[0]))
        for valueidx in range(len(fieldnames) - 1):
            values[valueidx].append(row[valueidx+1])

    #args = ()
    #for valueidx in range(len(fieldnames) - 1):
        #args += (dates, values[valueidx], '')

    #plt.plot(*args)
    #plt.gcf().autofmt_xdate()
    #plt.show()

    mpl.style.use('classic')

    fig, ax1 = plt.subplots()
    ax1.plot(dates, values[0], 'C0')
    ax1.set_xlabel('Date')
    ax1.set_ylabel(fieldnames[1], color='C0')
    ax1.tick_params('n', colors='C0')
    ax1.yaxis.set_major_locator(MaxNLocator(integer=True))

    for valueidx in range(1, len(fieldnames) - 1):
        color = 'C' + str(valueidx)
        print(color)
        ax = ax1.twinx()
        ax.plot(dates, values[valueidx], color)
        ax.set_ylabel(fieldnames[valueidx+1], color=color)
        ax.tick_params('n', colors=color)
        ax.yaxis.set_major_locator(MaxNLocator(integer=True))

    fig.tight_layout()
    plt.show()

if __name__ == '__main__':
    twitterPlot(None)
