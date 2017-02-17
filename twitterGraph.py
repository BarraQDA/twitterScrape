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
from igraph import *
import unicodecsv

parser = argparse.ArgumentParser(description='Graph twitter cooccurrence matrix.')

parser.add_argument('-v', '--verbosity', type=int, default=1)

parser.add_argument('--margin',    type=int, default=0, help='Graph margin')
parser.add_argument('--width',     type=int, default=600)
parser.add_argument('--height',    type=int, default=800)

parser.add_argument('infile', type=str, nargs='?', help='Input cooccurrence matrix CSV file.')

args = parser.parse_args()

if args.infile is None:
    infile = sys.stdin
else:
    infile = file(args.infile, 'r')

comments = u''
while True:
    pos = infile.tell()
    line = infile.readline()
    if line[:1] == '#':
        comments += line
    else:
        infile.seek(pos)
        break

inreader=unicodecsv.reader(infile)

wordlist = next(inreader)[1:]
cooccurrencematrix = []
for row in inreader:
    introw = [int(element) for element in row[1:]]
    cooccurrencematrix.append(introw)

if args.verbosity > 1:
    print("Generating co-occurrence graph.", file=sys.stderr)

graph = Graph.Weighted_Adjacency(cooccurrencematrix, mode='undirected', loops=False)

visual_style={}
visual_style['vertex_size'] =  rescale(graph.degree(), out_range=(1, 30))
visual_style['vertex_label'] = [word.encode('ascii', 'ignore') for word in wordlist]
visual_style['margin'] = 100
visual_style['bbox'] = (args.width, args.height)
visual_style['edge_width'] = rescale(graph.es["weight"], out_range=(1, 20))
visual_style['layout'] = graph.layout_fruchterman_reingold()

plot(graph, **visual_style)
