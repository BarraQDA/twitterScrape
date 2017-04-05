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
import math

def twitterGraph(arglist):

    parser = argparse.ArgumentParser(description='Graph twitter cooccurrence matrix.',
                                     fromfile_prefix_chars='@')

    parser.add_argument('-v', '--verbosity', type=int, default=1)

    parser.add_argument('-l', '--limit', type=int, help='Limit number of edges to process')
    parser.add_argument('-t',  '--threshold', type=float, help='Threshold score for pair to be included')

    parser.add_argument('--directed',  action='store_true', help='Directed graph edges')
    parser.add_argument('--loops',     action='store_true', help='Show loops')

    parser.add_argument('--margin',    type=int, default=0, help='Graph margin')
    parser.add_argument('--width',     type=int, default=600)
    parser.add_argument('--height',    type=int, default=800)

    parser.add_argument('infile', type=str, nargs='?', help='Input edge CSV file.')

    args = parser.parse_args(arglist)

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

    if args.verbosity >= 1:
        print("Loading graph edges.", file=sys.stderr)

    nodes = set()
    graph = Graph(directed=args.directed)
    rowcount = 0
    for row in inreader:
        if row[0] == row[1] and not args.loops:
            continue
        if args.threshold and float(row[2]) < args.threshold:
            continue

        if row[0] not in nodes:
            graph.add_vertex(row[0], label=row[0].encode('utf-8'))
            nodes.add(row[0])
        if row[1] not in nodes:
            graph.add_vertex(row[1], label=row[1].encode('utf-8'))
            nodes.add(row[1])
        graph.add_edge(row[0], row[1], weight=int(row[2]))
        rowcount += 1
        if args.limit and rowcount == args.limit:
            break

    #print(graph.es["weight"])
    if args.verbosity >= 1:
        print("Plotting graph with " + str(graph.vcount()) + " vertices and " + str(graph.ecount()) + " edges.", file=sys.stderr)

    plot(graph,
        edge_width = rescale([math.log(float(val)) for val in graph.es["weight"]], out_range=(1, 20)),
        vertex_size = rescale([math.log(float(val)) for val in graph.vs.degree()], out_range=(1, 20)),
        #vertex_label = [v['label'] if v.degree() > 2 else ' ' for v in graph.vs],
        bbox = (args.width, args.height),
        margin = 100,
        layout = graph.layout_fruchterman_reingold(),
        vertex_label_size=10,
        vertex_label_dist=0.5)

if __name__ == '__main__':
    twitterGraph(None)
