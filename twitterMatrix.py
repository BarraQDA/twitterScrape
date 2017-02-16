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
import string
import unicodedata
import pymp
from igraph import *
import numpy as np

parser = argparse.ArgumentParser(description='Twitter word matrix visualisation.')

parser.add_argument('-v', '--verbosity', type=int, default=1)
parser.add_argument('-j', '--jobs',      type=int, help='Number of parallel tasks, default is number of CPUs')
parser.add_argument('-b', '--batch',      type=int, default=1000000, help='Number of tweets to process per batch, or zero for unlimited. May affect performance but not results.')
parser.add_argument('-l', '--limit',     type=int, help='Limit number of tweets to process')

parser.add_argument('-c', '--column',    type=str, default='text', help='CSV column')
parser.add_argument('-w', '--words',     type=unicode, help='Comma separated list of words to use in matrix, otherwise scan CSV for most common words')

parser.add_argument('--margin',    type=int, default=0, help='Graph margin')
parser.add_argument('--width',     type=int, default=600)
parser.add_argument('--height',    type=int, default=800)


parser.add_argument('--textblob', action='store_true', help='Use textblob for analysis')

parser.add_argument('infile', type=str, nargs='?',      help='Input CSV file, if missing use stdin.')

args = parser.parse_args()

if args.jobs is None:
    import multiprocessing
    args.jobs = multiprocessing.cpu_count()

if args.verbosity > 1:
    print("Using " + str(args.jobs) + " jobs.", file=sys.stderr)

if args.infile is None:
    infile = sys.stdin
else:
    infile = file(args.infile, 'r')

if args.batch == 0:
    args.batch = sys.maxint

# See https://bytes.com/topic/python/answers/513222-csv-comments#post1997980
def CommentStripper (iterator):
    for line in iterator:
        if line [:1] == '#':
            continue
        if not line.strip ():
            continue
        yield line

inreader=unicodecsv.DictReader(CommentStripper(infile))

if args.textblob:
    # We want to catch handles and hashtags so need to manage punctuation manually
    from textblob import TextBlob, Word

    from nltk.tokenize import RegexpTokenizer
    tokenizer=RegexpTokenizer(r'https?://[^"\' ]+|[@|#]?\w+')

if args.textblob:
    wordlist = [Word(word).lemmatize().lower() for word in args.words.split(',')]
else:
    wordlist = [word.lower() for word in args.words.split(',')]

if args.verbosity > 1:
    print("Loading twitter data.", file=sys.stderr)

mergedmatrices = []
tweetcount = 0
while (tweetcount < args.limit) if args.limit is not None else True:
    if args.verbosity > 2:
        print("Loading twitter batch.", file=sys.stderr)

    rows = []
    batchtotal = min(args.batch, args.limit - tweetcount) if args.limit is not None else args.batch
    batchcount = 0
    while batchcount < batchtotal:
        try:
            rows += [next(inreader)]
            batchcount += 1
        except StopIteration:
            break

    if batchcount == 0:
        break

    if args.verbosity > 2:
        print("Processing twitter batch.", file=sys.stderr)

    tweetcount += batchcount
    rowcount = len(rows)

    matrices = pymp.shared.list()
    with pymp.Parallel(args.jobs) as p:
        matrix = []
        for rowindex in p.range(0, rowcount):
            row = rows[rowindex]
            text = row[args.column]

            if args.textblob:
                textblob = TextBlob(text, tokenizer=tokenizer)
                rowwordlist = []
                for word in textblob.tokens:
                    if word.isalpha():
                        lemma = word.lemmatize()
                        if lemma.lower() in wordlist:
                            rowwordlist += [lemma]

                #rowwordlist = [word.lemmatize() for word,pos in textblob.tags if word.lower() not in exclude and pos[0:2] in posinclude]

            else:
                rowwordlist = [word for word in text.split() if word.lower() in wordlist]

            matrix.append( [int(word in rowwordlist) for word in wordlist] )

        with p.lock:
            matrices.append(matrix)

    for matrix in matrices:
        mergedmatrices += list(matrix)

# Calculate the dot product of the transposed occurrence matrix with the occurrence matrix
cooccurrencematrix = np.dot(zip(*mergedmatrices), mergedmatrices).tolist()

if args.verbosity > 1:
    print("Generating co-occurrence graph.", file=sys.stderr)

graph = Graph.Weighted_Adjacency(cooccurrencematrix, mode='undirected', loops=False)

visual_style={}
visual_style['vertex_size'] =  rescale(graph.degree(), out_range=(5, 30))
visual_style['vertex_label'] = [word.encode('ascii', 'ignore') for word in wordlist]
visual_style['margin'] = 100
visual_style['bbox'] = (args.width, args.height)
visual_style['edge_width'] = rescale(graph.es["weight"], out_range=(1, 8))
visual_style['layout'] = graph.layout_fruchterman_reingold()

plot(graph, **visual_style)
