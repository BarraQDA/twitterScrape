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
parser.add_argument('-l', '--limit',     type=int, help='Limit number of tweets to process')

parser.add_argument('-c', '--column',    type=str, default='text', help='CSV column')
parser.add_argument('-x', '--exclude',   type=unicode, help='Comma separated list of words to exclude from matrix')
parser.add_argument('-n', '--number',    type=int, default=100, help='Number of words to include in graph')

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

# See https://bytes.com/topic/python/answers/513222-csv-comments#post1997980
def CommentStripper (iterator):
    for line in iterator:
        if line [:1] == '#':
            continue
        if not line.strip ():
            continue
        yield line

inreader=unicodecsv.DictReader(CommentStripper(infile))

from nltk.corpus import stopwords
exclude = set(stopwords.words('english'))
if args.exclude is not None:
    exclude = exclude.union(word.lower() for word in args.exclude.split(','))

if args.textblob:
    # We want to catch handles and hashtags so need to manage punctuation manually
    from textblob import TextBlob

    from nltk.tokenize import RegexpTokenizer
    tokenizer=RegexpTokenizer(r'https?://[^"\' ]+|[@|#]?\w+')

if args.verbosity > 1:
    print("Loading twitter data.", file=sys.stderr)

if args.limit == None:
    rows = [row for row in inreader]
else:
    rows = []
    for count in range(args.limit):
        try:
            rows += [next(inreader)]
        except StopIteration:
            break

rowcount = len(rows)

if args.verbosity > 1:
    print("Processing twitter data.", file=sys.stderr)

frequencies = {}
line=0
frequencies = pymp.shared.list()
with pymp.Parallel(args.jobs) as p:
    frequency = {}
    for rowindex in p.range(0, rowcount):
        row = rows[rowindex]
        text = row[args.column]
        if args.textblob:
            textblob = TextBlob(text, tokenizer=tokenizer)
            wordlist = []
            for word in textblob.tokens:
                if word.isalpha():
                    lemma = word.lemmatize()
                    if lemma.lower() not in exclude:
                        wordlist += [lemma]

            #wordlist = [word.lemmatize() for word,pos in textblob.tags if word.lower() not in exclude and pos[0:2] in posinclude]
        else:
            wordlist = [word for word in text.split() if word.lower() not in exclude]

        for word in wordlist:
            score = 1 + int(row['retweets']) + int(row['favorites'])
            frequency[word] = frequency.get(word, 0) + score

        #row['wordlist'] = wordlist

    with p.lock:
        frequencies.append(frequency)

mergedfrequencies = None
for frequency in frequencies:
    if mergedfrequencies is None:
        mergedfrequencies = frequency.copy()
    else:
        for index in frequency:
            mergedfrequencies[index] = mergedfrequencies.get(index, 0) + frequency[index]

if args.verbosity > 1:
    print("Sorting " + str(len(mergedfrequencies)) + " frequencies.", file=sys.stderr)

sortedfrequencies = sorted([(word, mergedfrequencies[word])
                                for word in mergedfrequencies.keys()],
                           key=lambda item: item[1],
                           reverse=True)

sortedfrequencies = sortedfrequencies[0:args.number]

if args.verbosity > 1:
    print("Building co-occurrence matrix.", file=sys.stderr)

matrices = pymp.shared.list()
with pymp.Parallel(args.jobs) as p:
    matrix = []
    for rowindex in p.range(0, rowcount):
        row = rows[rowindex]
        text = row[args.column]

        if args.textblob:
            textblob = TextBlob(text, tokenizer=tokenizer)
            wordlist = []
            for word in textblob.tokens:
                if word.isalpha():
                    lemma = word.lemmatize()
                    if lemma.lower() not in exclude:
                        wordlist += [lemma]

            #wordlist = [word.lemmatize() for word,pos in textblob.tags if word.lower() not in exclude and pos[0:2] in posinclude]
        else:
            wordlist = [word for word in text.split() if word.lower() not in exclude]

        matrix.append( [int(wordscore[0] in wordlist) for wordscore in sortedfrequencies] )

    with p.lock:
        matrices.append(matrix)

mergedmatrices = []
for matrix in matrices:
    mergedmatrices += list(matrix)

# Calculate the dot product of the transposed occurrence matrix with the occurrence matrix
cooccurrencematrix = np.dot(zip(*mergedmatrices), mergedmatrices).tolist()

if args.verbosity > 1:
    print("Generating co-occurrence graph.", file=sys.stderr)

graph = Graph.Weighted_Adjacency(cooccurrencematrix, mode='undirected', loops=False)

visual_style={}
visual_style['vertex_size'] =  rescale([wordscore[1] for wordscore in sortedfrequencies], out_range=(5, 50))
visual_style['vertex_label'] = [wordscore[0].encode('ascii', 'ignore') for wordscore in sortedfrequencies]
visual_style['margin'] = 100
visual_style['edge_width'] = rescale(graph.es["weight"], out_range=(1, 8))

plot(graph, **visual_style)
