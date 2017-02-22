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

parser = argparse.ArgumentParser(description='Twitter co-occurrence matrix computation.')

parser.add_argument('-v', '--verbosity', type=int, default=1)
parser.add_argument('-j', '--jobs',      type=int, help='Number of parallel tasks, default is number of CPUs')
parser.add_argument('-b', '--batch',      type=int, default=1000000, help='Number of tweets to process per batch, or zero for unlimited. May affect performance but not results.')
parser.add_argument('-l', '--limit',     type=int, help='Limit number of tweets to process')

parser.add_argument('-w', '--words',     type=unicode, required=True, help='Comma separated list of words to use in matrix')

parser.add_argument('--textblob', action='store_true', help='Use textblob to tokenise text and lemmatise words')

parser.add_argument('-o', '--outfile',    type=str, help='Output CSV file, otherwise use stdout.')

parser.add_argument('infile', type=str, nargs='?', help='Input CSV file, otherwise use stdin.')

args = parser.parse_args()

if args.jobs is None:
    import multiprocessing
    args.jobs = multiprocessing.cpu_count()

if args.verbosity > 1:
    print("Using " + str(args.jobs) + " jobs.", file=sys.stderr)

if args.batch == 0:
    args.batch = sys.maxint

if args.textblob:
    # We want to catch handles and hashtags so need to manage punctuation manually
    from textblob import TextBlob, Word

    from nltk.tokenize import RegexpTokenizer
    tokenizer=RegexpTokenizer(r'https?://[^"\' ]+|[@|#]?\w+')

if args.textblob:
    wordlist = [Word(word).lemmatize().lower() for word in args.words.split(',')]
else:
    wordlist = [word.lower() for word in args.words.split(',')]

if args.outfile is None:
    outfile = sys.stdout
else:
    outfile = file(args.outfile, 'w')

if args.infile is None:
    infile = sys.stdin
else:
    infile = file(args.infile, 'r')

# Copy comments at start of infile to outfile
while True:
    pos = infile.tell()
    line = infile.readline()
    if line[:1] == '#':
        outfile.write(line)
    else:
        infile.seek(pos)
        break

outfile.write('# twitterMatrix\n')
outfile.write('#     outfile=' + (args.outfile or '<stdout>') + '\n')
outfile.write('#     infile=' + (args.infile or '<stdin>') + '\n')
if args.limit:
    outfile.write('#     limit=' + str(args.limit) + '\n')
outfile.write('#     words=' + str(args.words) + '\n')
if args.textblob:
    outfile.write('#     textblob\n')

inreader=unicodecsv.DictReader(infile)

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
            text = rows[rowindex]['text']

            if args.textblob:
                textblob = TextBlob(text, tokenizer=tokenizer)
                rowwordlist = []
                for word in textblob.tokens:
                    if word.isalpha():
                        lemma = word.lemmatize()
                        if lemma.lower() in wordlist:
                            rowwordlist += [lemma]
            else:
                rowwordlist = [word for word in text.split() if word.lower() in wordlist]

            matrix.append( [int(word in rowwordlist) for word in wordlist] )

        with p.lock:
            matrices.append(matrix)

    for matrix in matrices:
        mergedmatrices += list(matrix)

# Calculate the dot product of the transposed occurrence matrix with the occurrence matrix
cooccurrencematrix = np.dot(zip(*mergedmatrices), mergedmatrices)
np.fill_diagonal(cooccurrencematrix, 0)
cooccurrencematrix = cooccurrencematrix.tolist()

if args.verbosity > 1:
    print("Saving co-occurrence matrix.", file=sys.stderr)

outunicodecsv=unicodecsv.writer(outfile)
outunicodecsv.writerow([''] + wordlist)
for row in range(0, len(wordlist)):
    outunicodecsv.writerow([wordlist[row]] + cooccurrencematrix[row])
outfile.close()
