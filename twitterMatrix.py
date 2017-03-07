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
import unicodecsv
import string
import unicodedata
from dateutil import parser as dateparser
import pymp
from igraph import *
import numpy as np

parser = argparse.ArgumentParser(description='Twitter co-occurrence matrix computation.')

parser.add_argument('-v', '--verbosity', type=int, default=1)
parser.add_argument('-j', '--jobs',      type=int, help='Number of parallel tasks, default is number of CPUs')
parser.add_argument('-b', '--batch',     type=int, default=1000000, help='Number of tweets to process per batch, or zero for unlimited. May affect performance but not results.')

parser.add_argument('-p', '--prelude',    type=str, nargs="*", help='Python code to execute before processing')
parser.add_argument('-f', '--filter',    type=str, help='Python expression evaluated to determine whether tweet is included')
parser.add_argument(      '--since',     type=str, help='Lower bound tweet date.')
parser.add_argument(      '--until',     type=str, help='Upper bound tweet date.')
parser.add_argument('-l', '--limit',     type=int, help='Limit number of tweets to process')

parser.add_argument('-c', '--column',    type=str, default='text', help='Column to generate word matrix')
parser.add_argument('-w', '--words',     type=unicode, required=True, help='Comma separated list of words to use in matrix')

parser.add_argument('--textblob', action='store_true', help='Use textblob to tokenise text and lemmatise words')

parser.add_argument('-o', '--outfile',    type=str, help='Output CSV file, otherwise use stdout.')
parser.add_argument('--no-comments',    action='store_true', help='Do not output descriptive comments')

parser.add_argument('infile', type=str, nargs='?', help='Input CSV file, otherwise use stdin.')

args = parser.parse_args()

if args.jobs is None:
    import multiprocessing
    args.jobs = multiprocessing.cpu_count()

if args.verbosity > 1:
    print("Using " + str(args.jobs) + " jobs.", file=sys.stderr)

if args.batch == 0:
    args.batch = sys.maxint

if args.prelude:
    if args.verbosity > 1:
        print("Executing prelude code.", file=sys.stderr)

    for line in args.prelude:
        exec(line)

if args.filter:
    filter = compile(args.filter, 'filter argument', 'eval')
    def evalfilter(user, date, retweets, favorites, text, lang, geo, mentions, hashtags, id, permalink, **extra):
        return eval(filter)

# Parse since and until dates
if args.until:
    args.until = dateparser.parse(args.until).date().isoformat()
if args.since:
    args.since = dateparser.parse(args.since).date().isoformat()

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

twitterread  = TwitterRead(args.infile, since=args.since, until=args.until, limit=args.limit)
if not args.no_comments:
    comments = ''

    comments += '# twitterMatrix\n'
    comments += '#     outfile=' + (args.outfile or '<stdout>') + '\n'
    comments += '#     infile=' + (args.infile or '<stdin>') + '\n'
    if args.limit:
        comments += '#     limit=' + str(args.limit) + '\n'
    if args.filter:
        comments += '#     filter=' + args.filter + '\n'
    if args.since:
        comments += '#     since=' + args.since+ '\n'
    if args.until:
        comments += '#     until=' + args.until + '\n'
    comments += '#     column=' + args.column + '\n'
    comments += '#     words=' + str(args.words) + '\n'
    if args.textblob:
        comments += '#     textblob\n'

    comments += twitterread.comments

    outfile.write(comments)

if args.verbosity > 1:
    print("Loading twitter data.", file=sys.stderr)

mergedmatrices = []
while True:
    if args.verbosity > 2:
        print("Loading twitter batch.", file=sys.stderr)

    rows = []
    batchcount = 0
    while batchcount < args.batch:
        try:
            rows.append(next(twitterread))
            batchcount += 1
        except StopIteration:
            break

    if batchcount == 0:
        break

    if args.verbosity > 2:
        print("Processing twitter batch.", file=sys.stderr)

    rowcount = len(rows)
    matrices = pymp.shared.list()
    with pymp.Parallel(args.jobs) as p:
        matrix = []
        for rowindex in p.range(0, rowcount):
            row = rows[rowindex]
            if args.filter and not evalfilter(**row):
                continue

            text = row[args.column]
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
