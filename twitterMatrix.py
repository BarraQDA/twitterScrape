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
import os
import shutil
from TwitterFeed import TwitterRead
import unicodecsv
import string
import unicodedata
from dateutil import parser as dateparser
import pymp
from igraph import *
import numpy as np

def twitterMatrix(arglist):

    parser = argparse.ArgumentParser(description='Twitter co-occurrence matrix computation.',
                                     fromfile_prefix_chars='@')

    parser.add_argument('-v', '--verbosity', type=int, default=1)
    parser.add_argument('-j', '--jobs',      type=int, help='Number of parallel tasks, default is number of CPUs')
    parser.add_argument('-b', '--batch',     type=int, default=100000, help='Number of tweets to process per batch, or zero for unlimited. May affect performance but not results.')

    parser.add_argument('-p', '--prelude',    type=str, nargs="*", help='Python code to execute before processing')
    parser.add_argument('-f', '--filter',    type=str, help='Python expression lluated to determine whether tweet is included')
    parser.add_argument(      '--since',     type=str, help='Lower bound tweet date/time in any sensible format.')
    parser.add_argument(      '--until',     type=str, help='Upper bound tweet date/time in any sensible format.')
    parser.add_argument('-l', '--limit',     type=int, help='Limit number of tweets to process')

    parser.add_argument('-c', '--column',    type=str, default='text', help='Column to generate word matrix')
    parser.add_argument('-w', '--words',     type=unicode, required=True, help='Comma separated list of words to use in matrix')

    parser.add_argument('--textblob', action='store_true', help='Use textblob to tokenise text and lemmatise words')

    parser.add_argument('-o', '--outfile',    type=str, help='Output CSV file, otherwise use stdout.')
    parser.add_argument('--no-comments',      action='store_true', help='Do not output descriptive comments')
    parser.add_argument('--no-header',        action='store_true', help='Do not output CSV header with column names')

    parser.add_argument('infile', type=str, nargs='?', help='Input CSV file, otherwise use stdin.')

    args = parser.parse_args(arglist)
    hiddenargs = ['verbosity', 'jobs', 'batch', 'no_comments']

    if args.jobs is None:
        import multiprocessing
        args.jobs = multiprocessing.cpu_count()

    if args.verbosity >= 1:
        print("Using " + str(args.jobs) + " jobs.", file=sys.stderr)

    if args.batch == 0:
        args.batch = sys.maxint

    if args.prelude:
        if args.verbosity >= 1:
            print("Executing prelude code.", file=sys.stderr)

        for line in args.prelude:
            exec(line)

    until = dateparser.parse(args.until) if args.until else None
    since = dateparser.parse(args.since) if args.since else None

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
        if os.path.exists(args.outfile):
            shutil.move(args.outfile, args.outfile + '.bak')

        outfile = file(args.outfile, 'w')

    twitterread  = TwitterRead(args.infile, since=since, until=until, limit=args.limit)
    if not args.no_comments:
        comments = ((' ' + args.outfile + ' ') if args.outfile else '').center(80, '#') + '\n'
        comments += '# ' + os.path.basename(sys.argv[0]) + '\n'
        arglist = args.__dict__.keys()
        for arg in arglist:
            if arg not in hiddenargs:
                val = getattr(args, arg)
                if type(val) == int:
                    comments += '#     --' + arg + '=' + str(val) + '\n'
                elif type(val) == str:
                    comments += '#     --' + arg + '="' + val + '"\n'
                elif type(val) == bool and val:
                    comments += '#     --' + arg + '\n'
                elif type(val) == list:
                    for valitem in val:
                        if type(valitem) == int:
                            comments += '#     --' + arg + '=' + str(valitem) + '\n'
                        elif type(valitem) == str:
                            comments += '#     --' + arg + '="' + valitem + '"\n'

        outfile.write(comments+twitterread.comments)

    argbadchars = re.compile(r'[^0-9a-zA-Z_]')
    if args.filter:
        exec "\
def evalfilter(" + ','.join([argbadchars.sub('_', fieldname) for fieldname in twitterread.fieldnames]) + ",**kwargs):\n\
    return [" + ','.join([filteritem for filteritem in args.filter]) + "]" in locals()

    if args.verbosity >= 1:
        print("Loading twitter data.", file=sys.stderr)

    mergedmatrices = []
    while True:
        if args.verbosity >= 2:
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

        if args.verbosity >= 2:
            print("Processing twitter batch.", file=sys.stderr)

        rowcount = len(rows)
        matrices = pymp.shared.list()
        with pymp.Parallel(args.jobs) as p:
            matrix = []
            for rowindex in p.range(0, rowcount):
                row = rows[rowindex]
                if args.filter:
                    rowargs = {argbadchars.sub('_', key): value for key, value in row.iteritems()}
                    if not evalfilter(**rowargs):
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

    if args.verbosity >= 1:
        print("Saving co-occurrence matrix.", file=sys.stderr)

    outunicodecsv=unicodecsv.writer(outfile, lineterminator=os.linesep)
    if not args.no_header:
        outunicodecsv.writerow(['word'] + wordlist)
    for row in range(0, len(wordlist)):
        outunicodecsv.writerow([wordlist[row]] + cooccurrencematrix[row])
    outfile.close()

if __name__ == '__main__':
    twitterMatrix(None)

