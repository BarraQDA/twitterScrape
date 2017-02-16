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

parser = argparse.ArgumentParser(description='Word proximity calculator.')

parser.add_argument('-v', '--verbosity', type=int, default=1)

parser.add_argument('-j', '--jobs', type=int, help='Number of parallel tasks, default is number of CPUs')
parser.add_argument('-b', '--batch',      type=int, default=1000000, help='Number of tweets to process per batch, or zero for unlimited. May affect performance but not results.')
parser.add_argument('-l', '--limit',     type=int, help='Limit number of tweets to process')

parser.add_argument('-k', '--keyword', type=str, help='Key word for search.')
parser.add_argument('-t', '--threshold', type=float,
                    help='Threshold value for word to be output')
parser.add_argument('-n', '--number', type=int, default=100,
                    help='Limit number of words to output')
parser.add_argument('-o', '--outfile', type=str, nargs='?',
                    help='Output file name, otherwise use stdout.')

parser.add_argument('--textblob', action='store_true', help='Use textblob for analysis')

parser.add_argument('infile', type=str, nargs='?',
                    help='Input CSV file, if missing use stdin.')

args = parser.parse_args()

if args.jobs is None:
    import multiprocessing
    args.jobs = multiprocessing.cpu_count()

if args.keyword is None:
    raise RuntimeError("Keyword must be provided.")

keywordlc = args.keyword.lower()

if args.verbosity > 1:
    print("Using " + str(args.jobs) + " jobs.", file=sys.stderr)

if args.infile is None:
    infile = sys.stdin
else:
    infile = file(args.infile, 'r')

# Open output file already so we catch file error before doing all the hard work
if args.outfile is None:
    outfile = sys.stdout
else:
    outfile = file(args.outfile, 'w')

from nltk.corpus import stopwords
stop = set(stopwords.words('english'))

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
    from textblob import TextBlob

    from nltk.tokenize import RegexpTokenizer
    tokenizer=RegexpTokenizer(r'https?://[^"\' ]+|[@|#]?\w+')

if args.verbosity > 1:
    print("Loading twitter data.", file=sys.stderr)

mergedscore = {}
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

    scores = pymp.shared.list()
    with pymp.Parallel(args.jobs) as p:
        score = {}
        for rowindex in p.range(0, rowcount):
            #print("Thread :" + str(p.thread_num) + " score count is " + str(len(score.values())))
            if args.textblob:
                textblob = TextBlob(rows[rowindex]['text'], tokenizer=tokenizer)
                wordlist = textblob.tokens
            else:
                wordlist = rows[rowindex]['text'].split()

            keywordindices = [index for index,word in enumerate(wordlist)
                                    if keywordlc in word.lower()]
            if len(keywordindices) > 0:
                if args.textblob:
                    wordproximity = [(word.lemmatize().lower(), min([abs(index - keywordindex) for keywordindex in keywordindices]))
                                    for index,word in enumerate(wordlist) if word.lower() not in stop]
                else:
                    wordproximity = [(word.lower(), min([abs(index - keywordindex) for keywordindex in keywordindices]))
                                    for index,word in enumerate(wordlist) if word.lower() not in stop]

                for word,proximity in wordproximity:
                    if proximity > 0:
                        #wordscore = 1.0
                        wordscore = 1.0 / proximity
                        score[word] = score.get(word, 0) + wordscore

        if args.verbosity > 1:
            print("Thread " + str(p.thread_num) + " analysed " + str(len(score)) + " words.", file=sys.stderr)

        with p.lock:
            scores += [score]

    for score in scores:
        for word in score:
            mergedscore[word] = mergedscore.get(word, 0) + score[word]

if args.verbosity > 1:
    print("Sorting " + str(len(mergedscore)) + " words.", file=sys.stderr)

sortedscore = sorted([{'word': word, 'score':mergedscore[word]}
                                for word in mergedscore.keys()
                                if mergedscore[word] >= args.threshold or 0],
                           key=lambda item: item['score'],
                           reverse=True)

if args.number != 0:
    sortedscore = sortedscore[0:args.number]

outunicodecsv=unicodecsv.DictWriter(outfile, fieldnames=['word', 'score'])
outunicodecsv.writeheader()
outunicodecsv.writerows(sortedscore)
outfile.close()
