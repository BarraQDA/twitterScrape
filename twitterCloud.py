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
import string
import unicodedata
import pymp
from dateutil import parser as dateparser
from wordcloud import WordCloud

parser = argparse.ArgumentParser(description='Twitter feed word cloud.')

parser.add_argument('-v', '--verbosity', type=int, default=1)
parser.add_argument('-j', '--jobs',      type=int, help='Number of parallel tasks, default is number of CPUs')
parser.add_argument('-b', '--batch',      type=int, default=100000, help='Number of tweets to process per batch. Use to limit memory usage with very large files. May affect performance but not results.')

parser.add_argument('-p', '--prelude',    type=str, nargs="*", help='Python code to execute before processing')
parser.add_argument('-f', '--filter',     type=str, help='Python expression evaluated to determine whether tweet is included')
parser.add_argument(      '--since',      type=str, help='Lower bound tweet date.')
parser.add_argument(      '--until',      type=str, help='Upper bound tweet date.')
parser.add_argument('-l', '--limit',     type=int, help='Limit number of tweets to process')

parser.add_argument('-c', '--column',    type=str, default='text', help='Text column')
parser.add_argument('-s', '--score',     type=str,                 help='Comma separated list of score columns')
parser.add_argument('-x', '--exclude',   type=unicode, help='Comma separated list of words to exclude from cloud')

parser.add_argument('-m', '--mode',      choices=['textblob', 'word', 'phrase'], default='textblob')

# Arguments to pass to wordcloud
parser.add_argument('--max_font_size', type=int)
parser.add_argument('--max_words',     type=int)
parser.add_argument('--width',         type=int, default=600)
parser.add_argument('--height',        type=int, default=800)

parser.add_argument('infile', type=str, nargs='?',      help='Input CSV file, if missing use stdin.')

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

twitterread  = TwitterRead(args.infile, since=args.since, until=args.until, limit=args.limit)

from nltk.corpus import stopwords
exclude = set(stopwords.words('english'))
if args.exclude is not None:
    exclude = exclude.union(word.lower() for word in args.exclude.split(','))

score = args.score.split(',') if args.score else None

if args.mode == 'textblob':
    # We want to catch handles and hashtags so need to manage punctuation manually
    from textblob import TextBlob

    from nltk.tokenize import RegexpTokenizer
    tokenizer=RegexpTokenizer(r'https?://[^"\' ]+|[@|#]?\w+')

if args.verbosity > 1:
    print("Loading twitter data.", file=sys.stderr)

mergedscoredicts = {}
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

    scoredicts = pymp.shared.list()
    with pymp.Parallel(args.jobs) as p:
        scoredict = {}
        for rowindex in p.range(0, rowcount):
            row = rows[rowindex]
            if args.filter and not evalfilter(**row):
                continue

            text = row[args.column]
            if args.mode == 'textblob':
                textblob = TextBlob(text, tokenizer=tokenizer)
                wordlist = []
                for word in textblob.tokens:
                    if word.isalpha():
                        lemma = word.lemmatize()
                        if lemma.lower() not in exclude:
                            wordlist += [lemma]
            elif args.mode == 'word':
                wordlist = [word for word in text.split() if word.lower() not in exclude]
            else:
                wordlist = [text]

            for word in wordlist:
                if score is None:
                    wordscore = 1
                else:
                    wordscore = 0
                    for col in score:
                        wordscore += int(row[col])

                scoredict[word] = scoredict.get(word, 0) + wordscore

        with p.lock:
            scoredicts += [scoredict]

    for scoredict in scoredicts:
        for index in scoredict:
            mergedscoredicts[index] = mergedscoredicts.get(index, 0) + scoredict[index]

mergedscoredicts = mergedscoredicts.items()

if args.verbosity > 1:
    print("Generating word cloud.", file=sys.stderr)

# Generate a word cloud image
wordcloud = WordCloud(max_font_size=args.max_font_size,
                      max_words=args.max_words,
                      width=args.width,
                      height=args.height).generate_from_frequencies(mergedscoredicts)

# Display the generated image:
# the matplotlib way:
import matplotlib.pyplot as plt
plt.figure()
plt.imshow(wordcloud)
plt.axis("off")
plt.show()

# The pil way (if you don't have matplotlib)
#image = wordcloud.to_image()
#image.show()

