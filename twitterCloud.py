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
import re
from wordcloud import WordCloud

parser = argparse.ArgumentParser(description='Twitter feed word cloud.')

parser.add_argument('-v', '--verbosity', type=int, default=1)
parser.add_argument('-j', '--jobs',      type=int, help='Number of parallel tasks, default is number of CPUs')
parser.add_argument('-l', '--limit',     type=int, help='Limit number of tweets to process')

parser.add_argument('-c', '--column',    type=str, default='text', help='CSV column')
parser.add_argument('-x', '--exclude',   type=unicode, help='Comma separated list of words to exclude from cloud')

parser.add_argument('--textblob', action='store_true', help='Use textblob for analysis')

# Arguments to pass to wordcloud
parser.add_argument('--max_font_size', type=int, default=None)
parser.add_argument('--max_words',     type=int, default=None)
parser.add_argument('--width',         type=int, default=None)
parser.add_argument('--height',        type=int, default=None)

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

#posinclude = {'FW', 'JJ', 'NN', 'RB', 'VB', 'WB', 'WR'}

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
            wordlist = [word.lemmatize() for word in textblob.tokens if unicode(word).isalpha() and word.lower() not in exclude]
            #wordlist = [word.lemmatize() for word,pos in textblob.tags if word.lower() not in exclude and pos[0:2] in posinclude]
        else:
            wordlist = [word for word in text.split() if word.lower() not in exclude]

        for word in wordlist:
            frequency[word] = frequency.get(word, 0) + 1

    with p.lock:
        frequencies += [frequency]

mergedfrequencies = None
for frequency in frequencies:
    if mergedfrequencies is None:
        mergedfrequencies = frequency.copy()
    else:
        for index in frequency:
            mergedfrequencies[index] = mergedfrequencies.get(index, 0) + frequency[index]

mergedfrequencies = mergedfrequencies.items()

if args.verbosity > 1:
    print("Generating word cloud.", file=sys.stderr)

# Generate a word cloud image
wordcloud = WordCloud(max_font_size=args.max_font_size,
                      max_words=args.max_words,
                      width=args.width,
                      height=args.height).generate_from_frequencies(mergedfrequencies)

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

