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

parser.add_argument('infile', type=str, nargs='?',      help='Input CSV file, if missing use stdin.')
parser.add_argument('-c', '--column', type=str, default='text', help='CSV column')
parser.add_argument('--textblob', action='store_true', help='Use textblob for analysis')

# Arguments to pass to wordcloud
parser.add_argument('--max_font_size', type=int, default=None)
parser.add_argument('--max_words',     type=int, default=None)
parser.add_argument('--width',         type=int, default=None)
parser.add_argument('--height',        type=int, default=None)


args = parser.parse_args()

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
stop = set(stopwords.words('english'))

if args.textblob:
    # We want to catch handles and hashtags so need to manage punctuation manually
    from textblob import TextBlob

    from nltk.tokenize import RegexpTokenizer
    tokenizer=RegexpTokenizer(r'https?://[^"\' ]+|[@|#]?\w+')

if args.verbosity > 1:
    print("Colllecting twitter text.", file=sys.stderr)

frequencies = {}
line=0
for row in inreader:
    text = row[args.column]
    if args.textblob:
        textblob = TextBlob(text, tokenizer=tokenizer)
        wordlist = [word.lemmatize() for word in textblob.tokens if word.lower() not in stop]
    else:
        wordlist = [word for word in text.split() if word.lower() not in stop]

    for word in wordlist:
        frequencies[word] = frequencies.get(word, 0) + 1

    line += 1
    if line % 100000 == 0:
        print ('.', end='', file=sys.stderr)

frequencies = frequencies.items()
print ('', file=sys.stderr)

if args.verbosity > 1:
    print("Generating word cloud.", file=sys.stderr)

# Generate a word cloud image
wordcloud = WordCloud(max_font_size=args.max_font_size,
                      max_words=args.max_words,
                      width=args.width,
                      height=args.height).generate_from_frequencies(frequencies)

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

