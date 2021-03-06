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

import json,re,sys,ssl
if sys.version_info[0] < 3:
    from urllib import quote
    import urllib2 as urllibrequest
    import cookielib as cookiejar
    import unicodecsv as csv
else:
    from urllib.parse import quote
    import urllib.request as urllibrequest
    import http.cookiejar as cookiejar
    import csv

from pyquery import PyQuery
import lxml
import os
import shutil
from datetime import datetime
from dateutil import parser as dateparser
from future.utils import implements_iterator

@implements_iterator
class TwitterFeed(object):
    def __init__(self, language=None, user=None, since=None, until=None, query=None, timeout=None):
        urlGetData = ''
        urlGetData += (' lang:' + language) if language else ''
        urlGetData += (' from:' + user)     if user     else ''
        urlGetData += (' since:' + since.isoformat()) if since else ''
        urlGetData += (' until:' + until.isoformat()) if until else ''
        urlGetData += (' ' + query)         if query    else ''

        self.timeout = timeout
        self.url = 'https://twitter.com/i/search/timeline?f=tweets&q=' + quote(urlGetData) + '&src=typd&max_position='
        self.position = ''
        self.opener = None
        self.cookieJar = cookiejar.CookieJar()
        self.tweets = None

    PARSER=None
    FORCE_SPACE_TAGS={'a'}
    MENTIONREGEXP=re.compile(r'(?:@(\w+))', re.UNICODE)
    HASHTAGREGEXP=re.compile(r'(?:#(\w+))', re.UNICODE)

    def __next__(self):

        # Define our own text extraction function as pyquery's is buggy - it puts spaces
        # in the middle of URLs.
        def text(tweet):
            text = []

            def add_text(tag, no_tail=False):
                if tag.tag in TwitterFeed.FORCE_SPACE_TAGS:
                    text.append(u' ')
                if tag.text and not isinstance(tag, lxml.etree._Comment):
                    text.append(tag.text)
                if tag.tag == 'img':
                    text.append(tag.get("alt") or '')
                for child in tag.getchildren():
                    add_text(child)
                if not no_tail and tag.tail:
                    text.append(tag.tail)

            for tag in tweet:
                add_text(tag, no_tail=True)

            return u''.join(text)


        if self.opener is None:
            self.opener = urllibrequest.build_opener(urllibrequest.HTTPSHandler(context=ssl._create_unverified_context()),
                                                     urllibrequest.HTTPCookieProcessor(self.cookieJar))
            self.opener.addheaders = [
                ('Host', "twitter.com"),
                ('User-Agent', "Mozilla/5.0 (Windows NT 6.1; Win64; x64)"),
                ('Accept', "application/json, text/javascript, */*; q=0.01"),
                ('Accept-Language', "de,en-US;q=0.7,en;q=0.3"),
                ('X-Requested-With', "XMLHttpRequest"),
                ('Referer', self.url),
                ('Connection', "keep-alive")
            ]

        while True:
            if self.tweets is None:
                try:
                    dataJson = json.loads(self.opener.open(self.url + self.position, timeout=self.timeout).read())
                    if dataJson is not None and len(dataJson['items_html'].strip()) > 0:
                        self.position = dataJson['min_position']
                        self.tweets = PyQuery(dataJson['items_html'], parser=TwitterFeed.PARSER).items('div.js-stream-tweet')
                except KeyboardInterrupt:
                    raise
                except:
                    print ("Exception:", sys.exc_info())
                    sys.stderr.write("in response to URL: " + self.url + self.position + '\n')

            if self.tweets is None:
                raise StopIteration

            try:
                tweet = next(self.tweets)
                tweetPQ = PyQuery(tweet, parser=TwitterFeed.PARSER)
            except StopIteration:
                self.tweets = None
                continue

            # Skip retweets - this doesn't seem to ever happen???
            retweet = tweetPQ("span.js-retweet-text").text()
            if retweet != '':
                continue

            # Build tweet as dictionary
            ret = {}

            try:
                ret['id']    = int(tweetPQ.attr("data-tweet-id"))
                conversation = int(tweetPQ.attr("data-conversation-id"))
                if conversation != ret['id']:
                    ret['conversation'] = conversation

                ret['date']      = datetime.utcfromtimestamp(
                                        int(tweetPQ("small.time span.js-short-timestamp").attr("data-time")))
                ret['user']      = tweetPQ.attr("data-screen-name")
                ret['user-id']   = tweetPQ.attr("data-user-id")
                ret['lang']      = tweetPQ("p.js-tweet-text").attr("lang")
                ret['text']      = text(tweetPQ("p.js-tweet-text"))
                ret['replies']   = int(tweetPQ("span.ProfileTweet-action--reply span.ProfileTweet-actionCount").attr("data-tweet-stat-count").replace(",", ""))
                ret['retweets']  = int(tweetPQ("span.ProfileTweet-action--retweet span.ProfileTweet-actionCount").attr("data-tweet-stat-count").replace(",", ""))
                ret['favorites'] = int(tweetPQ("span.ProfileTweet-action--favorite span.ProfileTweet-actionCount").attr("data-tweet-stat-count").replace(",", ""))
                quotetweet = tweetPQ("div.QuoteTweet-innerContainer")
                if quotetweet:
                    ret['quote']         = int(quotetweet.attr("data-item-id"))
                    ret['quote-user-id'] = int(quotetweet.attr("data-user-id"))
                    ret['quote-user']    = quotetweet.attr("data-screen-name")

                #ret['permalink'] = 'https://twitter.com' + tweetPQ.attr("data-permalink-path")

                geoSpan = tweetPQ('span.Tweet-geo')
                ret['geo'] = geoSpan.attr('title') if geoSpan else ''

                ret['mentions']  = " ".join(TwitterFeed.MENTIONREGEXP.findall(ret['text']))
                ret['hashtags']  = " ".join(TwitterFeed.HASHTAGREGEXP.findall(ret['text']))

                return ret

            except TypeError:
                sys.stderr.write("Unrecognised tweet in response to URL: " + self.url + self.position + '\n')
                raise StopIteration

class TwitterRead(object):
    def __init__(self, filename, since=None, until=None, limit=None, blanks=False):
        if filename is None:
            self.file = sys.stdin
        else:
            try:
                self.file = file(filename, 'rU')
            except:
                self.file = None
                raise

        self.since  = since
        self.until  = until
        self.limit  = limit
        self.blanks = blanks

        # Extract comments at start of file
        self.comments = ''
        while True:
            line = self.file.readline()
            if line[:1] == '#':
                self.comments += line
            else:
                self.fieldnames = next(csv.reader([line]))
                break

        self.csvreader = csv.DictReader(self.file, fieldnames=self.fieldnames)
        self.count = 0

    def __iter__(self):
        return self

    def __del__(self):
        if self.file:
            self.file.close()

    def comments(self):
        return self.comments

    def fieldnames(self):
        return self.fieldnames

    def next(self):
        if self.limit and self.count == self.limit:
            raise StopIteration

        while True:
            row = next(self.csvreader)
            if row.get('id', '') == '':
                if self.blanks:
                    row['id'] = None
                    break
                else:
                    continue

            date = row.get('date')
            if date:
                try:
                    row['date'] = dateparser.parse(date)
                except (TypeError, ValueError):
                    row['date'] = None

                if self.until and row['date'] >= self.until:
                    continue
                if self.since and row['date'] < self.since:
                    raise StopIteration

            def convert_to_int(key):
                val = row.get(key)
                if val is not None:
                    try:
                        row[key] = int(val)
                    except (TypeError, ValueError):
                        row[key] = None

            convert_to_int('id')
            convert_to_int('user-id')
            convert_to_int('replies')
            convert_to_int('retweets')
            convert_to_int('favorites')
            convert_to_int('conversation')
            convert_to_int('quote')
            convert_to_int('quote-user-id')
            convert_to_int('reply-to')
            convert_to_int('reply-to-user-id')

            break

        self.count += 1

        return row

class TwitterWrite(object):
    def __init__(self, filename, comments=None, fieldnames=None, header=True):
        if filename is None:
            self.file = sys.stdout
        else:
            if os.path.exists(filename):
                shutil.move(filename, filename + '.bak')

            self.file = file(filename, 'w')

        if comments is not None:
            self.file.write(comments)

        if fieldnames is None:
            fieldnames = ['user', 'date', 'retweets', 'favorites', 'text', 'lang', 'geo', 'mentions', 'hashtags', 'id']

        self.csvwriter = csv.DictWriter(self.file,
                                               fieldnames=fieldnames,
                                               extrasaction='ignore',
                                               lineterminator=os.linesep,
                                               quoting=csv.QUOTE_NONNUMERIC)
        if header:
            self.csvwriter.writeheader()

        self.count = 0
        self.filename = filename

    def __del__(self):
        self.file.close()

    def write(self, row):
        self.csvwriter.writerow(row)
        self.count += 1
