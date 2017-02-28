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

import urllib,urllib2,json,re,datetime,sys,cookielib
from pyquery import PyQuery
import lxml

class TwitterFeed(object):
    def __init__(self, language=None, user=None, since=None, until=None, query=None, timeout=None):
        urlGetData = ''
        urlGetData += (' lang:' + language) if language is not None else ''
        urlGetData += (' from:' + user)     if user     is not None else ''
        urlGetData += (' since:' + since)   if since    is not None else ''
        urlGetData += (' until:' + until)   if until    is not None else ''
        urlGetData += (' ' + query)         if query    is not None else ''

        self.timeout = timeout
        self.url = 'https://twitter.com/i/search/timeline?f=tweets&q=' + urllib.quote(urlGetData) + '&src=typd&max_position='
        self.position = ''
        self.opener = None
        self.cookieJar = cookielib.CookieJar()
        self.tweets = None

    PARSER=None
    FORCE_SPACE_TAGS={'a'}

    def next(self):

        # Define our own text extraction function as pyquery's is buggy - it puts spaces
        # in the middle of URLs.
        def text(tweet):
            text = []

            def add_text(tag, no_tail=False):
                if tag.tag in TwitterFeed.FORCE_SPACE_TAGS:
                    text.append(u' ')
                if tag.text and not isinstance(tag, lxml.etree._Comment):
                    text.append(tag.text)
                for child in tag.getchildren():
                    add_text(child)
                if not no_tail and tag.tail:
                    text.append(tag.tail)

            for tag in tweet:
                add_text(tag, no_tail=True)

            return u''.join(text)


        if self.opener is None:
            self.opener = urllib2.build_opener(urllib2.HTTPCookieProcessor(self.cookieJar))
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
                except:
                    sys.stderr.write("Unrecognised response from twitter to URL: " + self.url + self.position + '\n')
                    raise

            if self.tweets is None:
                raise StopIteration

            try:
                tweet = next(self.tweets)
                tweetPQ = PyQuery(tweet, parser=TwitterFeed.PARSER)
            except StopIteration:
                self.tweets = None
                continue

            # Skip retweets
            retweet = tweetPQ("span.js-retweet-text").text()
            if retweet != '':
                continue

            # Build tweet as dictionary
            ret = {}

            ret['id']        = int(tweetPQ.attr("data-tweet-id"))
            ret['datetime']  = datetime.datetime.utcfromtimestamp(
                                    int(tweetPQ("small.time span.js-short-timestamp").attr("data-time")))
            ret['user']      = tweetPQ("span.username.js-action-profile-name b").text()
            ret['lang']      = tweetPQ("p.js-tweet-text").attr("lang")
            #ret['text']      = re.sub(r"\s+", " ", tweetPQ("p.js-tweet-text").text().replace('# ', '#').replace('@ ', '@'))
            ret['text']      = text(tweetPQ("p.js-tweet-text"))
            ret['retweets']  = int(tweetPQ("span.ProfileTweet-action--retweet span.ProfileTweet-actionCount").attr("data-tweet-stat-count").replace(",", ""))
            ret['favorites'] = int(tweetPQ("span.ProfileTweet-action--favorite span.ProfileTweet-actionCount").attr("data-tweet-stat-count").replace(",", ""))
            ret['permalink'] = 'https://twitter.com' + tweetPQ.attr("data-permalink-path")

            geoSpan = tweetPQ('span.Tweet-geo')
            ret['geo'] = geoSpan.attr('title') if len(geoSpan) > 0 else ''

            ret['mentions']  = " ".join(re.compile('(@\\w*)').findall(ret['text']))
            ret['hashtags']  = " ".join(re.compile('(#\\w*)').findall(ret['text']))

            return ret
