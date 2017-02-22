# twitterScrape

A collection of Python tools to mine and manipulate mass Twitter data.


## Overview

The traditional way of retrieving Twitter data uses Twitter's API, and numerous libraries exist to do so.  This method work s well, but has certain limitations inherent in the API, namely a hard limit of 3200 results per query and, even more critically, no access to tweets older than one or two weeks.

twitterScrape builds on [work by Jefferson Henrique](https://github.com/Jefferson-Henrique/GetOldTweets-python) that retrieves data by mimicking someone using the Web interface to search Twitter and repeatedly scrolling down to collect more tweets. It includes a class TwitterFeed that presents Twitter data as a feed.

The core of twitterScrape is the script twitterScrape.py that runs and merges queries, outputting its results in the form of CSV (comma-separated variable) files. It can merge multiple input files along with live twitter feeds.

The scripts twitterFilter.py, twitterRegExp.py and twitterMatrix.py process the resulting output file from twitterScrape.py.  twitterFilter.py performs additional filtering of the tweets, producing a new CSV file in the same format containing only those tweets that pass the filter.  twitterRegExp.py runs each tweet from an input CSV file through a regular expression processor, outputting a new CSV file whose columns are the named matches defined by the regular expression, and sorted according to the total score obtained by each distinct result. For more details, see the description of each of these scripts. twitterMatrix.py computes word co-occurrence matrices from an twitter data output file.

The scripts twitterCloud.py and twitterGraph.py produce visualisation of data from the preceding scripts. twitterCloud.py produces word clouds from a given column of either result of data mining, or subsequent processing with twitterFilter.py or twitterRegExp.py. twitterGraph.py produces a simple graph from a matrix such as a co-occurrence matrix produced by twitterMatrix.py.

## Usage

All of the scripts use the [argparse](https://docs.python.org/3/library/argparse.html) module to parse their arguments.  Calling the script with a single argument '-h' will generate a help message that outlines the usage of the script.
