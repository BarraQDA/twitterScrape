# twitterScrape

A collection of Python tools to mine and manipulate mass Twitter data.


## Overview

The traditional way of retrieving Twitter data uses Twitter's API, and numerous libraries exist to do so.  This method work s well, but has certain limitations inherent in the API, namely a hard limit of 3200 results per query and, even more critically, no access to tweets older than one or two weeks.

twitterScrape builds on [work by Jefferson Henrique](https://github.com/Jefferson-Henrique/GetOldTweets-python) that retrieves data by mimicking someone using the Web interface to search Twitter and repeatedly scrolling down to collect more tweets. It includes a class TwitterFeed that presents Twitter data as a feed.

The core of twitterScrape is the script [twitterScrape.py](https://github.com/BarraQDA/twitterScrape/blob/master/twitterScrape.py) that runs and merges queries, outputting its results in the form of CSV (comma-separated variable) files. It can merge multiple input files along with live twitter feeds.

The scripts [twitterFilter.py](https://github.com/BarraQDA/twitterScrape/blob/master/twitterFilter.py), [twitterRegExp.py](https://github.com/BarraQDA/twitterScrape/blob/master/twitterRegExp.py) and [twitterMatrix.py](https://github.com/BarraQDA/twitterScrape/blob/master/twitterMatrix.py) process the resulting output file from [twitterScrape.py](https://github.com/BarraQDA/twitterScrape/blob/master/twitterScrape.py).  [twitterFilter.py](https://github.com/BarraQDA/twitterScrape/blob/master/twitterFilter.py) performs additional filtering of the tweets, producing a new CSV file in the same format containing only those tweets that pass the filter.  [twitterRegExp.py](https://github.com/BarraQDA/twitterScrape/blob/master/twitterRegExp.py) runs each tweet from an input CSV file through a regular expression processor, outputting a new CSV file whose columns are the named matches defined by the regular expression, and sorted according to the total score obtained by each distinct result. For more details, see the description of each of these scripts. [twitterMatrix.py](https://github.com/BarraQDA/twitterScrape/blob/master/twitterMatrix.py) computes word co-occurrence matrices from an twitter data output file.

The scripts [twitterCloud.py](https://github.com/BarraQDA/twitterScrape/blob/master/twitterCloud.py) and [twitterGraph.py](https://github.com/BarraQDA/twitterScrape/blob/master/twitterGraph.py) produce visualisation of data from the preceding scripts. [twitterCloud.py](https://github.com/BarraQDA/twitterScrape/blob/master/twitterCloud.py) produces word clouds from a given column of either result of data mining, or subsequent processing with [twitterFilter.py](https://github.com/BarraQDA/twitterScrape/blob/master/twitterFilter.py) or [twitterRegExp.py](https://github.com/BarraQDA/twitterScrape/blob/master/twitterRegExp.py). [twitterGraph.py](https://github.com/BarraQDA/twitterScrape/blob/master/twitterGraph.py) produces a simple graph from a matrix such as a co-occurrence matrix produced by [twitterMatrix.py](https://github.com/BarraQDA/twitterScrape/blob/master/twitterMatrix.py).

## Installation

The scripts have all been developed and tested using Python 2.7.  I have made reasonable efforts to make the code portable to Python 3, but your mileage may vary.

The scripts all require a number of libraries that can be installed using pip. I haven't made a full list of these yet but it should be simple enough to work out.

## Usage

All of the scripts begin with a [shebang](https://en.wikipedia.org/wiki/Shebang_(Unix)) so that they can be directly invoked from the command line on a Unix-like system, including Mac OS, by entering

    <path to script name> <script arguments>

With Windows you will need to do something like

    python <path to script name> <script arguments>

All of the scripts use the [argparse](https://docs.python.org/3/library/argparse.html) module to parse their arguments.  Invoking a script with the single argument `-h` generates a help message that outlines the usage of the script.

## Get involved

Please help me out! Fork this library and make pull requests, including to this README file. I promise to accept them promptly and with minimal fuss.
