#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2017 Jonathan Schultz
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of264264
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

from __future__ import print_function
import argparse
import gooey
from importlib import import_module

modulenames = ['twitterScrape.twitterScrape', 'twitterScrape.twitterSearch', 'csvProcess.csvReplay']

@gooey.Gooey(default_cols=1,
             load_cmd_args=False, use_argparse_groups=True, use_tabs=True)
def twitterGui(arglist=None):
    parser = gooey.GooeyParser(description="Twitter Scraping tools.",
                               fromfile_prefix_chars='@')
    subparsers = parser.add_subparsers()
    for modulename in modulenames:
        module = import_module(modulename)
        subparser = subparsers.add_parser(modulename.split(".")[-1])
        module.add_arguments(subparser)

    kwargs = vars(parser.parse_args(arglist))
    kwargs['comments'] = kwargs['build_comments'](kwargs)
    kwargs['func'](**kwargs)

if __name__ == '__main__':
    twitterGui(None)
