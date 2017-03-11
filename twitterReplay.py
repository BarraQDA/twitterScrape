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
import re
import shutil
import importlib

parser = argparse.ArgumentParser(description='Replay file building process.')

parser.add_argument('-v', '--verbosity',  type=int, default=1)
parser.add_argument('-j', '--jobs',       type=int, help='Number of parallel tasks, default is number of CPUs. May affect performance but not results.')
parser.add_argument('-b', '--batch',      type=int, default=100000, help='Number of tweets to process per batch. Use to limit memory usage with very large files. May affect performance but not results.')
parser.add_argument('--no-comments',    action='store_true', help='Do not output descriptive comments')

parser.add_argument('--dry-run',       action='store_true', help='Print but do not execute command')

parser.add_argument('infile',  type=str, help='Input CSV file, if missing use stdin.')

args = parser.parse_args()

twitterread  = TwitterRead(args.infile)

fileregexp = re.compile(r"#+ (.+) #+", re.UNICODE)
cmdregexp = re.compile(r"#\s+(\w+)", re.UNICODE)
argregexp = re.compile(r"#\s+(\w+)(?:=(.+))?", re.UNICODE)

comments = twitterread.comments.splitlines()
del twitterread

filematch = fileregexp.match(comments.pop(0))
if filematch:
    file = filematch.group(1)

cmdmatch = cmdregexp.match(comments.pop(0))
if cmdmatch:
    cmd = cmdmatch.group(1)

arglist = []
infile = []
outfile = None
argmatch = argregexp.match(comments.pop(0))
lastargname = ''
while argmatch:
    argname  = argmatch.group(1)
    argvalue = argmatch.group(2)

    if argname == 'infile':
        infile.append(argvalue)
    else:
        if argname == 'outfile':
            outfile = argvalue
        if argname != lastargname:
            arglist.append('--' + argname)
            lastargname = argname

        if argvalue is not None:
            arglist.append(argvalue)

    argmatch = argregexp.match(comments.pop(0))

arglist += ['--verbosity', str(args.verbosity), '--batch', str(args.batch)]
if args.jobs:
    arglist += ['--jobs', str(args.jobs)]
if args.no_comments:
    arglist += ['--no-comments']

arglist += infile

if args.verbosity > 1:
    print("Command: " + cmd + " " + ' '.join(arglist), file=sys.stderr)

if not args.dry_run:
    if outfile == args.infile:
        bakfile = file + '.bak'
        if args.verbosity > 1:
            print("Renaming " + file + " to " + bakfile, file=sys.stderr)

        shutil.move(file, bakfile)

    module = importlib.import_module(cmd)
    function = getattr(module, cmd)
    function(arglist)

