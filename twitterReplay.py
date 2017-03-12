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
import os
import datetime
from dateutil import parser as dateparser

parser = argparse.ArgumentParser(description='Replay twitter file processing.')

parser.add_argument('-v', '--verbosity', type=int, default=1)
parser.add_argument('-f', '--force',     action='store_true', help='Replay command even if infile is not more recent.')
parser.add_argument('--dry-run',         action='store_true', help='Print but do not execute command')

parser.add_argument('infile',  type=str, nargs='*', help='Input CSV files with comments to replay.')

args, extraargs = parser.parse_known_args()

for infilename in args.infile:
    if args.verbosity > 1:
         print("Replaying " + infilename, file=sys.stderr)

    twitterread  = TwitterRead(infilename)

    fileregexp = re.compile(r"#+ (.+) #+", re.UNICODE)
    cmdregexp = re.compile(r"#\s+(\w+)", re.UNICODE)
    argregexp = re.compile(r"#\s+(\w+)(?:=(.+))?", re.UNICODE)
    infileregexp = re.compile(r"(.+) (\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?)", re.UNICODE)

    comments = twitterread.comments.splitlines()
    del twitterread

    filematch = fileregexp.match(comments.pop(0))
    if filematch:
        file = filematch.group(1)

    cmdmatch = cmdregexp.match(comments.pop(0))
    if cmdmatch:
        cmd = cmdmatch.group(1)

    arglist = []
    replayinfile = []
    replayoutfile = None
    argmatch = argregexp.match(comments.pop(0))
    lastargname = ''
    while argmatch:
        argname  = argmatch.group(1)
        argvalue = argmatch.group(2)

        if argname == 'infile':
            infilematch = infileregexp.match(argvalue)
            if infilematch:
                infilename = infilematch.group(1)
                infilestamp = dateparser.parse(infilematch.group(2))
                curfilestamp = datetime.datetime.utcfromtimestamp(os.path.getctime(infilename))
            else:
                infilename = argvalue
                infilestamp = None

            replayinfile.append(infilename)
        else:
            if argname == 'outfile':
                replayoutfile = argvalue
            if argname != lastargname:
                arglist.append('--' + argname)
                lastargname = argname

            if argvalue is not None:
                arglist.append(argvalue)

        argmatch = argregexp.match(comments.pop(0))

    arglist += extraargs
    arglist += replayinfile

    if args.verbosity > 1:
        print("Command: " + cmd + " " + ' '.join(arglist), file=sys.stderr)

    if not args.dry_run:
        if args.force or curfilestamp > infilestamp:
            if replayoutfile == infilename:
                bakfile = file + '.bak'
                if args.verbosity > 1:
                    print("Renaming " + file + " to " + bakfile, file=sys.stderr)

                shutil.move(file, bakfile)

            module = importlib.import_module(cmd)
            function = getattr(module, cmd)
            function(arglist)
        else:
            if args.verbosity > 1:
                print("Infile is not more recent, command not executed", file=sys.stderr)

