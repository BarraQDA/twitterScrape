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

parser = argparse.ArgumentParser(description='Replay twitter file processing.')

parser.add_argument('-v', '--verbosity', type=int, default=1)
parser.add_argument('-d', '--depth',     type=int, help='Depth of command history to replay.')
parser.add_argument('-f', '--force',     action='store_true', help='Replay command even if infile is not more recent.')
parser.add_argument('--dry-run',         action='store_true', help='Print but do not execute command')

parser.add_argument('infile',  type=str, nargs='*', help='Input CSV files with comments to replay.')

args, extraargs = parser.parse_known_args()

fileregexp = re.compile(r"#+ (.+) #+", re.UNICODE)
cmdregexp = re.compile(r"#\s+(\w+)", re.UNICODE)
argregexp = re.compile(r"#\s+(\w+)(?:=(.+))?", re.UNICODE)

depth = 0
for infilename in args.infile:
    if args.verbosity > 1:
         print("Replaying " + infilename, file=sys.stderr)

    twitterread  = TwitterRead(infilename)

    comments = twitterread.comments.splitlines()
    del twitterread

    replaystack = []
    commentline = comments.pop(0)
    filematch = fileregexp.match(commentline)
    while filematch:
        file = filematch.group(1)

        commentline = comments.pop(0)
        cmdmatch = cmdregexp.match(commentline)
        if cmdmatch:
            cmd = cmdmatch.group(1)

        arglist = []
        outfile = None
        lastargname = ''
        infilelist = []
        outfile = None
        commentline = comments.pop(0)
        argmatch = argregexp.match(commentline)
        while argmatch:
            argname  = argmatch.group(1)
            argvalue = argmatch.group(2)

            if argname == 'infile':
                infilelist.append(argvalue)
            else:
                if argname == 'outfile':
                    outfile = argvalue
                if argname != lastargname:
                    arglist.append('--' + argname)
                    lastargname = argname

                if argvalue is not None:
                    arglist.append(argvalue)

            commentline = comments.pop(0)
            argmatch = argregexp.match(commentline)

        replaystack.append((cmd, infilelist + arglist + extraargs, infilelist, outfile))

        depth += 1
        if depth == args.depth:
            break

        filematch = fileregexp.match(commentline)


    if replaystack:
        (cmd, arglist, infilelist, outfilename) = replaystack.pop()
    else:
        cmd = None

    execute = args.force
    while cmd:
        if not execute:
            outfilestamp = datetime.datetime.utcfromtimestamp(os.path.getctime(outfilename))
            for infilename in infilelist:
                infilestamp = datetime.datetime.utcfromtimestamp(os.path.getctime(infilename))
                if infilestamp > outfilestamp:
                    execute = True
                    break

        if execute and infilelist:
            bakfile = outfilename + '.bak'
            if args.verbosity > 1:
                print("Renaming " + outfilename + " to " + bakfile, file=sys.stderr)

            if not args.dry_run:
                shutil.move(outfilename, bakfile)

            if args.verbosity > 1:
                print("Executing: " + cmd + ' ' + ' '.join(arglist), file=sys.stderr)

            if not args.dry_run:
                module = importlib.import_module(cmd)
                function = getattr(module, cmd)
                function(arglist)
        else:
            if args.verbosity > 2:
                print("Command not executed: " + cmd + ' ' + ' '.join(arglist), file=sys.stderr)

        if replaystack:
            (cmd, arglist, infilelist, outfilename) = replaystack.pop()
        else:
            cmd = None
