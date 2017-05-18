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
import os
import sys
from TwitterFeed import TwitterRead
import re
import datetime
import subprocess

def twitterReplay(arglist):

    parser = argparse.ArgumentParser(description='Replay twitter file processing.',
                                     fromfile_prefix_chars='@')

    parser.add_argument('-v', '--verbosity', type=int, default=1)
    parser.add_argument('-d', '--depth',     type=int, default=1, help='Depth of command history to replay.')
    parser.add_argument('-f', '--force',     action='store_true', help='Replay command even if infile is not more recent.')
    parser.add_argument('--dry-run',         action='store_true', help='Print but do not execute command')

    parser.add_argument('infile',  type=str, nargs='*', help='Input CSV files with comments to replay.')

    args, extraargs = parser.parse_known_args()

    fileregexp = re.compile(r"^#+ (.+) #+$", re.UNICODE)
    cmdregexp  = re.compile(r"^#\s+([\w-]+)", re.UNICODE)
    argregexp  = re.compile(r"^#\s+([\w-]+)(?:=(.+))?", re.UNICODE)
    piperegexp = re.compile(r"^#+$", re.UNICODE)

    for infilename in args.infile:
        if args.verbosity >= 1:
            print("Replaying " + infilename, file=sys.stderr)

        # Read comments at start of infile.
        infile = file(infilename, 'r')
        comments = []
        while True:
            line = infile.readline()
            if line[:1] == '#':
                comments.append(line)
            else:
                infile.close()
                break

        depth = 0
        replaystack = []
        commentline = comments.pop(0)
        filematch = fileregexp.match(commentline)
        while filematch:
            pipestack = []
            infilelist = []
            outfile = None
            pipematch = True
            while pipematch and len(comments) > 0:
                commentline = comments.pop(0)
                cmdmatch = cmdregexp.match(commentline)
                if cmdmatch:
                    cmd = cmdmatch.group(1)
                else:
                    break

                arglist = []
                lastargname = ''
                commentline = comments.pop(0)
                argmatch = argregexp.match(commentline)
                while argmatch:
                    argname  = argmatch.group(1)
                    argvalue = argmatch.group(2)

                    if argname == 'infile':
                        if argvalue != '<stdin>':
                            infilelist.append(argvalue)
                    else:
                        if argname == 'outfile':
                            if argvalue != '<stdout>':
                                outfile = argvalue
                        else:
                            if argname != lastargname:
                                arglist.append('--' + argname)
                                lastargname = argname

                            if argvalue is not None:
                                arglist.append(argvalue)

                    commentline = comments.pop(0)
                    argmatch = argregexp.match(commentline)

                pipestack.append((cmd, arglist + extraargs))

                pipematch = piperegexp.match(commentline)

            replaystack.append((pipestack, infilelist, outfile))

            depth += 1
            if depth == args.depth:
                break

            filematch = fileregexp.match(commentline)


        if replaystack:
            (pipestack, infilelist, outfilename) = replaystack.pop()
        else:
            pipestack = None

        execute = args.force
        while pipestack:
            if not execute:
                outfilestamp = datetime.datetime.utcfromtimestamp(os.path.getmtime(outfilename))
                for infilename in infilelist:
                    infilestamp = datetime.datetime.utcfromtimestamp(os.path.getmtime(infilename))
                    if infilestamp > outfilestamp:
                        execute = True
                        break

            if execute:
                process = None
                while len(pipestack) > 0:
                    (cmd, arglist) = pipestack.pop()
                    if infilelist:
                        arglist = infilelist + arglist
                    infilelist = None
                    if len(pipestack) == 0:
                        arglist = arglist + ['--outfile', outfilename]
                    if args.verbosity >= 1:
                        print("Executing: " + cmd + ' ' + ' '.join(arglist), file=sys.stderr)

                    if not args.dry_run:
                        if not process:
                            process = subprocess.Popen([cmd+'.py'] + arglist,
                                                       stdout=subprocess.PIPE,
                                                       stderr=sys.stderr)
                        else:
                            process = subprocess.Popen([cmd+'.py'] + arglist,
                                                       stdout=subprocess.PIPE,
                                                       stdin=process.stdout,
                                                       stderr=sys.stderr)
                process.wait()
            else:
                if args.verbosity >= 2:
                    print("File not replayed: " + outfilename, file=sys.stderr)

            if replaystack:
                (pipestack, infilelist, outfilename) = replaystack.pop()
            else:
                pipestack = None

if __name__ == '__main__':
    twitterReplay(None)
