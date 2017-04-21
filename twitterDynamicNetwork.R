#!/usr/bin/Rscript
#
# Copyright 2017 Jonathan Schultz
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

library('argparse')
library('data.table')
library('network')
library('ndtv')

parser <- ArgumentParser(description='Visualise a dynamic network from a CSV file')

parser$add_argument('-v', '--verbosity',  type="integer", default=1)

parser$add_argument('-p', '--prelude',    type="character", nargs="*", help='Python code to execute before processing')
parser$add_argument('-w', '--weight',      type="character", default='1', help='Python expression(s) to evaluate tweet weight, for example "1 + retweets + favorites"')

parser$add_argument(      '--since',      type="character", help='Lower bound tweet date.')
parser$add_argument(      '--until',      type="character", help='Upper bound tweet date.')
parser$add_argument('-l', '--limit',      type="integer", help='Limit number of tweets to process')

parser$add_argument('-i', '--interval',   type="character", required=TRUE,
                    help='Interval for node/edge persistence. Format is "<w>w <d>d <h>h <m>m <s>s"')

parser$add_argument('-on', '--outnodefile',    type="character", help='Output CSV file for nodes.')
parser$add_argument('-oe', '--outedgefile',    type="character", help='Output CSV file for edges.')

parser$add_argument('infile', type="character", nargs='?', help='Input CSV file, otherwise use stdin.')

args <- parser$parse_args()

if (! is.null(args$until)) {
    until <- as.integer(as.POSIXct(args$until, tz="UTC"))
} else {
    until <- .Machine$integer.max
}
if (! is.null(args$since)) {
    since <- as.integer(as.POSIXct(args$since, tz="UTC"))
} else {
    since <- 0
}
match <- regexpr('(?:(?<weeks>[\\d]+)\\s*w)?\\s*(?:(?<days>[\\d]+)\\s*d)?\\s*(?:(?<hours>[\\d]+)\\s*h)?\\s*(?:(?<mins>[\\d]+)\\s*m)?\\s*(?:(?<secs>[\\d]+)\\s*s)?', args$interval, ignore.case=TRUE, perl=TRUE)
interval=as.difftime(0, unit="secs")
for (.name in attr(match, 'capture.name')) {
    thisinterval <- as.difftime(as.numeric(substr(args$interval,
                                   attr(match, 'capture.start')[,.name],
                                   attr(match, 'capture.start')[,.name] + attr(match, 'capture.length')[,.name] - 1)),
                            unit=.name)
    if (! is.na(thisinterval) )
        interval <- interval + thisinterval
}

if (is.null(args$infile))
    args$infile = 'stdin'

infile <- file(args$infile, open="rU")
pos = seek(infile)
while (TRUE) {
    line <- readLines(infile, 1)
    if (substr(line, 1, 1) != '#') {
        seek(infile, where=pos)
        break
    }
    else
        pos = seek(infile)
}

twitterread <- read.csv(infile, header=T, colClasses="character")
twitterread$ts <- as.integer(as.POSIXct(strptime(twitterread$date, "%Y-%m-%dT%H:%M:%S", tz="UTC")))

twitterread <- twitterread[twitterread$ts >= since & twitterread$ts < until,]

twitterread$mentionlist <- strsplit(twitterread$mentions, " ", fixed=TRUE)
twitterread$userlist <- ifelse(twitterread$mentions == "", "", strsplit(paste(twitterread$user, twitterread$mentions), " ", fixed=TRUE))

uniqueusers <- unique(unlist(twitterread$userlist))
uniqueusers <- uniqueusers[uniqueusers != ""]

edges <- rbindlist(apply(twitterread, MARGIN=1, function(x) expand.grid(source=which(uniqueusers == x$user), target=which(uniqueusers == x$mentionlist), onset=x$ts, terminus=x$ts + as.integer(interval))))

times<-rbindlist(apply(twitterread,MARGIN=1,function(x) expand.grid(user=x$userlist, ts=x$ts)))
times <- times[times$user != ""]
vertices <- data.frame(onset=sapply(uniqueusers, function(x) min(times[times$user == x,]$ts)), terminus=sapply(uniqueusers, function(x) max(times[times$user == x,]$ts) + as.integer(interval)), id=1:length(uniqueusers))

net <- network(edges, loops=F, multiple=F, ignore.eval = F)

edgesdyn <- data.frame(onset=edges$onset, terminus=edges$terminus, source=edges$source, target=edges$target)
verticesdyn <- data.frame(onset=vertices$onset, terminus=vertices$terminus, id=vertices$id)

print(vertices)
print(edgesdyn)

net.dyn<-networkDynamic(base.net=net, edge.spells=edgesdyn, vertex.spells=verticesdyn, interval=86400)

start <- min(twitterread$ts)
end   <- max(twitterread$ts)

compute.animation(net.dyn, animation.mode = "MDSJ", slice.par=list(start=start,end=end,interval=86400, aggregate.dur=1, rule='any'))
render.d3movie(net.dyn, filename="twitterDynamicNetwork.html")
