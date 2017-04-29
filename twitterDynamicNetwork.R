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

twitterDynamicNetwork <- function(arglist) {
    parser <- ArgumentParser(description='Visualise a dynamic network from a CSV file')
    
    parser$add_argument('-v', '--verbosity',  type="integer", default=1)
    
    parser$add_argument(      '--since',      type="character", help='Lower bound tweet date.')
    parser$add_argument(      '--until',      type="character", help='Upper bound tweet date.')
    parser$add_argument('-l', '--limit',      type="integer",   help='Limit number of tweets to process')
    
    parser$add_argument('-m',  '--mode',      type="character", choices=c("kamadakawai", "MDSJ", "Graphviz"), default="kamadakawai", help='Animation layout mode.')
    
    parser$add_argument('-d', '--duration', type="character", required=TRUE,
                        help='Network aggregation duration. Format is "<w>w <d>d <h>h <m>m <s>s"')
    parser$add_argument('-i', '--interval',   type="character", required=TRUE,
                        help='Interval for node/edge interval. Format is "<w>w <d>d <h>h <m>m <s>s"')
    
    parser$add_argument('-o', '--outfile',    type="character", required=TRUE, help='Output HTML file.')
    
    parser$add_argument('infile', type="character", nargs='?', help='Input CSV file, otherwise use stdin.')
    
    args <- parser$parse_args(arglist)
    
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
    
    parsetimediff <- function(td) {
        match <- regexpr('(?:(?<weeks>[\\d]+)\\s*w)?\\s*(?:(?<days>[\\d]+)\\s*d)?\\s*(?:(?<hours>[\\d]+)\\s*h)?\\s*(?:(?<mins>[\\d]+)\\s*m)?\\s*(?:(?<secs>[\\d]+)\\s*s)?', td, ignore.case=TRUE, perl=TRUE)
        result <- as.difftime(0, unit="secs")
        for (.name in attr(match, 'capture.name')) {
            thisresult <- as.difftime(as.numeric(substr(td,
                                        attr(match, 'capture.start')[,.name],
                                        attr(match, 'capture.start')[,.name] + attr(match, 'capture.length')[,.name] - 1)),
                                    unit=.name)
            if (! is.na(thisresult) )
                result <- result + thisresult
        }
        return (as.numeric(result, units="secs"))
    }
    
    duration <- parsetimediff(args$duration)
    interval <- parsetimediff(args$interval)

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
    
    library('data.table')
    library('network')
    library('ndtv')
    library('plotrix')
    
    twitterread <- read.csv(infile, header=T, colClasses="character")
    twitterread$ts <- as.integer(as.POSIXct(strptime(twitterread$date, "%Y-%m-%dT%H:%M:%S", tz="UTC")))
    
    twitterread <- twitterread[twitterread$ts >= since & twitterread$ts < until,]
    twitterread$user <- tolower(twitterread$user)
    twitterread$mentions <- tolower(twitterread$mentions)
    twitterread$reply.to.user <- tolower(twitterread$reply.to.user)
    
    linklist <- strsplit(paste(twitterread$mentions, twitterread$reply.to.user), " ", fixed=TRUE)
    linklist <- lapply(linklist, function(x) unique(x))
    twitterread$linklist <- linklist

    twitterread <- twitterread[twitterread$linklist != "",]
    userlist <- strsplit(paste(twitterread$user, twitterread$mentions, twitterread$reply.to.user), " ", fixed=TRUE)
    userlist <- lapply(userlist, function(x) unique(x))
    twitterread$userlist <- userlist
    
    uniqueusers <- unique(unlist(twitterread$userlist))
    uniqueusers <- uniqueusers[uniqueusers != ""]
    
    edges <- data.frame(rbindlist(apply(twitterread, MARGIN=1, function(x) expand.grid(
                from=x$user, to=unique(x$linklist), text=x$text, weight=1))))
    
    vertices <- data.frame(id=uniqueusers, stringsAsFactors = FALSE)
    
    net <- network(edges, vertex.attr=vertices, matrix.type="edgelist", loops=F, multiple=, ignore.eval = F)
    
    netusers <- get.vertex.attribute(net,"vertex.names")
    
    # Compute user vs time array to help calculate edges and vertices dynamic data
    times<-rbindlist(apply(twitterread,MARGIN=1,function(x) expand.grid(user=x$userlist, ts=x$ts)))
    # times <- times[times$user != ""]
    
    edgesdyn <- rbindlist(apply(twitterread, MARGIN=1, function(x) expand.grid(
                    onset=x$ts, 
                    terminus=x$ts, 
                    source=which(netusers == x$user), 
                    target=as.integer(lapply(x$linklist, function(y) which(netusers == y))),
                    tail=which(netusers == x$user)
#                    head=which(netusers == x$linklist)
                )))
    
    verticesdyn <- rbindlist(apply(twitterread, MARGIN=1, function(x) expand.grid(onset=x$ts, terminus=x$ts, id=as.integer(lapply(x$userlist, function(y) which(netusers == y))))))
    
    net.dyn<-networkDynamic(base.net=net, edge.spells=edgesdyn, vertex.spells=verticesdyn, interval=interval, create.TEAs=TRUE)
    
    start <- min(twitterread$ts)
    end   <- max(twitterread$ts)
    
    initialize.pids(net.dyn)
    
    vertexdegree = function(x) {
        netx<-network.extract(net.dyn,at=x)
        vertices<-get.vertex.id(net.dyn, get.vertex.pid(netx))
        values<-sapply(vertices, function(x) degree(netx)[get.vertex.id(netx, get.vertex.pid(net.dyn, x))])
        activate.vertex.attribute(net.dyn, "degree", values, at=x, v=vertices, dynamic.only = TRUE)
        net.dyn <<- net.dyn #  Export to calling environment
    }
    lapply(unique(verticesdyn$onset), vertexdegree)

    compute.animation(net.dyn, animation.mode = args$mode,
                      weight.attr="activity.count",
                      slice.par=list(start=start,end=end,interval=interval,
                      aggregate.dur=duration, rule='any'))
    
    vertexlabel = function(slice) {
        ifelse(slice %v% "degree" > 1, paste(net.dyn %v% "id", slice %v% "degree"), "")
    }
    
    edgelabel = function(slice) {
        d<-get.dyads.active(slice, onset=-Inf, terminus=Inf)
        #net.dyn 

    }

    if (endsWith(args$outfile, "html")) {
        render.d3movie(net.dyn,
                       filename=args$outfile,
                       displaylabels=TRUE, label=vertexlabel,
                       animationDuration=100,
    #                    vertex.cex=rescale(degree(net.dyn), c(1,10)),
                       vertex.tooltip = paste0("<a href=\"https://twitter.com/", (net.dyn %v% "id"), "\" target=\"_blank\">", (net.dyn %v% "id"), "</a>"),
    #                   edge.lwd = (net.dyn %e% "weight"),
    #                   edge.label=edgelabel,
                       edge.tooltip = paste0((net.dyn %e% "text")))
    } else {
        saveVideo(render.animation(net.dyn,
                                   displaylabels=FALSE,
                                   animationDuration=100,
    #                                vertex.cex=degree(net.dyn),
                                   vertex.tooltip = paste0("<a href=\"https://twitter.com/", (net.dyn %v% "id"), "\" target=\"_blank\">", (net %v% "id"), "</a>"),
                                   edge.lwd = (net.dyn %e% "weight"),
                                   edge.tooltip = paste0((net.dyn %e% "text"))),
                  video.name=args$outfile)
    }
}

if (! interactive())
    twitterDynamicNetwork(commandArgs(trailingOnly = T))
    warnings()
