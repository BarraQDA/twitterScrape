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

    parser$add_argument('-v', '--verbosity',  type="integer",  default=1)

    parser$add_argument(      '--since',      type="character", help='Lower bound tweet date.')
    parser$add_argument(      '--until',      type="character", help='Upper bound tweet date.')
    parser$add_argument('-l', '--limit',      type="integer",   help='Limit number of tweets to process')

    parser$add_argument(      '--oembed', action="store_true",  help='Retrieve twitter HTML from server if not in CSV file')
    
    parser$add_argument('-m',  '--mode',      type="character", choices=c("kamadakawai", "MDSJ", "Graphviz"), default="kamadakawai", help='Animation layout mode.')

    parser$add_argument('-d', '--duration',   type="character", required=TRUE,
                        help='Network aggregation duration. Format is "<w>w <d>d <h>h <m>m <s>s"')
    parser$add_argument('-i', '--interval',   type="character", required=TRUE,
                        help='Animation interval. Format is "<w>w <d>d <h>h <m>m <s>s"')

    parser$add_argument(      '--degree',     type="integer", help="Display name of vertices with at least this degree.")

    parser$add_argument('-o', '--outfile',    type="character", required=TRUE, help='Output HTML file.')

    parser$add_argument('--userfile', type="character", help="CSV file containing Twitter user info")
    parser$add_argument('infile', type="character", nargs='?', help='Input CSV file, otherwise use stdin.')

    args <- parser$parse_args(arglist)

    if (args$verbosity >= 2)
        options(warn=1)

    until <- ifelse(is.null(args$until), .Machine$integer.max, as.integer(as.POSIXct(args$until, tz="UTC")))
    since <- ifelse(is.null(args$since), 0,                    as.integer(as.POSIXct(args$since, tz="UTC")))

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
    
    skipheader <- function(file){
        pos = seek(file)
        while (TRUE) {
            line <- readLines(file, 1)
            if (substr(line, 1, 1) != '#') {
                seek(file, where=pos)
                break
            }
            else
                pos = seek(file)
        }
    }

    library('data.table')
    library('network')
    library('ndtv')
    library('plotrix')
    library('rjson')

    infile <- file(args$infile, open="rU")
    skipheader(infile)
    twitterread <- read.csv(infile, header=T, colClasses="character")
    twitterread$ts <- as.integer(as.POSIXct(strptime(twitterread$date, "%Y-%m-%d %H:%M:%S", tz="UTC")))

    twitterread <- twitterread[twitterread$ts >= since & twitterread$ts < until,]
    if (! is.null(args$limit) & args$limit < nrow(twitterread)){
        twitterread <- twitterread[1:args$limit,]
    }
    twitterread$user <- tolower(twitterread$user)
    twitterread$mentions <- tolower(twitterread$mentions)
    twitterread$reply.to.user <- tolower(twitterread$reply.to.user)

    twitterread$linklist <- lapply(strsplit(paste(twitterread$mentions, twitterread$reply.to.user), " ", fixed=TRUE), 
                                   function(x) unique(x))
    twitterread <- twitterread[twitterread$linklist != "",]

    
    twitterread$userlist <- lapply(strsplit(paste(twitterread$user, twitterread$mentions, twitterread$reply.to.user), " ", fixed=TRUE),
                                   function(x) unique(x))
    
    uniqueusers <- unique(unlist(twitterread$userlist))
    uniqueusers <- uniqueusers[uniqueusers != ""]
#    uniqueusers <- uniqueusers[uniqueusers != "NA"]
    
    if (! is.null(args$userfile)){
        userfile <- file(args$userfile, open="rU")
        skipheader(userfile)
        twitteruser <- read.csv(userfile, header=T, colClasses="character")
        
        hydratedusers <- rbindlist(lapply(
                            uniqueusers,
                            function(user) {
                                if (! is.na(user) & user != "NA") {
                                    useridx <- which(tolower(twitteruser$screen_name) == user )
                                    if (! is.null(useridx)) {
                                        hydrateduser = list()
                                        hydrateduser$id                <- user
                                        hydrateduser$screen_name       <- twitteruser$screen_name[useridx]
                                        hydrateduser$name              <- twitteruser$name[useridx]
                                        hydrateduser$location          <- twitteruser$location[useridx]
                                        hydrateduser$created_at        <- twitteruser$created_at[useridx]
                                        hydrateduser$favourites_count  <- twitteruser$favourites_count[useridx]
                                        hydrateduser$followers_count   <- twitteruser$followers_count[useridx]
                                        hydrateduser$following         <- twitteruser$following[useridx]
                                        hydrateduser$friends_count     <- twitteruser$friends_count[useridx]
                                        hydrateduser$lang              <- twitteruser$lang[useridx]
                                        hydrateduser$profile_image_url <- twitteruser$profile_image_url[useridx]
                                                                                
                                        return(hydrateduser)
                                    }
                                }
                            }))
    }
    edges <- rbindlist(apply(
                 twitterread, 
                 MARGIN=1, 
                 function(tweet) expand.grid(
                    from=tweet$user, 
                    to=unique(tweet$linklist), 
                    twitterid=tweet$id,
                    text=tweet$text,
                    html=ifelse(is.null(tweet$html),
                                ifelse(args$oembed,
                                       fromJSON(readLines(sprintf("https://publish.twitter.com/oembed?url=https://twitter.com/any/status/%s", tweet$id)))$html,
                                       ""),
                                tweet$html))))

    # vertices <- data.frame(id=uniqueusers[order(uniqueusers)], stringsAsFactors = FALSE)

    net <- network(edges,
                   vertex.attr=hydratedusers[order(hydratedusers$id)],
                   matrix.type="edgelist",
                   loops=T,
                   ignore.eval = F)

    netusers <- get.vertex.attribute(net,"vertex.names")

    # Compute user vs time array to help calculate edges and vertices dynamic data
    times<-rbindlist(apply(twitterread,MARGIN=1,function(x) expand.grid(user=x$userlist, ts=x$ts)))
    # times <- times[times$user != ""]

    edgesdyn <- rbindlist(apply(twitterread, MARGIN=1, function(x) expand.grid(
                    onset=x$ts,
                    terminus=x$ts,
                    source=which(netusers == x$user),
                    target=as.integer(lapply(x$linklist, function(y) which(netusers == y))))
                ))

    verticesdyn <- rbindlist(apply(twitterread, MARGIN=1, function(x) expand.grid(
        onset=x$ts, terminus=x$ts, id=as.integer(lapply(x$userlist, function(y) which(netusers == y))))))

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
        if (! is.null(args$degree))
            ifelse(slice %v% "degree" >= args$degree, paste(slice %v% "screen_name", slice %v% "degree"), "")
    }

    edgelabel = function(slice) {
#         d<-get.dyads.active(slice, onset=-Inf, terminus=Inf)
        if (! is.null(args$degree))
            ifelse(slice %v% "degree" >= args$degree, paste(slice %v% "id", slice %v% "degree"), "")
    }

    edgetooltip = function(slice) {
        ids<-unlist(lapply(slice$mel, function(x)  x$atl$twitterid))
        dyads<-get.dyads.active(slice, onset=-Inf, terminus=Inf)
      
        edgeids<-lapply(slice$mel, function(x) 
                     ids[dyads[,1] == x$outl & dyads[,2] == x$inl])
        lapply(edgeids, function(edgeid) edges[edges$twitterid == edgeid]$html)
      
      # lapply(slice$mel, function(x)  ifelse(x$atl$html == '', as.character(x$atl$text), as.character(x$atl$html)))
    }

    vertextooltip = function(slice) {
        paste0(
            "<div class=\"TweetAuthor \" data-scribe=\"component:author\">
                <a class=\"TweetAuthor-link Identity u-linkBlend\" data-scribe=\"element:user_link\" href=\"https://twitter.com/", (slice %v% "id"), "\" aria-label=\"", (slice %v% "name"), " (screen name: ", (slice %v% "screen_name"), ")\">
                <span class=\"TweetAuthor-avatar Identity-avatar\">
                <img class=\"Avatar\" data-scribe=\"element:avatar\" data-src-2x=\"", (slice %v% "profile_image_url"), "\" alt=\"\" data-src-1x=\"", (slice %v% "profile_image_url"), "\" src=\"", (slice %v% "profile_image_url"), "\">
                </span>
                <p>
                <span class=\"TweetAuthor-name Identity-name customisable-highlight\" title=\"", (slice %v% "name"), "\" data-scribe=\"element:name\">", (slice %v% "name"), "</span>
                ", ifelse(! is.na(slice %v% "verified"), "<span class=\"TweetAuthor-verifiedBadge\" data-scribe=\"element:verified_badge\"><div class=\"Icon Icon--verified \" aria-label=\"Verified Account\" title=\"Verified Account\" role=\"img\"></div><b class=\"u-hiddenVisually\">✔</b></span>", ""),"
                </p><p>
                <span class=\"TweetAuthor-screenName Identity-screenName\" title=\"@", (slice %v% "screen_name"), "\" data-scribe=\"element:screen_name\" dir=\"ltr\">@", (slice %v% "screen_name"), "</span>
                </p>
                </a>
                </div>")

            # "<p><img src=\"https://twitter.com/", (slice %v% "id"), "/profile_image?size=original\" align=\"left\" width=\"112\" height=\"101\"/>
            # ",(slice %v% "screen_name"),"
            # ",(slice %v% "name"),"
            # ",(slice %v% "followers_count"),
            # "</p>"
#            (slice %v% "tweet_count")
        # paste0("<a href=\"https://twitter.com/", (slice %v% "screen_name"), "\" target=\"_blank\">", (slice %v% "name"), "</a>")    
    }
    
    vertexcex = function(slice) {
        rescale(log(as.integer(slice %v% "followers_count")), c(1,5))
    }
    
    if (endsWith(args$outfile, "html")) {
        render.d3movie(net.dyn,
                       filename=args$outfile,
                       displaylabels=TRUE,
                       label=vertexlabel,
    #                   label=netusers,
    #                   label=(net.dyn %v% "id"),
                       animationDuration=100,
                       vertex.cex=vertexcex,
                       vertex.tooltip = vertextooltip,
    #                   edge.lwd = (net.dyn %e% "weight"),
                       edge.label=edgelabel,
                       edge.tooltip = edgetooltip)
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
