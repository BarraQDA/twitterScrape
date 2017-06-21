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

twitterDynamicNetwork <- function(script, arglist) {
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

    parser$add_argument('-o', '--outfile',    type="character", help='Output HTML or mp4 file.')
    parser$add_argument('--no-comments',      action='store_true', help='Do not output descriptive comments')

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

    loadcomments <- function(file){
        comments <- ''
        pos <- seek(file)
        while (TRUE) {
            line <- readLines(file, 1)
            if (substr(line, 1, 1) != '#') {
                seek(file, where=pos)
                break
            }
            else
                comments <- paste(comments, line, sep='\n')
                pos <- seek(file)
        }
        return (comments)
    }

    library('stringr')
    library('data.table')
    library('network')
    library('ndtv')
    library('plotrix')
    library('rjson')

    infile <- file(args$infile, open="rU")
    incomments <- loadcomments(infile)
    hiddenargs = c('verbosity')

    if (! args$no_comments) {
        comments <- paste0(str_pad(paste0(' ', args$outfile, ' '), 80, 'both', '#'), '\n')
        comments <- paste0(comments, '# ', script, '\n')
        for (arg in attributes(args)$names) {
            if (! arg %in% hiddenargs) {
                val <- args[[arg]]
                if (class(val) == 'character')
                    comments <- paste0(comments, '#     --', arg, '="', val, '"\n')
                else if (class(val) == 'logical') {
                    if (val)
                        comments <- paste0(comments, '#     --', arg, '\n')
                }
                else if (class(val) == 'list') {
                    for (valitem in val)
                        if (class(valitem) == 'character')
                            comments <- paste0(comments, '#     --', arg, '="', valitem, '"\n')
                        else
                            comments <- paste0(comments, '#     --', arg, '=', valitem, '\n')


                }
                else if (! is.null(val))
                    comments <- paste0(comments, '#     --', arg, '=', val, '\n')
            }
        }

        write(comments, file=paste0(tools::file_path_sans_ext(args$outfile), '.log'))
    }

    twitterread <- read.csv(infile, header=T, colClasses="character")
    twitterread$ts <- as.integer(as.POSIXct(strptime(twitterread$date, "%Y-%m-%d %H:%M:%S", tz="UTC")))
    if (is.null(twitterread$ts))
        twitterread$ts <- as.integer(as.POSIXct(strptime(twitterread$date, "%Y-%m-%dT%H:%M:%S", tz="UTC")))

    twitterread <- twitterread[twitterread$ts >= since & twitterread$ts < until,]
    if (! is.null(args$limit))
        if (args$limit < nrow(twitterread))
            twitterread <- twitterread[1:args$limit,]

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

    if (! is.null(args$userfile)) {
        userfile <- file(args$userfile, open="rU")
        loadcomments(userfile)
        twitteruser <- read.csv(userfile, header=T, colClasses="character")

        hydratedusers <- rbindlist(lapply(
                            uniqueusers,
                            function(user) {
                                if (! is.na(user) & user != "NA") {
                                    useridx <- which(tolower(twitteruser$screen_name) == user )
                                    if (! is.null(useridx)) {
                                        hydrateduser = list()
                                        hydrateduser$id                <- user
                                        hydrateduser$statuses_count    <- twitteruser$statuses_count[useridx]
                                        hydrateduser$screen_name       <- twitteruser$screen_name[useridx]
                                        hydrateduser$name              <- twitteruser$name[useridx]
                                        hydrateduser$location          <- twitteruser$location[useridx]
                                        hydrateduser$created_at        <- twitteruser$created_at[useridx]
                                        hydrateduser$favourites_count  <- twitteruser$favourites_count[useridx]
                                        hydrateduser$followers_count   <- twitteruser$followers_count[useridx]
                                        hydrateduser$following         <- twitteruser$following[useridx]
                                        hydrateduser$friends_count     <- twitteruser$friends_count[useridx]
                                        hydrateduser$lang              <- twitteruser$lang[useridx]
                                        hydrateduser$profile_background_image_url <- twitteruser$profile_background_image_url[useridx]
                                        hydrateduser$profile_background_color <- twitteruser$profile_background_color[useridx]
                                        hydrateduser$profile_image_url <- twitteruser$profile_image_url[useridx]
                                        hydrateduser$description       <- twitteruser$description[useridx]
                                        hydrateduser$verified          <- twitteruser$verified[useridx]

                                        return(hydrateduser)
                                    }
                                }
                            }))
    }
    else {
        hydratedusers <- rbindlist(lapply(
                            uniqueusers,
                            function(user) {
                                if (! is.na(user) & user != "NA") {
                                    hydrateduser = list()
                                    hydrateduser$id  <- user

                                    return(hydrateduser)
                                }
                            }))
    }
    hydratedusers <- hydratedusers[order(hydratedusers$id)]

    edges <- rbindlist(apply(
                 twitterread,
                 MARGIN=1,
                 function(tweet) expand.grid(
                    from=tweet$user,
                    to=unique(tweet$linklist),
                    twitterid=tweet$id,
                    text=tweet$text,
                    retweets=as.integer(tweet$retweets),
                    html=ifelse(is.null(tweet$html),
                                ifelse(args$oembed,
                                       fromJSON(readLines(sprintf("https://publish.twitter.com/oembed?url=https://twitter.com/any/status/%s", tweet$id)))$html,
                                       ""),
                                tweet$html))))

    # vertices <- data.frame(id=uniqueusers[order(uniqueusers)], stringsAsFactors = FALSE)

    net <- network(edges,
                   vertex.attr=hydratedusers,
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

    compute.animation(net.dyn, animation.mode = args$mode,
                      slice.par=list(start=start,end=end,interval=interval,
                      aggregate.dur=duration, rule='any'))

    vertexlabel = function(slice) {
        # ifelse(degree(slice) >= args$degree, paste(slice %v% "screen_name", ""))
        ifelse(degree(slice) >= args$degree & ! is.na(slice %v% "screen_name"),
               paste(slice %v% "screen_name"), "")
    }

    edgelabel = function(slice) {
    }

    edgetooltip = function(slice) {
        ids<-unlist(lapply(slice$mel, function(x)  x$atl$twitterid))
        dyads<-get.dyads.active(slice, onset=-Inf, terminus=Inf)

        edgeids<-lapply(slice$mel, function(x)
                     ids[dyads[,1] == x$outl & dyads[,2] == x$inl])
        lapply(edgeids,
               function(edgeid)
                   edges[edges$twitterid == edgeid]$text
                   # ifelse(is.na(edges[edges$twitterid == edgeid]$html),
                   #        edges[edges$twitterid == edgeid]$text,
                   #        edges[edges$twitterid == edgeid]$html)
        )
    }

    vertextooltip = function(slice) {
        paste0("
  <div class=\"ProfileCard-bg\" style=\"", ifelse((slice %v% "profile_background_image_url") == "",
                                                  paste0("background-color: #", (slice %v% "profile_background_color")),
                                                  paste0("background-image: url('", (slice %v% "profile_background_image_url"), "')")),
                                            "\"></div>
    <a href=\"https://twitter.com/", (slice %v% "screen_name"), "\" class=\"ProfileCard-avatarLink js-nav js-tooltip\" title=\"", (slice %v% "name"), "\" tabindex=\"-1\" aria-hidden=\"true\" data-send-impression-cookie=\"true\">
        <img src=\"", (slice %v% "profile_image_url"),"\" alt=\"", (slice %v% "name"), "\" class=\"ProfileCard-avatarImage js-action-profile-avatar\">
    </a>

    <div class=\"UserActions   UserActions--small u-textLeft\">

      <div class=\"user-actions btn-group not-following not-muting \" data-user-id=\"2425411130\" data-screen-name=\"", (slice %v% "screen_name"), "\" data-name=\"", (slice %v% "name"), "\" data-protected=\"false\">

      </div>
    </div>

  <div class=\"ProfileCard-content \" data-screen-name=\"", (slice %v% "screen_name"), "\" data-user-id=\"2425411130\">
    <div class=\"ProfileCard-userFields\">
        <div class=\"ProfileNameTruncated account-group\">
  <div class=\"u-textTruncate u-inlineBlock\">
    <a class=\"fullname ProfileNameTruncated-link u-textInheritColor js-nav\" href=\"https://twitter.com/", (slice %v% "screen_name"), "\" data-aria-label-part=\"\" data-send-impression-cookie=\"true\">
      ", (slice %v% "name"), "</a></div><span class=\"UserBadges\">",
      ifelse((slice %v% "verified") == "True", "<span class=\"Icon Icon--verified\">::before<span class=\"u-hiddenVisually\">Verified account</span>::after</span>", ""),
      "</span>
  </div>
        <div class=\"ProfileCard-screenname\">
          <a href=\"https://twitter.com/", (slice %v% "screen_name"), "\" class=\"ProfileCard-screennameLink u-linkComplex js-nav\" data-aria-label-part=\"\" data-send-impression-cookie=\"true\">
            <span class=\"username u-dir\" dir=\"ltr\">@<b class=\"u-linkComplex-target\">", (slice %v% "screen_name"), "</b></span>
          </a>
        </div>
    </div>

      <div class=\"bio-container\">
        <p class=\"bio profile-field u-dir\" dir=\"ltr\">", (slice %v% "description"), "</p>
      </div>

  </div>
      <div class=\"ProfileCardStats\">
    <ul class=\"ProfileCardStats-statList Arrange Arrange--bottom Arrange--equal\"><li class=\"ProfileCardStats-stat Arrange-sizeFit\">
        <a class=\"ProfileCardStats-statLink u-textUserColor u-linkClean u-block js-nav js-tooltip\" title=\"", (slice %v% "statuses_count"), " Tweets\" href=\"https://twitter.com/", (slice %v% "screen_name"), "\" data-element-term=\"tweet_stats\" data-send-impression-cookie=\"true\">
          <span class=\"ProfileCardStats-statLabel u-block\">Tweets</span>
          <span class=\"ProfileCardStats-statValue\" data-count=\"", (slice %v% "statuses_count"), "\" data-is-compact=\"false\">", (slice %v% "statuses_count"), "</span>
        </a>
      </li><li class=\"ProfileCardStats-stat Arrange-sizeFit\">
          <a class=\"ProfileCardStats-statLink u-textUserColor u-linkClean u-block js-nav js-tooltip\" title=\"", (slice %v% "friends_count"), " Following\" href=\"https://twitter.com/", (slice %v% "screen_name"), "/following\" data-element-term=\"following_stats\" data-send-impression-cookie=\"true\">
            <span class=\"ProfileCardStats-statLabel u-block\">Following</span>
            <span class=\"ProfileCardStats-statValue\" data-count=\"", (slice %v% "friends_count"), "\" data-is-compact=\"false\">", (slice %v% "friends_count"), "</span>
          </a>
        </li><li class=\"ProfileCardStats-stat Arrange-sizeFit\">
          <a class=\"ProfileCardStats-statLink u-textUserColor u-linkClean u-block js-nav js-tooltip\" title=\"", (slice %v% "followers_count"), " Followers\" href=\"https://twitter.com/", (slice %v% "screen_name"), "/followers\" data-element-term=\"follower_stats\" data-send-impression-cookie=\"true\">
            <span class=\"ProfileCardStats-statLabel u-block\">Followers</span>
            <span class=\"ProfileCardStats-statValue\" data-count=\"", (slice %v% "followers_count"), "\" data-is-compact=\"false\">", (slice %v% "followers_count"), "</span>
          </a>
        </li>
    </ul>
  </div>

</div></div>

")
    }

    vertexcex = function(slice) {
        ifelse(is.na(slice %v% "followers_count"),
               1,
               rescale(log(as.integer(slice %v% "followers_count")), c(0.1,2)))
    }

    edgelwd = function(slice) {
        rescale(log(as.integer(slice %e% "retweets")), c(1,5))
    }

    if (is.null(args$outfile) || endsWith(args$outfile, "html")) {
        render.d3movie(net.dyn,
                       filename=if (is.null(args$outfile)) tempfile(fileext = '.html') else args$outfile,
                       displaylabels=TRUE,
                       label=vertexlabel,
    #                   label=netusers,
    #                   label=(net.dyn %v% "id"),
                       animationDuration=100,
                       vertex.cex=vertexcex,
                       vertex.tooltip = vertextooltip,
                       edge.lwd = edgelwd,
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
    script = basename(sub(".*=", "", commandArgs()[4])) # Ugly!
    twitterDynamicNetwork(script, commandArgs(trailingOnly = T))
